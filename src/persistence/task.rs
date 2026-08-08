use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use data_bucket::page::PageId;
use futures::FutureExt;
use parking_lot::Mutex as ParkingMutex;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use worktable_codegen::worktable;

use crate::persistence::operation::{BatchInnerRow, BatchInnerWorkTable, BatchOperation, OperationId};
use crate::persistence::{
    PersistenceEngine, PersistenceError, PersistenceIndexCorruption, PersistenceResult, PersistenceState,
};
use crate::prelude::*;
use crate::util::OptimizedVec;
use crate::vacuum::VacuumPersistence;

worktable! (
    name: QueueInner,
    columns: {
        id: u64 primary_key autoincrement,
        operation_id: OperationId,
        page_id: PageId,
        link: Link,
        pos: usize,
    },
    indexes: {
        operation_id_idx: operation_id,
        page_id_idx: page_id,
        link_idx: link,
    },
);

const MAX_PAGE_AMOUNT: usize = 16;

#[derive(Debug)]
struct PersistenceLifecycle {
    state: ParkingMutex<PersistenceState>,
    notify: Notify,
}

impl PersistenceLifecycle {
    fn new() -> Self {
        Self {
            state: ParkingMutex::new(PersistenceState::Running),
            notify: Notify::new(),
        }
    }

    fn state(&self) -> PersistenceState {
        self.state.lock().clone()
    }

    fn begin_close(&self) -> PersistenceResult {
        let mut state = self.state.lock();
        match &*state {
            PersistenceState::Running => {
                *state = PersistenceState::Closing;
                self.notify.notify_waiters();
                Ok(())
            }
            PersistenceState::Closing => Ok(()),
            PersistenceState::Failed(error) => Err(error.clone()),
            PersistenceState::Closed => Ok(()),
        }
    }

    fn finish_close(&self) {
        let mut state = self.state.lock();
        if matches!(*state, PersistenceState::Closing) {
            *state = PersistenceState::Closed;
        }
        self.notify.notify_waiters();
    }

    fn fail(&self, report: eyre::Report) -> Arc<PersistenceError> {
        let mut state = self.state.lock();
        let error = match &*state {
            PersistenceState::Failed(error) => error.clone(),
            _ => {
                let error = Arc::new(match report.downcast::<PersistenceIndexCorruption>() {
                    Ok(corruption) => PersistenceError::IndexCorruption(corruption),
                    Err(report) => PersistenceError::Engine(report),
                });
                *state = PersistenceState::Failed(error.clone());
                error
            }
        };
        self.notify.notify_waiters();
        error
    }

    fn ensure_running(&self) -> PersistenceResult {
        match self.state() {
            PersistenceState::Running => Ok(()),
            PersistenceState::Closing => Err(Arc::new(PersistenceError::Closing)),
            PersistenceState::Closed => Err(Arc::new(PersistenceError::Closed)),
            PersistenceState::Failed(error) => Err(error),
        }
    }
}

pub struct QueueAnalyzer<PrimaryKeyGenState, PrimaryKey, SecondaryKeys, AvailableIndexes> {
    operations: OptimizedVec<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>>,
    queue_inner_wt: Arc<QueueInnerWorkTable>,
    last_events_ids: LastEventIds<AvailableIndexes>,
    last_invalid_batch_size: usize,
    page_limit: usize,
    attempts: usize,
}

#[derive(Debug)]
pub struct LastEventIds<AvailableIndexes> {
    pub primary_id: IndexChangeEventId,
    pub secondary_ids: HashMap<AvailableIndexes, IndexChangeEventId>,
}

impl<AvailableIndexes> Default for LastEventIds<AvailableIndexes>
where
    AvailableIndexes: Eq + Hash,
{
    fn default() -> Self {
        Self {
            primary_id: Default::default(),
            secondary_ids: HashMap::new(),
        }
    }
}

impl<AvailableIndexes> LastEventIds<AvailableIndexes>
where
    AvailableIndexes: Debug + Hash + Eq,
{
    pub fn merge(&mut self, another: Self) {
        if another.primary_id != IndexChangeEventId::default() {
            self.primary_id = another.primary_id
        }
        for (index, id) in another.secondary_ids {
            if id != IndexChangeEventId::default() || !self.secondary_ids.contains_key(&index) {
                self.secondary_ids.insert(index, id);
            }
        }
    }
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys, AvailableIndexes>
    QueueAnalyzer<PrimaryKeyGenState, PrimaryKey, SecondaryKeys, AvailableIndexes>
where
    PrimaryKeyGenState: Debug,
    PrimaryKey: Debug,
    SecondaryKeys: Debug,
    AvailableIndexes: Debug + Copy + Clone + Hash + Eq,
{
    pub fn new(queue_inner_wt: Arc<QueueInnerWorkTable>) -> Self {
        Self {
            operations: OptimizedVec::with_capacity(256),
            queue_inner_wt,
            last_events_ids: Default::default(),
            last_invalid_batch_size: 0,
            page_limit: MAX_PAGE_AMOUNT,
            attempts: 0,
        }
    }

    pub fn push(&mut self, value: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>) -> eyre::Result<()> {
        let link = value.link();
        let mut row = QueueInnerRow {
            id: self.queue_inner_wt.get_next_pk().into(),
            operation_id: value.operation_id(),
            page_id: link.page_id,
            link,
            pos: 0,
        };
        let pos = self.operations.push(value);
        row.pos = pos;
        self.queue_inner_wt.insert(row)?;
        Ok(())
    }

    pub fn extend_from_iter(
        &mut self,
        i: impl Iterator<Item = Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>>,
    ) -> eyre::Result<()> {
        for op in i {
            self.push(op)?
        }
        Ok(())
    }

    pub fn get_first_op_id_available(&self) -> Option<OperationId> {
        self.queue_inner_wt
            .0
            .indexes
            .operation_id_idx
            .iter()
            .next()
            .map(|(id, _)| *id)
    }

    pub async fn collect_batch_from_op_id(
        &mut self,
        op_id: OperationId,
    ) -> eyre::Result<Option<BatchOperation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys, AvailableIndexes>>>
    where
        PrimaryKeyGenState: Clone,
        PrimaryKey: Clone,
        SecondaryKeys: Clone + Default + TableSecondaryIndexEventsOps<AvailableIndexes>,
    {
        let mut ops_set = HashSet::new();
        let mut used_page_ids = HashSet::new();

        let mut next_op_id = op_id;
        let mut no_more_ops = false;
        while used_page_ids.len() < self.page_limit && !no_more_ops {
            let ops_rows = self.queue_inner_wt.select_by_operation_id(next_op_id).execute()?;
            match next_op_id {
                OperationId::Single(_) => {
                    let page_id = ops_rows
                        .first()
                        .expect("at least one row should be available as operation exists")
                        .page_id;
                    used_page_ids.insert(page_id);
                    let page_ops = self.queue_inner_wt.select_by_page_id(page_id).execute()?;
                    let max_op_id = &mut next_op_id;
                    ops_set.extend(page_ops.into_iter().map(move |r| {
                        if r.operation_id > *max_op_id {
                            *max_op_id = r.operation_id
                        }
                        r.operation_id
                    }));
                }
                OperationId::Multi(_) => {
                    let mut ops_set_to_extend = HashSet::new();
                    used_page_ids.extend(ops_rows.iter().map(|r| r.page_id));
                    for page_id in ops_rows.iter().map(|r| r.page_id) {
                        let page_ops = self.queue_inner_wt.select_by_page_id(page_id).execute()?;
                        ops_set_to_extend.extend(page_ops.into_iter().map(|r| r.operation_id));
                    }
                    let mut block_op_id = None;
                    for op_id in ops_set_to_extend.iter().filter(|op_id| match op_id {
                        OperationId::Single(_) => false,
                        OperationId::Multi(_) => true,
                    }) {
                        let rows = self.queue_inner_wt.select_by_operation_id(*op_id).execute()?;
                        let pages = rows.iter().map(|r| r.page_id).collect::<HashSet<_>>();
                        // if pages used by multi op are not available is used_page_ids set, it's blocker op
                        for page in pages.iter() {
                            if !used_page_ids.contains(page) {
                                if let Some(block_op_id) = block_op_id.as_mut() {
                                    if *block_op_id > *op_id {
                                        *block_op_id = *op_id
                                    }
                                } else {
                                    block_op_id = Some(*op_id)
                                }
                            }
                        }
                    }
                    // And if we found some blocker, we need to remove all ops after blocking op.
                    let ops_set_to_extend = if let Some(block_op_id) = block_op_id {
                        ops_set_to_extend
                            .into_iter()
                            .filter(|op_id| *op_id >= block_op_id)
                            .collect()
                    } else {
                        ops_set_to_extend
                    };
                    ops_set.extend(ops_set_to_extend);
                    no_more_ops = true;
                }
            };
            let mut range = self.queue_inner_wt.0.indexes.operation_id_idx.range(next_op_id..);
            if let Some((id, _)) = range.nth(1) {
                next_op_id = *id;
            } else {
                no_more_ops = true
            }
        }
        // After this point, we have ops set ready for batch generation.
        let mut ops_pos_set = HashSet::new();
        for op_id in ops_set {
            let rows = self.queue_inner_wt.select_by_operation_id(op_id).execute()?;
            ops_pos_set.extend(rows.into_iter().map(|r| (r.pos, r.id)))
        }

        // Queue row IDs are monotonic insertion sequence numbers. Restore that
        // sequence after HashSet collection so operations sharing one Multi ID
        // remain in their original order through the stable OperationId sort.
        let mut ops_positions = ops_pos_set.into_iter().collect::<Vec<_>>();
        ops_positions.sort_unstable_by_key(|(_, id)| *id);

        let mut queued_ops = Vec::with_capacity(ops_positions.len());
        let info_wt = BatchInnerWorkTable::default();
        for (pos, id) in ops_positions {
            let row: BatchInnerRow = self.queue_inner_wt.select(id).expect("exists as Id exists").into();
            let op = self
                .operations
                .remove(pos)
                .expect("should be available as presented in table");
            queued_ops.push((op, row));
            self.queue_inner_wt.delete_without_lock::<_>(id).await?
        }
        // println!("New wt generated {:?}", start.elapsed());
        // The sort is stable, so queue creation order breaks ties for rows
        // produced by one multi-row operation.
        queued_ops.sort_by_key(|(op, _)| op.operation_id());
        let mut ops = Vec::with_capacity(queued_ops.len());
        for (pos, (op, mut row)) in queued_ops.into_iter().enumerate() {
            row.pos = pos;
            row.op_type = op.operation_type();
            info_wt.insert(row)?;
            ops.push(op);
        }

        let mut op = BatchOperation::new(ops, info_wt);
        let invalid_for_this_batch_ops = op.validate(&self.last_events_ids, self.attempts).await?;
        if let Some(invalid_for_this_batch_ops) = invalid_for_this_batch_ops {
            self.extend_from_iter(invalid_for_this_batch_ops.into_iter())?;
            let last_ids = op.get_last_event_ids();
            self.last_events_ids.merge(last_ids);
            self.last_invalid_batch_size = 0;
            self.page_limit = MAX_PAGE_AMOUNT;
            self.attempts = 0;

            Ok(Some(op))
        } else {
            // can't collect batch for now
            let ops = op.ops();
            self.attempts += 1;
            if self.last_invalid_batch_size == ops.len() {
                self.page_limit += 8;
            } else {
                self.last_invalid_batch_size = ops.len();
            }
            self.extend_from_iter(ops.into_iter())?;
            Ok(None)
        }
    }

    pub fn len(&self) -> usize {
        self.queue_inner_wt.count()
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Clone, Debug, Default)]
    struct TestConfig;

    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    enum TestIndex {}

    #[derive(Clone, Debug, Default)]
    struct TestEvents;

    impl TableSecondaryIndexEventsOps<TestIndex> for TestEvents {
        fn extend(&mut self, _another: Self) {}

        fn remove(&mut self, _another: &Self) {}

        fn last_evs(&self) -> HashMap<TestIndex, Option<IndexChangeEventId>> {
            HashMap::new()
        }

        fn first_evs(&self) -> HashMap<TestIndex, Option<IndexChangeEventId>> {
            HashMap::new()
        }

        fn iter_event_ids(&self) -> impl Iterator<Item = (TestIndex, IndexChangeEventId)> {
            std::iter::empty()
        }

        fn sort(&mut self) {}

        fn validate(&mut self) -> Self {
            Self
        }

        fn is_empty(&self) -> bool {
            true
        }

        fn is_unit() -> bool {
            true
        }
    }

    impl PersistenceConfig for TestConfig {
        fn table_path(&self) -> &str {
            ""
        }

        fn version(&self) -> u32 {
            0
        }
    }

    struct TestEngine {
        batches: Arc<AtomicUsize>,
        events: Arc<ParkingMutex<Vec<&'static str>>>,
        config: TestConfig,
        failure: TestFailure,
    }

    #[derive(Clone, Copy)]
    enum TestFailure {
        None,
        Engine,
        IndexCorruption,
        Panic,
    }

    impl PersistenceEngine<(), u64, TestEvents, TestIndex> for TestEngine {
        type Config = TestConfig;

        async fn new(config: Self::Config) -> eyre::Result<Self> {
            Ok(Self {
                batches: Arc::new(AtomicUsize::new(0)),
                events: Arc::new(ParkingMutex::new(Vec::new())),
                config,
                failure: TestFailure::None,
            })
        }

        async fn apply_operation(&mut self, _op: Operation<(), u64, TestEvents>) -> eyre::Result<()> {
            Ok(())
        }

        async fn apply_batch_operation(
            &mut self,
            _batch_op: BatchOperation<(), u64, TestEvents, TestIndex>,
        ) -> eyre::Result<()> {
            match self.failure {
                TestFailure::None => {}
                TestFailure::Engine => return Err(eyre::eyre!("injected batch failure")),
                TestFailure::IndexCorruption => {
                    return Err(
                        PersistenceIndexCorruption::new("table/primary.wt.idx", "injected shadow divergence").into(),
                    );
                }
                TestFailure::Panic => panic!("injected persistence worker panic"),
            }
            self.batches.fetch_add(1, Ordering::Relaxed);
            self.events.lock().push("batch");
            Ok(())
        }

        async fn reclaim_data_pages(&mut self, _page_ids: Vec<PageId>) -> eyre::Result<()> {
            self.events.lock().push("reclaim");
            Ok(())
        }

        fn config(&self) -> &Self::Config {
            &self.config
        }
    }

    fn insert_operation(id: u128) -> Operation<(), u64, TestEvents> {
        Operation::Insert(InsertOperation {
            id: OperationId::Single(uuid::Uuid::from_u128(id)),
            pk_gen_state: (),
            primary_key_events: vec![],
            secondary_keys_events: TestEvents,
            bytes: vec![id as u8],
            link: Link {
                page_id: 1.into(),
                offset: id as u32,
                length: 1,
            },
        })
    }

    fn multi_insert_operation(id: u128, offset: u32, byte: u8) -> Operation<(), u64, TestEvents> {
        Operation::Insert(InsertOperation {
            id: OperationId::Multi(uuid::Uuid::from_u128(id)),
            pk_gen_state: (),
            primary_key_events: vec![],
            secondary_keys_events: TestEvents,
            bytes: vec![byte; 8],
            link: Link {
                page_id: 1.into(),
                offset,
                length: 8,
            },
        })
    }

    #[tokio::test]
    async fn analyzer_preserves_queue_order_for_operations_sharing_a_multi_id() {
        let queue_inner_wt = Arc::new(QueueInnerWorkTable::default());
        let mut analyzer: QueueAnalyzer<(), u64, TestEvents, TestIndex> = QueueAnalyzer::new(queue_inner_wt);
        analyzer.push(multi_insert_operation(1, 128, 1)).unwrap();
        analyzer.push(multi_insert_operation(1, 132, 2)).unwrap();

        let batch = analyzer
            .collect_batch_from_op_id(OperationId::Multi(uuid::Uuid::from_u128(1)))
            .await
            .unwrap()
            .unwrap()
            .get_batch_data_op()
            .unwrap();

        assert_eq!(
            batch.get(&1.into()).unwrap(),
            &vec![
                (
                    Link {
                        page_id: 1.into(),
                        offset: 128,
                        length: 8,
                    },
                    vec![1; 8],
                ),
                (
                    Link {
                        page_id: 1.into(),
                        offset: 132,
                        length: 8,
                    },
                    vec![2; 8],
                ),
            ]
        );
    }

    #[tokio::test]
    async fn close_drains_and_joins_the_engine() {
        let batches = Arc::new(AtomicUsize::new(0));
        let task = PersistenceTask::run_engine(TestEngine {
            batches: batches.clone(),
            events: Arc::new(ParkingMutex::new(Vec::new())),
            config: TestConfig,
            failure: TestFailure::None,
        });

        task.apply_operation(insert_operation(1)).unwrap();
        task.close().await.unwrap();

        assert_eq!(batches.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn engine_failure_is_terminal_and_reused_for_later_callers() {
        let task = PersistenceTask::run_engine(TestEngine {
            batches: Arc::new(AtomicUsize::new(0)),
            events: Arc::new(ParkingMutex::new(Vec::new())),
            config: TestConfig,
            failure: TestFailure::Engine,
        });

        task.apply_operation(insert_operation(1)).unwrap();
        let wait_error = task.wait_for_ops().await.unwrap_err();
        assert!(wait_error.to_string().contains("injected batch failure"));

        let intake_error = task.apply_operation(insert_operation(2)).unwrap_err();
        assert!(Arc::ptr_eq(&wait_error, &intake_error));

        let close_error = task.close().await.unwrap_err();
        assert!(Arc::ptr_eq(&wait_error, &close_error));
    }

    #[tokio::test]
    async fn engine_panic_is_terminal_and_reported_to_waiters() {
        let task = PersistenceTask::run_engine(TestEngine {
            batches: Arc::new(AtomicUsize::new(0)),
            events: Arc::new(ParkingMutex::new(Vec::new())),
            config: TestConfig,
            failure: TestFailure::Panic,
        });

        task.apply_operation(insert_operation(1)).unwrap();
        let wait_error = task.wait_for_failure().await.unwrap_err();
        assert_eq!(
            wait_error.to_string(),
            "persistence engine failed: persistence worker panicked"
        );

        let intake_error = task.apply_operation(insert_operation(2)).unwrap_err();
        assert!(Arc::ptr_eq(&wait_error, &intake_error));
    }

    #[tokio::test]
    async fn vacuum_reclamation_waits_for_preceding_row_moves() {
        let events = Arc::new(ParkingMutex::new(Vec::new()));
        let task = PersistenceTask::run_engine(TestEngine {
            batches: Arc::new(AtomicUsize::new(0)),
            events: events.clone(),
            config: TestConfig,
            failure: TestFailure::None,
        });

        task.apply_operation(insert_operation(1)).unwrap();
        task.queue.reclaim_pages(vec![1.into()]).unwrap();
        task.apply_operation(insert_operation(2)).unwrap();
        task.wait_for_ops().await.unwrap();

        assert_eq!(&*events.lock(), &["batch", "reclaim", "batch"]);
    }

    #[tokio::test]
    async fn index_corruption_from_engine_is_typed_and_terminal_for_callers() {
        let task = PersistenceTask::run_engine(TestEngine {
            batches: Arc::new(AtomicUsize::new(0)),
            events: Arc::new(ParkingMutex::new(Vec::new())),
            config: TestConfig,
            failure: TestFailure::IndexCorruption,
        });

        task.apply_operation(insert_operation(1)).unwrap();
        let wait_error = task.wait_for_ops().await.unwrap_err();
        assert!(matches!(wait_error.as_ref(), PersistenceError::IndexCorruption(_)));

        let intake_error = task.apply_operation(insert_operation(2)).unwrap_err();
        assert!(Arc::ptr_eq(&wait_error, &intake_error));
        let close_error = task.close().await.unwrap_err();
        assert!(Arc::ptr_eq(&wait_error, &close_error));
    }

    #[test]
    fn typed_index_corruption_quarantines_the_persistence_lifecycle() {
        let lifecycle = PersistenceLifecycle::new();
        let error = lifecycle.fail(
            PersistenceIndexCorruption::new("table/primary.wt.idx", "shadow diverged from logical stream").into(),
        );

        match error.as_ref() {
            PersistenceError::IndexCorruption(corruption) => {
                assert_eq!(corruption.path(), std::path::Path::new("table/primary.wt.idx"));
                assert!(corruption.reason().contains("shadow diverged"));
            }
            other => panic!("expected typed index corruption, got {other:?}"),
        }

        let intake_error = lifecycle.ensure_running().unwrap_err();
        assert!(Arc::ptr_eq(&error, &intake_error));
    }
}

#[derive(Debug)]
enum PersistenceMessage<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
    Operation(Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>),
    ReclaimPages(Vec<PageId>),
}

#[derive(Debug)]
pub struct Queue<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
    // Not `lockfree::queue::Queue`: its `Removable::empty` materializes the
    // element type via `mem::uninitialized`, which aborts at runtime for
    // `Operation` layouts that reject uninit bytes. The queue has a single
    // consumer (the engine task), so a mutexed deque is uncontended here.
    queue: ParkingMutex<VecDeque<PersistenceMessage<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>>>,
    notify: Notify,
    // usize, not u16: the queue is unbounded and a 16-bit counter wraps at
    // 65_536 queued operations, making the wait triggers see an "empty"
    // queue that still holds work.
    len: Arc<AtomicUsize>,
    lifecycle: Arc<PersistenceLifecycle>,
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> Queue<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
    fn new(lifecycle: Arc<PersistenceLifecycle>) -> Self {
        Self {
            queue: ParkingMutex::new(VecDeque::new()),
            notify: Notify::new(),
            len: Arc::new(AtomicUsize::new(0)),
            lifecycle,
        }
    }

    pub fn push(&self, value: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>) -> PersistenceResult {
        self.push_message(PersistenceMessage::Operation(value))
    }

    fn push_message(
        &self,
        value: PersistenceMessage<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>,
    ) -> PersistenceResult {
        let state = self.lifecycle.state.lock();
        match &*state {
            PersistenceState::Running => {}
            PersistenceState::Closing => return Err(Arc::new(PersistenceError::Closing)),
            PersistenceState::Closed => return Err(Arc::new(PersistenceError::Closed)),
            PersistenceState::Failed(error) => return Err(error.clone()),
        }
        self.len.fetch_add(1, Ordering::Release);
        self.queue.lock().push_back(value);
        self.notify.notify_one();
        Ok(())
    }

    /// Pops the next operation, marking `in_progress` `true` before the queue
    /// length is decremented. The store must precede `len.fetch_sub` so that a
    /// waiter that reads `len() == 0` (`Acquire`) is guaranteed to observe
    /// `in_progress == true` for the popped-but-unprocessed operation;
    /// otherwise `wait_for_ops` can return while that operation is in flight.
    async fn pop_marking_in_progress(
        &self,
        in_progress: &AtomicBool,
    ) -> Option<PersistenceMessage<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>> {
        loop {
            let notified = self.notify.notified();
            // Drain values
            {
                let mut queue = self.queue.lock();
                if let Some(value) = queue.pop_front() {
                    in_progress.store(true, Ordering::Release);
                    self.len.fetch_sub(1, Ordering::Release);
                    return Some(value);
                }
            }

            if !matches!(self.lifecycle.state(), PersistenceState::Running) {
                return None;
            }

            // Wait for values to be available
            notified.await;
        }
    }

    fn wake(&self) {
        self.notify.notify_waiters();
    }

    fn immediate_pop(&self) -> Option<PersistenceMessage<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>> {
        if let Some(v) = self.queue.lock().pop_front() {
            self.len.fetch_sub(1, Ordering::Release);
            Some(v)
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> VacuumPersistence<PrimaryKey, SecondaryKeys>
    for Queue<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
where
    PrimaryKeyGenState: Send,
    PrimaryKey: Send,
    SecondaryKeys: Send,
{
    fn apply_move(
        &self,
        bytes: Vec<u8>,
        new_link: Link,
        primary_key_events: Vec<IndexChangeEvent<IndexPair<PrimaryKey, Link>>>,
        secondary_keys_events: SecondaryKeys,
    ) -> PersistenceResult {
        self.push(Operation::Update(UpdateOperation {
            id: OperationId::Single(uuid::Uuid::now_v7()),
            primary_key_events,
            secondary_keys_events,
            bytes,
            link: new_link,
        }))
    }

    fn reclaim_pages(&self, page_ids: Vec<PageId>) -> PersistenceResult {
        self.push_message(PersistenceMessage::ReclaimPages(page_ids))
    }
}

#[derive(Debug)]
pub struct PersistenceTask<PrimaryKeyGenState, PrimaryKey, SecondaryKeys, AvailableIndexes> {
    engine_task_handle: Option<JoinHandle<()>>,
    queue: Arc<Queue<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>>,
    analyzer_inner_wt: Arc<QueueInnerWorkTable>,
    analyzer_in_progress: Arc<AtomicBool>,
    lifecycle: Arc<PersistenceLifecycle>,
    table_path: String,
    phantom_data: PhantomData<AvailableIndexes>,
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys, AvailableIndexes> Drop
    for PersistenceTask<PrimaryKeyGenState, PrimaryKey, SecondaryKeys, AvailableIndexes>
{
    /// Aborts the engine task so it cannot outlive the table it persists.
    /// Without this the detached task keeps running on the runtime after the
    /// table is dropped, and a re-opened table can read the same files while
    /// the old engine is still writing them.
    ///
    /// The abort only happens when the engine is provably idle (queue and
    /// analyzer empty, no operation in flight) — that is the normal state
    /// after `wait_for_ops`, and an idle task is parked at the queue pop, an
    /// await point where cancellation is clean. Aborting a *busy* engine
    /// would cancel persistence futures that are not cancellation-safe (a
    /// data page could be left half-written while its index events are
    /// abandoned), so a busy engine is left running and reported instead:
    /// callers must drain with `wait_for_ops` before dropping. A proper
    /// `close()` lifecycle (drain, join, surface terminal errors) is the
    /// long-term replacement for this heuristic.
    fn drop(&mut self) {
        let Some(handle) = self.engine_task_handle.as_ref() else {
            return;
        };
        if handle.is_finished() {
            return;
        }
        if matches!(
            self.lifecycle.state(),
            PersistenceState::Failed(_) | PersistenceState::Closed
        ) {
            return;
        }
        if self.check_wait_triggers() {
            handle.abort();
        } else {
            tracing::error!(
                "PersistenceTask dropped with work in flight; the engine task keeps running detached.                  Call wait_for_ops() before dropping to guarantee a clean shutdown."
            );
        }
    }
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys, AvailableIndexes>
    PersistenceTask<PrimaryKeyGenState, PrimaryKey, SecondaryKeys, AvailableIndexes>
{
    pub fn apply_operation(&self, op: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>) -> PersistenceResult {
        self.queue.push(op)
    }

    pub fn ensure_running(&self) -> PersistenceResult {
        self.lifecycle.ensure_running()
    }

    pub fn state(&self) -> PersistenceState {
        self.lifecycle.state()
    }

    /// Returns the current physical size of the table data file.
    ///
    /// This is intentionally separate from `VacuumStats`: online vacuum makes
    /// freed pages durably reusable, but does not truncate `.wt.data`.
    /// Operators can sample this value to observe physical growth and reuse.
    pub async fn persisted_data_file_size_bytes(&self) -> std::io::Result<u64> {
        tokio::fs::metadata(format!(
            "{}/{}",
            self.table_path.trim_end_matches('/'),
            WT_DATA_EXTENSION
        ))
        .await
        .map(|metadata| metadata.len())
    }

    /// Returns a sink that lets vacuum queue persistence operations for row
    /// moves into this task's operation queue.
    pub fn vacuum_sink(&self) -> Arc<dyn VacuumPersistence<PrimaryKey, SecondaryKeys>>
    where
        PrimaryKeyGenState: Send + Sync + 'static,
        PrimaryKey: Send + Sync + 'static,
        SecondaryKeys: Send + Sync + 'static,
    {
        self.queue.clone()
    }

    pub fn run_engine<E>(mut engine: E) -> Self
    where
        E: PersistenceEngine<PrimaryKeyGenState, PrimaryKey, SecondaryKeys, AvailableIndexes> + Send + 'static,
        SecondaryKeys: Clone + Debug + Default + TableSecondaryIndexEventsOps<AvailableIndexes> + Send + Sync + 'static,
        PrimaryKeyGenState: Clone + Debug + Send + Sync + 'static,
        PrimaryKey: Clone + Debug + Send + Sync + 'static,
        AvailableIndexes: Copy + Clone + Debug + Hash + Eq + Send + Sync + 'static,
    {
        let table_path = engine.config().table_path().to_owned();
        let lifecycle = Arc::new(PersistenceLifecycle::new());
        let queue = Arc::new(Queue::new(lifecycle.clone()));

        let engine_queue = queue.clone();
        let engine_lifecycle = lifecycle.clone();
        let analyzer_inner_wt: Arc<QueueInnerWorkTable> = Default::default();
        let mut analyzer = QueueAnalyzer::new(analyzer_inner_wt.clone());
        let analyzer_in_progress = Arc::new(AtomicBool::new(true));
        let task_analyzer_in_progress = analyzer_in_progress.clone();

        let worker = async move {
            let mut pending_reclaim: Option<Vec<PageId>> = None;
            loop {
                let message = if pending_reclaim.is_none() {
                    engine_queue.immediate_pop()
                } else {
                    None
                };
                let message = if message.is_some() {
                    message
                } else if analyzer.len() == 0 && pending_reclaim.is_none() {
                    task_analyzer_in_progress.store(false, Ordering::Release);
                    engine_lifecycle.notify.notify_waiters();
                    if matches!(engine_lifecycle.state(), PersistenceState::Closing) {
                        engine_lifecycle.finish_close();
                        return;
                    }
                    // The pop sets the flag back to `true` atomically with the
                    // dequeue, so waiters never observe an empty queue with an
                    // idle analyzer while an operation is in flight.
                    engine_queue.pop_marking_in_progress(&task_analyzer_in_progress).await
                } else {
                    None
                };

                if let Some(message) = message {
                    match message {
                        PersistenceMessage::Operation(op) => {
                            if let Err(err) = analyzer.push(op) {
                                engine_lifecycle.fail(err);
                                return;
                            }
                        }
                        PersistenceMessage::ReclaimPages(page_ids) => pending_reclaim = Some(page_ids),
                    }
                }

                // Pull operations up to, but never past, a reclamation
                // barrier. This gives the analyzer every CDC event required
                // for a batch while preserving FIFO ordering for maintenance.
                while pending_reclaim.is_none() {
                    match engine_queue.immediate_pop() {
                        Some(PersistenceMessage::Operation(op)) => {
                            if let Err(err) = analyzer.push(op) {
                                engine_lifecycle.fail(err);
                                return;
                            }
                        }
                        Some(PersistenceMessage::ReclaimPages(page_ids)) => pending_reclaim = Some(page_ids),
                        None => break,
                    }
                }

                if let Some(op_id) = analyzer.get_first_op_id_available() {
                    let batch_op = analyzer.collect_batch_from_op_id(op_id).await;
                    if let Err(e) = batch_op {
                        engine_lifecycle.fail(e);
                        return;
                    } else if let Some(batch_op) = batch_op.unwrap() {
                        let res = engine.apply_batch_operation(batch_op).await;
                        if let Err(e) = res {
                            engine_lifecycle.fail(e);
                            return;
                        }
                    } else {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                } else if let Some(page_ids) = pending_reclaim.take() {
                    // `get_first_op_id_available() == None` is only sufficient
                    // when the analyzer itself is empty. If its operation-id
                    // index ever loses an entry, reclaiming here would make a
                    // source page reusable before its buffered row move became
                    // durable. Fail terminally instead of trusting that state.
                    let buffered_operations = analyzer.len();
                    if buffered_operations != 0 {
                        engine_lifecycle.fail(eyre::eyre!(
                            "persistence reclamation barrier found {buffered_operations} buffered operations without an operation-id index entry"
                        ));
                        return;
                    }
                    if let Err(error) = engine.reclaim_data_pages(page_ids).await {
                        engine_lifecycle.fail(error);
                        return;
                    }
                }
            }
        };
        let supervisor_lifecycle = lifecycle.clone();
        let task = async move {
            if let Err(payload) = AssertUnwindSafe(worker).catch_unwind().await {
                // The process panic hook already records the local diagnostic.
                // Do not propagate arbitrary panic payloads to API consumers:
                // engines can panic with paths or row-derived strings.
                drop(payload);
                supervisor_lifecycle.fail(eyre::eyre!("persistence worker panicked"));
            }
        };
        let engine_task_handle = tokio::spawn(task);
        Self {
            queue,
            engine_task_handle: Some(engine_task_handle),
            analyzer_inner_wt,
            analyzer_in_progress,
            lifecycle,
            table_path,
            phantom_data: PhantomData,
        }
    }

    fn check_wait_triggers(&self) -> bool {
        if self.queue.len() != 0 {
            return false;
        }
        if self.analyzer_inner_wt.count() != 0 {
            return false;
        }
        if self.analyzer_in_progress.load(Ordering::Acquire) {
            return false;
        }
        true
    }

    pub async fn wait_for_ops(&self) -> PersistenceResult {
        loop {
            match self.lifecycle.state() {
                PersistenceState::Failed(error) => return Err(error),
                PersistenceState::Closed => return Ok(()),
                PersistenceState::Running if self.check_wait_triggers() => return Ok(()),
                PersistenceState::Running | PersistenceState::Closing => {}
            }

            if self.engine_task_handle.as_ref().is_some_and(JoinHandle::is_finished) {
                let error = self
                    .lifecycle
                    .fail(eyre::eyre!("persistence engine task terminated unexpectedly"));
                return Err(error);
            }

            let queue_count = self.queue.len();
            let analyzer_count = self.analyzer_inner_wt.count();
            let count = queue_count + analyzer_count;
            if count == 0 {
                tracing::info!("Waiting for last operation");
            } else {
                tracing::info!("Waiting for {} operations", count);
            }

            tokio::select! {
                _ = self.lifecycle.notify.notified() => {},
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }

    /// Waits until the worker fails or closes.
    ///
    /// Unlike [`Self::wait_for_ops`], an idle healthy worker does not satisfy
    /// this future. Applications can keep it alive as a terminal-state
    /// notification without forcing persistence queues to drain or polling
    /// their state. A graceful close returns `Ok(())`; a failure returns the
    /// shared terminal error.
    pub async fn wait_for_failure(&self) -> PersistenceResult {
        loop {
            let notified = self.lifecycle.notify.notified();
            tokio::pin!(notified);
            // `notify_waiters` does not retain a permit. Register this waiter
            // before reading the lifecycle state so a terminal transition
            // cannot land between the state read and the first poll of
            // `notified` and be lost forever.
            notified.as_mut().enable();
            match self.lifecycle.state() {
                PersistenceState::Failed(error) => return Err(error),
                PersistenceState::Closed => return Ok(()),
                PersistenceState::Running | PersistenceState::Closing => notified.await,
            }
        }
    }

    pub async fn close(mut self) -> PersistenceResult {
        let begin_result = self.lifecycle.begin_close();
        self.queue.wake();

        if let Some(handle) = self.engine_task_handle.take()
            && let Err(error) = handle.await
        {
            return Err(self
                .lifecycle
                .fail(eyre::eyre!("persistence engine task failed to join: {error}")));
        }

        match self.lifecycle.state() {
            PersistenceState::Closed => begin_result,
            PersistenceState::Failed(error) => Err(error),
            PersistenceState::Running | PersistenceState::Closing => Err(self
                .lifecycle
                .fail(eyre::eyre!("persistence engine task exited without a terminal state"))),
        }
    }
}
