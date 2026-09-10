use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::sync::Arc;
use alloc::{borrow::ToOwned, string::String, string::ToString, vec::Vec};
use core::fmt::Debug;
use core::hash::Hash;
use core::marker::PhantomData;
use core::panic::Location;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use core::time::Duration;
use hashbrown::{HashMap, HashSet};

use data_bucket::page::PageId;
use nagoya::sync::Notify;
use parking_lot::Mutex as ParkingMutex;

use nagoya::JoinHandle;
use worktable_codegen::worktable;

use crate::persistence::event_ledger::{self, EventLedger, EventStream, Stages};
use crate::persistence::operation::{BatchInnerRow, BatchInnerWorkTable, BatchOperation, OperationId, OperationType};
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
        operation_id_idx: operation_id using worktables_index,
        page_id_idx: page_id using worktables_index,
        link_idx: link using worktables_index,
    },
);

const MAX_PAGE_AMOUNT: usize = 16;

/// Attempts after which batch collection stops grouping by data page and takes
/// the whole queue.
///
/// Grouping by page is a write-batching optimisation, and it cannot always
/// assemble a gapless event stream. Event ids are allocated during the index
/// mutations, while an operation id is minted later, at the push site, so two
/// concurrent writers can invert the two orders. Vacuum makes that systematic:
/// its update lands on the destination page while inserts append to the current
/// page, so the operation holding the missing id sits on a page this collection
/// never visits. Every retry then rebuilds the same gapped batch, and the
/// attempt budget eventually fails the engine for that table permanently.
///
/// Taking the whole queue breaks the loop. It gives up the grouping and keeps
/// the guarantee that matters: `validate_events` still refuses to apply a
/// stream with a hole, so a complete collection can only ever apply more of the
/// contiguous prefix than a partial one, never something unsafe.
const COLLECT_WHOLE_QUEUE_AFTER_ATTEMPTS: usize = 4;

#[derive(Debug)]
struct PersistenceLifecycle {
    state: ParkingMutex<PersistenceState>,
    /// Terminal transitions only: `Failed` or `Closed`.
    terminal_notify: Notify,
    /// Queue-drain and lifecycle progress observed by `wait_for_ops`.
    progress_notify: Notify,
}

impl PersistenceLifecycle {
    fn new() -> Self {
        Self {
            state: ParkingMutex::new(PersistenceState::Running),
            terminal_notify: Notify::new(),
            progress_notify: Notify::new(),
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
                self.progress_notify.notify_waiters();
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
        self.terminal_notify.notify_waiters();
        self.progress_notify.notify_waiters();
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
        self.terminal_notify.notify_waiters();
        self.progress_notify.notify_waiters();
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

/// Marks every non-graceful worker exit as terminal, including cancellation
/// before the worker future's first poll and panic unwinding during a poll.
struct WorkerCompletionGuard {
    lifecycle: Arc<PersistenceLifecycle>,
    armed: bool,
}

impl WorkerCompletionGuard {
    fn new(lifecycle: Arc<PersistenceLifecycle>) -> Self {
        Self { lifecycle, armed: true }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for WorkerCompletionGuard {
    fn drop(&mut self) {
        if self.armed {
            let reason = if std::thread::panicking() {
                "persistence worker panicked"
            } else {
                "persistence worker was cancelled"
            };
            self.lifecycle.fail(eyre::eyre!(reason));
        }
    }
}

/// Cloneable terminal-state handle independent of table ownership.
///
/// Create this handle before spawning a supervisor. The table can then still
/// be moved into `close()`, while the supervisor observes either graceful
/// closure or a terminal persistence failure.
#[derive(Clone, Debug)]
pub struct PersistenceMonitor {
    lifecycle: Arc<PersistenceLifecycle>,
}

impl PersistenceMonitor {
    /// Waits until the worker fails or closes.
    pub async fn wait_for_failure(self) -> PersistenceResult {
        loop {
            let notified = self.lifecycle.terminal_notify.notified();
            let mut notified = core::pin::pin!(notified);
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
}

pub struct QueueAnalyzer<PrimaryKeyGenState, PrimaryKey, SecondaryKeys, AvailableIndexes> {
    operations: OptimizedVec<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>>,
    queue_inner_wt: Arc<QueueInnerWorkTable>,
    last_events_ids: LastEventIds<AvailableIndexes>,
    last_invalid_batch_size: usize,
    page_limit: usize,
    /// Cycles since the engine last declared a batch failed. Drives only the
    /// give-up condition.
    attempts: usize,
    /// Cycles since the applied watermark last moved.
    ///
    /// Separate from `attempts` because the two questions are different. A
    /// batch can be valid and still apply nothing: everything in it sat behind
    /// a gap and was trimmed, and validation returns an empty batch rather than
    /// a deferral. That is a success, so it resets `attempts`, and widening the
    /// collection off `attempts` therefore never happened in exactly the case
    /// that needed it. Progress is what should widen the search.
    no_progress: usize,
    /// Shared with the queue that feeds this analyzer and with every
    /// `BatchOperation` it builds, so the event-gap guard can say which ids in
    /// a gap ever reached the queue. Diagnostics only: see
    /// [`crate::persistence::event_ledger`]. Detached until
    /// `attach_event_ledger` is called, which unit-test analyzers never do.
    event_ledger: Arc<EventLedger>,
}

/// How far each index's event stream has been applied.
///
/// `None` means nothing has been applied to that stream yet, and it has to be
/// a separate value rather than a reserved id. Event ids start at **0** and
/// `IndexChangeEventId::default()` is also 0, so using the id alone made
/// "nothing applied" indistinguishable from "applied event 0". The gap check
/// had to exempt the first batch to avoid deferring on that ambiguity, and the
/// exemption is what let a first batch of ids 3..=28 be applied ahead of
/// events 0..=2 and corrupt the index file.
#[derive(Debug)]
pub struct LastEventIds<AvailableIndexes> {
    pub primary_id: Option<IndexChangeEventId>,
    pub secondary_ids: HashMap<AvailableIndexes, Option<IndexChangeEventId>>,
}

impl<AvailableIndexes> LastEventIds<AvailableIndexes> {
    /// Whether `id` is the event this stream is waiting for.
    ///
    /// The first event of a stream is [`IndexChangeEventId::default`]; every
    /// later one must be the immediate successor of the last applied.
    pub fn follows(last: Option<IndexChangeEventId>, id: IndexChangeEventId) -> bool {
        match last {
            None => id == IndexChangeEventId::default(),
            Some(last) => id.is_next_for(last),
        }
    }
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
        // `None` is "this batch applied nothing to that stream", which must
        // not move the watermark backwards. Previously the same test was
        // `!= default`, which also discarded a genuine advance to event 0.
        if another.primary_id.is_some() {
            self.primary_id = another.primary_id
        }
        for (index, id) in another.secondary_ids {
            if id.is_some() || !self.secondary_ids.contains_key(&index) {
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
            no_progress: 0,
            event_ledger: Arc::new(EventLedger::detached()),
        }
    }

    /// Shares the feeding queue's event bookkeeping with this analyzer.
    ///
    /// Only the producer side records who pushed an event, so the analyzer has
    /// to read the *same* ledger the queue writes for its gap reports to mean
    /// anything.
    pub fn attach_event_ledger(&mut self, ledger: Arc<EventLedger>) {
        self.event_ledger = ledger;
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
        // The generated wrapper is async now; this is a synchronous internal
        // queue push, so it uses the inner table's insert directly. The inner
        // one is the same write without the async signature.
        self.queue_inner_wt.0.insert(row)?;
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
            .map(|(id, _)| id)
    }

    /// Records `stage` against every event id carried by `ops`.
    ///
    /// Diagnostics only. The whole body is skipped when bookkeeping is off,
    /// which keeps the `Debug` formatting of secondary index labels off the
    /// path of a release build entirely.
    fn record_ops_stage(&self, ops: &[Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>], stage: Stages)
    where
        SecondaryKeys: TableSecondaryIndexEventsOps<AvailableIndexes>,
    {
        if !event_ledger::enabled() {
            return;
        }
        for op in ops {
            if let Some(evs) = op.primary_key_events() {
                self.event_ledger
                    .record_stage_for_events(EventStream::Primary, evs, stage);
            }
            // `Stages::QUEUED` is unioned in for secondary streams because the
            // producer side records primary ids only: an operation reaching
            // the analyzer at all proves it was queued, and without this the
            // secondary gap report would call every id it knows about leaked.
            // The cost is that a secondary record carries no producer call
            // site, which the report prints as `<unrecorded>`.
            for (index, id) in op.secondary_key_events().iter_event_ids() {
                self.event_ledger.record_stage(
                    EventStream::Secondary(format!("{index:?}")),
                    id.inner(),
                    stage.union(Stages::QUEUED),
                );
            }
        }
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

        // See `COLLECT_WHOLE_QUEUE_AFTER_ATTEMPTS`: page grouping can wedge on a
        // stream whose event order and operation order disagree, so after a few
        // failed attempts the batch is assembled from everything queued.
        let took_whole_queue = self.no_progress >= COLLECT_WHOLE_QUEUE_AFTER_ATTEMPTS;
        if took_whole_queue {
            for (queued_op_id, _) in self.queue_inner_wt.0.indexes.operation_id_idx.iter() {
                ops_set.insert(queued_op_id);
            }
        }

        let mut next_op_id = op_id;
        let mut no_more_ops = took_whole_queue;
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
                    // Deduplicate before querying: a large multi-row operation
                    // has many rows per page, and issuing the page query per
                    // row instead of per page made batch collection quadratic
                    // in rows per page.
                    let multi_page_ids = ops_rows.iter().map(|r| r.page_id).collect::<HashSet<_>>();
                    used_page_ids.extend(multi_page_ids.iter().copied());
                    for page_id in multi_page_ids {
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
                    // A blocker was found: keep only operations *before* it.
                    // They are complete within the collected page set and can
                    // be applied now; the blocker and everything after it stay
                    // queued for the next collection cycle. Keeping the later
                    // operations instead (as this filter previously did with
                    // `>=`) applies a stream whose earlier event ids were
                    // dropped, which the event-gap validation then defers
                    // forever, failing the persistence worker after its
                    // attempt budget.
                    let ops_set_to_extend = if let Some(block_op_id) = block_op_id {
                        ops_set_to_extend
                            .into_iter()
                            .filter(|op_id| *op_id < block_op_id)
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
                next_op_id = id;
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
            // Inner insert: see the note in `push`.
            info_wt.0.insert(row)?;
            ops.push(op);
        }

        self.record_ops_stage(&ops, Stages::COLLECTED);
        let mut op = BatchOperation::new(ops, info_wt).with_event_ledger(self.event_ledger.clone());
        let invalid_for_this_batch_ops = op.validate(&self.last_events_ids, self.attempts).await?;
        if let Some(invalid_for_this_batch_ops) = invalid_for_this_batch_ops {
            self.record_ops_stage(&invalid_for_this_batch_ops, Stages::REQUEUED);
            self.extend_from_iter(invalid_for_this_batch_ops.into_iter())?;
            let previous_primary = self.last_events_ids.primary_id;
            let last_ids = op.get_last_event_ids();
            let advanced = last_ids.primary_id > previous_primary;
            if let Some(id) = last_ids.primary_id {
                self.event_ledger.record_applied_upto(EventStream::Primary, id.inner());
            }
            if event_ledger::enabled() {
                for (index, id) in &last_ids.secondary_ids {
                    if let Some(id) = id {
                        self.event_ledger
                            .record_applied_upto(EventStream::Secondary(format!("{index:?}")), id.inner());
                    }
                }
            }
            self.last_events_ids.merge(last_ids);
            self.last_invalid_batch_size = 0;
            self.page_limit = MAX_PAGE_AMOUNT;
            self.attempts = 0;
            if advanced {
                self.no_progress = 0;
            } else {
                self.no_progress += 1;
            }

            Ok(Some(op))
        } else {
            // can't collect batch for now
            let ops = op.ops();
            self.record_ops_stage(&ops, Stages::REQUEUED);
            self.attempts += 1;
            self.no_progress += 1;
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
    use core::sync::atomic::{AtomicUsize, Ordering};
    use hashbrown::HashMap;

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
            core::iter::empty()
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

    /// A single-row insert on `page`, carrying one primary event with
    /// `event_id`. The two ids are independent on purpose: that is the whole
    /// point of the regression below.
    fn insert_operation_with_event(id: u128, page: u32, event_id: u64) -> Operation<(), u64, TestEvents> {
        let link = Link {
            page_id: page.into(),
            offset: 0,
            length: 1,
        };
        Operation::Insert(InsertOperation {
            id: OperationId::Single(uuid::Uuid::from_u128(id)),
            pk_gen_state: (),
            primary_key_events: vec![indexset::cdc::change::ChangeEvent::InsertAt {
                event_id: event_id.into(),
                max_value: indexset::core::pair::Pair {
                    key: event_id,
                    value: link,
                },
                value: indexset::core::pair::Pair {
                    key: event_id,
                    value: link,
                },
                index: 0,
            }],
            secondary_keys_events: TestEvents,
            bytes: vec![id as u8],
            link,
        })
    }

    /// Regression: collection could not assemble a gapless event stream when
    /// event order and operation order disagree, and never recovered.
    ///
    /// Event ids are allocated during the index mutations; an operation id is
    /// minted later, at the push site. Two concurrent writers can therefore
    /// invert the two orders, and vacuum does it systematically, because its
    /// update lands on the destination page while inserts append to the
    /// current page. Observed in the field as 110 inversions in one run, in a
    /// regular alternating pattern.
    ///
    /// Page-grouped collection then never visits the page holding the missing
    /// id, so every retry rebuilt the same gapped batch until the attempt
    /// budget failed the engine and persistence for that table stopped for
    /// good. Before the whole-queue fallback this panics on the ninth attempt
    /// with "persistence stalled on primary index event gap".
    #[tokio::test]
    async fn collection_recovers_when_event_order_and_operation_order_disagree() {
        let queue_inner_wt = Arc::new(QueueInnerWorkTable::default());
        let mut analyzer: QueueAnalyzer<(), u64, TestEvents, TestIndex> = QueueAnalyzer::new(queue_inner_wt);
        analyzer.last_events_ids.primary_id = Some(1.into());

        // Collecting page 5 from operation 1 also takes operation 3, and
        // advances past it. Operation 2 sits between them in operation order,
        // on another page, and carries the event the stream needs next, so it
        // is skipped and then the walk runs out of operations entirely. The
        // page-limit growth that normally widens a stuck collection cannot
        // help here: the loop ended because it ran out, not because it was
        // full.
        analyzer.push(insert_operation_with_event(1, 5, 3)).unwrap();
        analyzer.push(insert_operation_with_event(2, 9, 2)).unwrap();
        analyzer.push(insert_operation_with_event(3, 5, 4)).unwrap();

        let start = OperationId::Single(uuid::Uuid::from_u128(1));
        for attempt in 0..12 {
            if analyzer
                .collect_batch_from_op_id(start)
                .await
                .expect("collection must not fail the engine over an ordering it can recover from")
                .is_some()
            {
                assert!(
                    attempt >= 1,
                    "the first attempt is expected to defer; progress on attempt 0 would mean \
                     the inversion was not reproduced"
                );
                return;
            }
        }
        panic!("collection never made progress: the gapped batch was rebuilt every time");
    }

    fn multi_insert_operation(id: u128, offset: u32, byte: u8) -> Operation<(), u64, TestEvents> {
        multi_insert_operation_on(1, id, offset, byte)
    }

    fn multi_insert_operation_on(page: u32, id: u128, offset: u32, byte: u8) -> Operation<(), u64, TestEvents> {
        Operation::Insert(InsertOperation {
            id: OperationId::Multi(uuid::Uuid::from_u128(id)),
            pk_gen_state: (),
            primary_key_events: vec![],
            secondary_keys_events: TestEvents,
            bytes: vec![byte; 8],
            link: Link {
                page_id: page.into(),
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
            batch.get(&PageId::from(1u32)).unwrap(),
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

    /// Regression: the blocker filter kept the operations *after* a blocking
    /// multi operation instead of the complete ones before it.
    ///
    /// Group A (Multi id 1) lives entirely on page 1. Group B (Multi id 2)
    /// shares page 1 but also spans page 2, so collecting from A finds B as a
    /// blocker. The collected batch must contain exactly group A — applying B
    /// while dropping A ships a stream whose earlier event ids never arrive,
    /// which the event-gap validation defers until the worker's attempt
    /// budget fails the engine.
    #[tokio::test]
    async fn blocked_multi_collection_applies_the_complete_earlier_group() {
        let queue_inner_wt = Arc::new(QueueInnerWorkTable::default());
        let mut analyzer: QueueAnalyzer<(), u64, TestEvents, TestIndex> = QueueAnalyzer::new(queue_inner_wt);
        analyzer.push(multi_insert_operation_on(1, 1, 0, 1)).unwrap();
        analyzer.push(multi_insert_operation_on(1, 1, 8, 2)).unwrap();
        analyzer.push(multi_insert_operation_on(1, 2, 16, 3)).unwrap();
        analyzer.push(multi_insert_operation_on(2, 2, 0, 4)).unwrap();

        let batch = analyzer
            .collect_batch_from_op_id(OperationId::Multi(uuid::Uuid::from_u128(1)))
            .await
            .unwrap()
            .unwrap()
            .get_batch_data_op()
            .unwrap();

        let page_one_writes = batch.get(&PageId::from(1u32)).unwrap();
        assert_eq!(
            page_one_writes,
            &vec![
                (
                    Link {
                        page_id: 1.into(),
                        offset: 0,
                        length: 8,
                    },
                    vec![1; 8],
                ),
                (
                    Link {
                        page_id: 1.into(),
                        offset: 8,
                        length: 8,
                    },
                    vec![2; 8],
                ),
            ],
            "the complete earlier group must be applied"
        );
        assert!(
            !batch.contains_key(&PageId::from(2u32)),
            "the blocking group must stay queued, not be applied without its earlier events"
        );
        assert_eq!(analyzer.len(), 2, "both rows of the blocked group remain queued");
    }

    #[tokio::test]
    async fn batched_intake_drains_multi_id_operations_in_one_engine_batch() {
        let batches = Arc::new(AtomicUsize::new(0));
        let task = PersistenceTask::run_engine(TestEngine {
            batches: batches.clone(),
            events: Arc::new(ParkingMutex::new(Vec::new())),
            config: TestConfig,
            failure: TestFailure::None,
        });

        task.apply_operations(vec![
            multi_insert_operation(1, 128, 1),
            multi_insert_operation(1, 136, 2),
            multi_insert_operation(1, 144, 3),
        ])
        .unwrap();
        task.close().await.unwrap();

        assert_eq!(
            batches.load(Ordering::Relaxed),
            1,
            "operations sharing one Multi id must reach the engine as one batch"
        );
    }

    #[tokio::test]
    async fn batched_intake_refuses_operations_after_failure() {
        let task = PersistenceTask::run_engine(TestEngine {
            batches: Arc::new(AtomicUsize::new(0)),
            events: Arc::new(ParkingMutex::new(Vec::new())),
            config: TestConfig,
            failure: TestFailure::Engine,
        });

        task.apply_operation(insert_operation(1)).unwrap();
        let wait_error = task.wait_for_ops().await.unwrap_err();

        let intake_error = task
            .apply_operations(vec![insert_operation(2), insert_operation(3)])
            .unwrap_err();
        assert!(Arc::ptr_eq(&wait_error, &intake_error));
    }

    /// Regression: `close` reported success having written nothing.
    ///
    /// The worker polled the queue, found it empty, and only then read the
    /// lifecycle state. An operation enqueued in that window was abandoned,
    /// because observing `Closing` returned without looking at the queue
    /// again. The caller had already been told the row was accepted, so this
    /// was silent data loss on a clean shutdown.
    ///
    /// A single-threaded runtime makes it deterministic rather than a race to
    /// lose: the worker advances only where this test yields, so it parks on
    /// the `cfg(test)` yield inside that window, and the enqueue and the close
    /// both land before it reads the state. The single-shot
    /// `close_drains_and_joins_the_engine` above passes either way.
    #[tokio::test]
    async fn close_never_abandons_an_operation_enqueued_as_it_begins() {
        let batches = Arc::new(AtomicUsize::new(0));
        let task = PersistenceTask::run_engine(TestEngine {
            batches: batches.clone(),
            events: Arc::new(ParkingMutex::new(Vec::new())),
            config: TestConfig,
            failure: TestFailure::None,
        });

        // Drive the worker to its idle poll, where it parks inside the window.
        nagoya::yield_now().await;

        task.apply_operation(insert_operation(1)).unwrap();
        task.close().await.unwrap();

        assert_eq!(
            batches.load(Ordering::Relaxed),
            1,
            "close returned Ok without applying an operation enqueued as it began"
        );
    }

    /// Regression: `close` hung forever on an idle worker.
    ///
    /// `pop_marking_in_progress` created its `Notified` future but never
    /// enabled it before draining the queue and reading the lifecycle state.
    /// `close` wakes the worker with `notify_waiters`, which stores no permit,
    /// so a wake landing between that state read and the first poll of the
    /// future was lost. The worker then parked on a notification that could
    /// never come again: the state had left `Running`, so no push (the only
    /// `notify_one` source) would ever arrive. `close` awaited the worker's
    /// join handle and hung with it.
    ///
    /// The window is a few instructions wide, so the test parks the popping
    /// task inside it with the semaphore gate, lands the `Closing` transition
    /// and the wake exactly there, and only then lets the pop run on to its
    /// await. The timeout bounds a regression to a test failure, not a hang.
    #[tokio::test]
    async fn wake_landing_inside_the_pop_race_window_is_not_lost() {
        let lifecycle = Arc::new(PersistenceLifecycle::new());
        let mut queue = Queue::<(), u64, TestEvents>::new(lifecycle.clone(), "tests/queue");
        let gate = Arc::new(PopRaceWindowGate::new());
        queue.pop_race_window_gate = Some(gate.clone());
        let queue = Arc::new(queue);

        let popping_queue = queue.clone();
        let popping = tokio::spawn(async move {
            let in_progress = AtomicBool::new(false);
            popping_queue.pop_marking_in_progress(&in_progress).await
        });

        gate.wait_entered().await;
        lifecycle.begin_close().unwrap();
        queue.wake();
        gate.release();

        let popped = tokio::time::timeout(Duration::from_secs(5), popping)
            .await
            .expect("pop hung: the shutdown wake landed in the race window and was lost")
            .unwrap();
        assert!(popped.is_none(), "an empty closing queue must pop None");
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

    /// A worker that stops publishes a terminal state instead of leaving its
    /// waiters parked, and refuses operations afterwards.
    ///
    /// This used to build a tokio runtime, spawn the engine onto it and drop
    /// the runtime out from under the worker. That worked because
    /// `tokio::spawn` picked up whatever runtime the caller happened to be on,
    /// and it is no longer how the worker is scheduled: it runs on the
    /// engine's own pool, so tearing down a caller's runtime leaves it
    /// running. Deliberately. A flush loop that dies because its caller
    /// dropped an unrelated runtime loses writes it had already accepted.
    ///
    /// So the shutdown under test is the one that still exists: `Drop` on an
    /// idle task, with `monitor()` for the waiter that has to outlive it.
    ///
    /// Both terminal outcomes are accepted, because which one happens is a
    /// genuine race rather than a fact about the engine. `Drop` wakes the
    /// queue and then cancels; if a pool thread polls the worker inside that
    /// window it sees `Closing` and closes cleanly, otherwise the cancellation
    /// lands first and the completion guard reports it. Asserting one of them
    /// would be asserting who won.
    #[tokio::test]
    async fn a_stopped_worker_is_terminal_and_rejects_later_operations() {
        let task = PersistenceTask::run_engine(TestEngine {
            batches: Arc::new(AtomicUsize::new(0)),
            events: Arc::new(ParkingMutex::new(Vec::new())),
            config: TestConfig,
            failure: TestFailure::None,
        });
        nagoya::yield_now().await;

        let monitor = task.monitor();
        let sink = task.vacuum_sink();
        drop(task);

        let outcome = tokio::time::timeout(Duration::from_secs(1), monitor.wait_for_failure())
            .await
            .expect("a stopped worker must notify terminal waiters");

        match &outcome {
            Ok(()) => {}
            Err(error) => assert_eq!(
                error.to_string(),
                "persistence engine failed: persistence worker was cancelled"
            ),
        }

        // Terminal either way means no further operation is accepted. The
        // queue outlives the task through `vacuum_sink`, which is exactly the
        // path that made this worth asserting: a push accepted here would be
        // acknowledged to a caller and then never written.
        let intake_error = sink
            .reclaim_pages(vec![1.into()])
            .expect_err("a terminal engine must refuse operations");
        if let Err(wait_error) = &outcome {
            assert!(Arc::ptr_eq(wait_error, &intake_error));
        }
    }

    /// Regression: an operation pushed while `Drop` ran was accepted, then
    /// silently lost.
    ///
    /// `Drop` checked the idle triggers and aborted the worker while the
    /// lifecycle still said `Running`, and the queue stays reachable through
    /// `vacuum_sink` clones. A push landing between the idle check and the
    /// abort was accepted — the caller told its row was on its way to disk —
    /// and then lost with the aborted worker. The lifecycle must leave
    /// `Running` before the abort so such a push is refused instead.
    #[tokio::test]
    async fn drop_refuses_vacuum_operations_instead_of_losing_them() {
        let task = PersistenceTask::run_engine(TestEngine {
            batches: Arc::new(AtomicUsize::new(0)),
            events: Arc::new(ParkingMutex::new(Vec::new())),
            config: TestConfig,
            failure: TestFailure::None,
        });
        // Let the worker reach its idle poll so `Drop` takes the abort path.
        nagoya::yield_now().await;

        let sink = task.vacuum_sink();
        drop(task);

        // No await between the drop and this push: the refusal must be
        // synchronous with `Drop`, not an eventual effect of the abort.
        sink.reclaim_pages(vec![1.into()])
            .expect_err("a push racing Drop must be refused, not accepted into a dead queue");
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

/// Holds `pop_marking_in_progress` open inside its race window — after its
/// lifecycle state read, before its `notified.await` — so a test can land a
/// wake there deterministically. Semaphore permits are retained, unlike
/// `notify_waiters`, so the gate itself cannot miss a signal.
#[cfg(test)]
#[derive(Debug)]
struct PopRaceWindowGate {
    entered: nagoya::sync::Semaphore,
    proceed: nagoya::sync::Semaphore,
}

#[cfg(test)]
impl PopRaceWindowGate {
    fn new() -> Self {
        Self {
            entered: nagoya::sync::Semaphore::new(0),
            proceed: nagoya::sync::Semaphore::new(0),
        }
    }

    /// Called by the popping task inside the window: reports entry, then
    /// blocks until [`Self::release`].
    async fn pause(&self) {
        self.entered.add_permits(1);
        // No `expect` here any more: nagoya's semaphore has no closed state,
        // so `acquire` yields the permit rather than a `Result`. The panic
        // this used to carry was for a case that cannot arise.
        self.proceed.acquire().await.forget();
    }

    /// Waits until the popping task is parked inside the window.
    async fn wait_entered(&self) {
        self.entered.acquire().await.forget();
    }

    /// Lets the popping task run on from the window.
    fn release(&self) {
        self.proceed.add_permits(1);
    }
}

/// Primary index event ids lifted off an operation before it is moved into the
/// queue, so they can be recorded once the push is known to have been accepted.
///
/// Primary only: `Queue` is generic over the secondary event type with no bound
/// that could iterate it, and the primary stream is the one whose gap guard
/// stalls the engine. The analyzer records secondary ids at collection time,
/// where that bound does exist.
///
/// Cheap when bookkeeping is off: `EventLedger::event_ids` returns an empty
/// `Vec`, which allocates nothing, and the rest is two `Copy` field reads.
struct QueuedEventIds {
    ids: Vec<u64>,
    op_id: OperationId,
    op_type: OperationType,
}

impl QueuedEventIds {
    fn of<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>(
        value: &Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>,
    ) -> Self {
        Self {
            ids: value
                .primary_key_events()
                .map(|evs| EventLedger::event_ids(evs.as_slice()))
                .unwrap_or_default(),
            op_id: value.operation_id(),
            op_type: value.operation_type(),
        }
    }

    fn record(&self, ledger: &EventLedger, site: &'static Location<'static>) {
        ledger.record_queued(EventStream::Primary, &self.ids, self.op_id, self.op_type, site);
    }
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
    /// Producer-side half of the event-gap bookkeeping: every operation that
    /// reaches persistence passes through this queue, so an event id the
    /// engine is waiting for that never appears here was assigned by the index
    /// and dropped before it was queued. Shared with the analyzer, which reads
    /// it when the gap guard fires. Diagnostics only, and inert unless
    /// [`event_ledger::enabled`].
    event_ledger: Arc<EventLedger>,
    #[cfg(test)]
    pop_race_window_gate: Option<alloc::sync::Arc<PopRaceWindowGate>>,
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> Queue<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
    fn new(lifecycle: Arc<PersistenceLifecycle>, table_path: &str) -> Self {
        Self {
            queue: ParkingMutex::new(VecDeque::new()),
            notify: Notify::new(),
            len: Arc::new(AtomicUsize::new(0)),
            lifecycle,
            event_ledger: Arc::new(EventLedger::new(table_path)),
            #[cfg(test)]
            pop_race_window_gate: None,
        }
    }

    /// The event bookkeeping this queue writes, for sharing with the analyzer.
    pub fn event_ledger(&self) -> Arc<EventLedger> {
        self.event_ledger.clone()
    }

    /// Enqueues one operation, naming the producer's call site.
    ///
    /// The site is passed explicitly rather than taken with `#[track_caller]`,
    /// because that only reaches one frame up: a wrapper that wants its own
    /// caller named in a gap report has to forward a location through here.
    /// through here rather than call `push` and lose it.
    pub fn push_at(
        &self,
        value: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>,
        site: &'static Location<'static>,
    ) -> PersistenceResult {
        // The ids have to be lifted out before the operation is moved into the
        // queue, but they are only recorded once the push is accepted: a
        // refused push (the engine is closing or already failed) genuinely
        // does not queue its events, and recording it as queued would hide
        // exactly that leak mode.
        let queued = QueuedEventIds::of(&value);
        self.push_message(PersistenceMessage::Operation(value))?;
        queued.record(&self.event_ledger, site);
        Ok(())
    }

    /// Enqueues a whole batch under one lifecycle check, one queue lock and one
    /// worker wake-up, so a caller producing many operations at once
    /// (`insert_many`) pays the intake overhead once instead of per row.
    /// All-or-nothing: either every operation is accepted or none is. Takes the
    /// producer's call site for the same reason as [`Queue::push_at`].
    pub fn push_many_at(
        &self,
        values: Vec<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>>,
        site: &'static Location<'static>,
    ) -> PersistenceResult {
        if values.is_empty() {
            return Ok(());
        }
        let queued = values.iter().map(QueuedEventIds::of).collect::<Vec<_>>();
        let state = self.lifecycle.state.lock();
        match &*state {
            PersistenceState::Running => {}
            PersistenceState::Closing => return Err(Arc::new(PersistenceError::Closing)),
            PersistenceState::Closed => return Err(Arc::new(PersistenceError::Closed)),
            PersistenceState::Failed(error) => return Err(error.clone()),
        }
        self.len.fetch_add(values.len(), Ordering::Release);
        self.queue
            .lock()
            .extend(values.into_iter().map(PersistenceMessage::Operation));
        self.notify.notify_one();
        drop(state);
        // Recorded after acceptance, for the reason given in `push_at`.
        for queued in &queued {
            queued.record(&self.event_ledger, site);
        }
        Ok(())
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
            let mut notified = core::pin::pin!(notified);
            // `wake()` uses `notify_waiters`, which stores no permit: only a
            // waiter that already exists observes it. Register this waiter
            // before draining the queue and reading the lifecycle state, so a
            // `close()` that transitions to `Closing` and wakes inside that
            // window cannot be lost forever — no further push will ever
            // arrive to deliver a permit once the state has left `Running`.
            // Creating the future up here is what carries the guarantee
            // (`Notified` snapshots the `notify_waiters` generation at
            // creation); the explicit `enable` additionally registers for
            // `notify_one` and keeps the registration point obvious, the same
            // pattern as `PersistenceMonitor::wait_for_failure`.
            notified.as_mut().enable();
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

            // The window between the state read above and the await below is
            // where a `close()` wake could previously vanish. It is a few
            // instructions wide, so a test cannot land in it by luck; this
            // gate holds it open. Unit tests only, inert unless installed.
            #[cfg(test)]
            if let Some(gate) = &self.pop_race_window_gate {
                gate.pause().await;
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
        // `Location::caller()` without `#[track_caller]` resolves to this line
        // rather than to vacuum's call site. That is deliberate: the trait
        // declaration lives outside this module and cannot be annotated, and
        // this line is already a unique producer label, because `apply_move`
        // is only ever reached from a vacuum row move.
        self.push_at(
            Operation::Update(UpdateOperation {
                id: OperationId::Single(uuid::Uuid::now_v7()),
                primary_key_events,
                secondary_keys_events,
                bytes,
                link: new_link,
            }),
            Location::caller(),
        )
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
        match self.engine_task_handle.as_ref() {
            None => return,
            Some(handle) if handle.is_finished() => return,
            Some(_) => {}
        }
        if matches!(
            self.lifecycle.state(),
            PersistenceState::Failed(_) | PersistenceState::Closed
        ) {
            return;
        }
        // Leave `Running` before deciding anything else. `push_message`
        // accepts operations while the state is `Running`, and the queue
        // stays reachable through `vacuum_sink` clones after this task is
        // gone, so an operation pushed between the idle check below and the
        // abort would be accepted and then silently lost with the aborted
        // worker. After `begin_close` a push is refused with the standard
        // shutdown error before it can be accepted into a doomed queue.
        if self.lifecycle.begin_close().is_err() {
            // Failed terminally since the check above; the worker exits on
            // its own and pushes are already refused.
            return;
        }
        self.queue.wake();
        if self.check_wait_triggers() {
            // `cancel` consumes the handle, where `abort` took `&self`. Taking
            // the field is the whole difference, and it is safe here because
            // this task is being dropped and nothing reads the handle again.
            if let Some(handle) = self.engine_task_handle.take() {
                handle.cancel();
            }
        } else {
            tracing::error!(
                "PersistenceTask dropped with work in flight; the engine task keeps draining detached and                  then stops, but its errors can no longer be observed. Call close() (or wait_for_ops()                  before dropping) to guarantee a clean shutdown."
            );
        }
    }
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys, AvailableIndexes>
    PersistenceTask<PrimaryKeyGenState, PrimaryKey, SecondaryKeys, AvailableIndexes>
{
    /// `#[track_caller]` so an event-gap report names the producer that
    /// pushed the operation rather than this forwarding line.
    #[track_caller]
    pub fn apply_operation(&self, op: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>) -> PersistenceResult {
        self.queue.push_at(op, Location::caller())
    }

    /// Enqueues a batch of operations atomically with a single worker
    /// wake-up. See [`Queue::push_many`].
    #[track_caller]
    pub fn apply_operations(
        &self,
        ops: Vec<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>>,
    ) -> PersistenceResult {
        self.queue.push_many_at(ops, Location::caller())
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
    pub async fn persisted_data_file_size_bytes(&self) -> Result<u64, crate::fsx::Error> {
        // `metadata` answers with the length, which is the only thing anything
        // here ever asked a metadata handle for.
        crate::fsx::metadata(format!(
            "{}/{}",
            self.table_path.trim_end_matches('/'),
            WT_DATA_EXTENSION
        ))
        .await
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
        let queue = Arc::new(Queue::new(lifecycle.clone(), &table_path));

        let engine_queue = queue.clone();
        let engine_lifecycle = lifecycle.clone();
        let analyzer_inner_wt: Arc<QueueInnerWorkTable> = Default::default();
        let mut analyzer = QueueAnalyzer::new(analyzer_inner_wt.clone());
        // Producer and consumer must share one ledger: the queue records who
        // pushed an event, the analyzer's gap guard reads it back.
        analyzer.attach_event_ledger(queue.event_ledger());
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
                    engine_lifecycle.progress_notify.notify_waiters();
                    // The gap between the poll above and the state read below
                    // is where an operation can be enqueued and lost. It is a
                    // few instructions wide, so a test cannot land in it by
                    // luck; this yield holds it open. Unit tests only, and it
                    // changes scheduling rather than behaviour.
                    #[cfg(test)]
                    nagoya::yield_now().await;
                    if matches!(engine_lifecycle.state(), PersistenceState::Closing) {
                        // Re-check the queue before giving up on it. An
                        // operation can be enqueued between the poll above and
                        // this read of the state, and returning on that stale
                        // emptiness abandons it: `close` then reports success
                        // having written nothing, losing a row the caller was
                        // told had been accepted. A push is refused once the
                        // state is `Closing`, so a queue seen empty here cannot
                        // fill again, and this terminates.
                        match engine_queue.immediate_pop() {
                            Some(message) => {
                                task_analyzer_in_progress.store(true, Ordering::Release);
                                Some(message)
                            }
                            None => {
                                engine_lifecycle.finish_close();
                                return;
                            }
                        }
                    } else {
                        // The pop sets the flag back to `true` atomically with the
                        // dequeue, so waiters never observe an empty queue with an
                        // idle analyzer while an operation is in flight.
                        engine_queue.pop_marking_in_progress(&task_analyzer_in_progress).await
                    }
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
                        nagoya::sleep(Duration::from_millis(500)).await;
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
        // Constructed outside the async block so cancellation before its first
        // poll still drops the guard and publishes terminal failure.
        let completion_guard = WorkerCompletionGuard::new(lifecycle.clone());
        let task = async move {
            worker.await;
            completion_guard.disarm();
        };
        // This worker is the engine's business, not the caller's: a flush loop
        // drains a queue, so unlike the vacuum sweep there is no foreground
        // task it could be folded into. It needs a thread whether or not
        // anything else does, which is why an ambient runtime was reached for
        // in the first place, and `nagoya::runtime::background` is that same
        // convenience with the gate tokio's global never had.
        //
        // Which pool that is, is a process-level choice rather than a hardcoded
        // one: see `runtime::engine_executor`. Leaving it pinned to the
        // locality pool while a benchmark moved its client tasks elsewhere
        // would put the two halves of the stack on different schedulers.
        let engine_task_handle = crate::runtime::engine_executor().spawn(task);
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

    /// Wait until the queue and the analyzer are both idle.
    ///
    /// Note the seam against a concurrent `close`: the worker clears its
    /// in-progress flag and notifies waiters before it re-checks the queue, so
    /// a `wait_for_ops` racing a close can observe idle-and-empty and return
    /// while the operation the close is about to drain is still in flight.
    /// `close` persists it regardless, so this is a reporting nuance rather
    /// than a durability hole, but do not treat `wait_for_ops` returning during
    /// a shutdown as proof that everything is on disk. `close` returning `Ok`
    /// is that proof.
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

            // A `tokio::select!` racing the notify against a sleep, which is
            // what a timeout is. The second arm exists so a wake lost to a
            // race still gets re-checked, not to measure anything.
            let _ = nagoya::timeout(Duration::from_secs(1), self.lifecycle.progress_notify.notified()).await;
        }
    }

    /// Returns a cloneable monitor independent of this task's ownership.
    pub fn monitor(&self) -> PersistenceMonitor {
        PersistenceMonitor {
            lifecycle: self.lifecycle.clone(),
        }
    }

    /// Waits until the worker fails or closes.
    ///
    /// Prefer [`Self::monitor`] when another task must keep waiting while this
    /// task is moved into [`Self::close`].
    pub async fn wait_for_failure(&self) -> PersistenceResult {
        self.monitor().wait_for_failure().await
    }

    pub async fn close(mut self) -> PersistenceResult {
        let begin_result = self.lifecycle.begin_close();
        self.queue.wake();

        // `None` is cancellation. Note what this no longer catches: tokio's
        // `JoinError` also reported a *panic* in the worker, and nagoya's
        // handle does not, because the panic propagates out of the await
        // instead. A panicking worker therefore unwinds through this call
        // rather than arriving as a terminal error.
        if let Some(handle) = self.engine_task_handle.take()
            && handle.await.is_none()
        {
            return Err(self
                .lifecycle
                .fail(eyre::eyre!("persistence engine task was cancelled before it closed")));
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
