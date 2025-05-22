use std::fmt::Debug;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use data_bucket::page::PageId;
use tokio::sync::{Notify, RwLock};
use uuid::Uuid;
use worktable_codegen::worktable;

use crate::persistence::PersistenceEngineOps;
use crate::prelude::*;
use crate::util::OptimizedVec;

worktable! (
    name: QueueInner,
    columns: {
        id: u64 primary_key autoincrement,
        operation_id: Uuid,
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

pub struct QueueAnalyzer<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
    operations: OptimizedVec<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>>,
    queue_inner_wt: QueueInnerWorkTable,
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
    QueueAnalyzer<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
{
    pub fn new() -> Self {
        Self {
            operations: OptimizedVec::with_capacity(256),
            queue_inner_wt: QueueInnerWorkTable::default(),
        }
    }

    pub fn push(&mut self, value: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>) -> eyre::Result<()>{
        let link = value.link();
        let mut row = QueueInnerRow {
            id: self.queue_inner_wt.get_next_pk().into(),
            operation_id: value.operation_id().into(),
            page_id: link.page_id.into(),
            link,
            pos: 0,
        };
        let pos = self.operations.push(value);
        row.pos = pos;
        self.queue_inner_wt.insert(row)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Queue<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
    queue: lockfree::queue::Queue<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>>,
    notify: Notify,
    len: AtomicU16,
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
    Queue<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
{
    pub fn new() -> Self {
        Self {
            queue: lockfree::queue::Queue::new(),
            notify: Notify::new(),
            len: AtomicU16::new(0),
        }
    }

    pub fn push(&self, value: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>) {
        self.queue.push(value);
        self.len.fetch_add(1, Ordering::Relaxed);
        self.notify.notify_one();
    }

    pub async fn pop(&self) -> Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
        loop {
            // Drain values
            if let Some(value) = self.queue.pop() {
                self.len.fetch_sub(1, Ordering::Relaxed);
                return value;
            }

            // Wait for values to be available
            self.notify.notified().await;
        }
    }

    pub fn immediate_pop(
        &self,
    ) -> Option<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>> {
        if let Some(v) = self.queue.pop() {
            self.len.fetch_sub(1, Ordering::Relaxed);
            Some(v)
        } else {
            None
        }
    }

    pub fn pop_iter(
        &self,
    ) -> impl Iterator<Item = Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>> {
        self.queue.pop_iter()
    }

    pub async fn wait_for_available(&self) {
        self.notify.notified().await;
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed) as usize
    }
}

#[derive(Debug)]
pub struct PersistenceTask<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
    #[allow(dead_code)]
    engine_task_handle: tokio::task::AbortHandle,
    queue: Arc<Queue<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>>,
    progress_notify: Arc<Notify>,
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
    PersistenceTask<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
{
    pub fn apply_operation(&self, op: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>) {
        self.queue.push(op);
    }

    pub fn run_engine<E>(mut engine: E) -> Self
    where
        E: PersistenceEngineOps<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> + Send + 'static,
        SecondaryKeys: Debug + Send + 'static,
        PrimaryKeyGenState: Debug + Send + 'static,
        PrimaryKey: Debug + Send + 'static,
    {
        let queue = Arc::new(Queue::new());
        let progress_notify = Arc::new(Notify::new());

        let engine_queue = queue.clone();
        let engine_progress_notify = progress_notify.clone();
        let task = async move {
            let analyzer = QueueAnalyzer::new();
            loop {
                engine_queue.wait_for_available().await;
                let ops_available_iter = engine_queue.pop_iter();
                
                let next_op = if let Some(next_op) = engine_queue.immediate_pop() {
                    next_op
                } else {
                    engine_progress_notify.notify_waiters();
                    engine_queue.pop().await
                };
                tracing::debug!("Applying operation {:?}", next_op);
                let res = engine.apply_operation(next_op).await;
                if let Err(err) = res {
                    tracing::warn!("{}", err);
                }
            }
        };
        let engine_task_handle = tokio::spawn(task).abort_handle();
        Self {
            queue,
            engine_task_handle,
            progress_notify,
        }
    }

    pub async fn wait_for_ops(&self) {
        let count = self.queue.len();
        tracing::info!("Waiting for {} operations", count);
        self.progress_notify.notified().await
    }
}
