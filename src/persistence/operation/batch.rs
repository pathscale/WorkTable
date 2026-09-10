use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::{string::ToString, vec::Vec};
use core::fmt::Debug;
use core::hash::Hash;
use core::marker::PhantomData;
use hashbrown::HashMap;

use data_bucket::page::PageId;
use data_bucket::{Link, SizeMeasurable};
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;
use worktable_codegen::{MemStat, worktable};

use crate::persistence::OperationType;
use crate::persistence::event_ledger::{self, EventLedger, EventStream, Stages};
use crate::persistence::space::{BatchChangeEvent, BatchData};
use crate::persistence::task::{LastEventIds, QueueInnerRow};
use crate::prelude::*;
use crate::prelude::{Order, SelectQueryExecutor};

/// Cycles of a persistently gapped event stream before the engine gives up and
/// fails the table.
///
/// A gap is usually transient: the operation carrying the missing id has been
/// pushed but not yet batched, or its producer has not reached its push. Each
/// deferral sleeps 500ms in the worker loop, so this is about a minute of
/// waiting. The previous value of eight was about four seconds, which a
/// producer descheduled under load can lose, and the engine then blamed a
/// permanent bug for what was a slow thread.
///
/// Widening the collection is a *separate* decision, taken far sooner and
/// tracked by the analyzer's own no-progress counter. This one only decides
/// when to stop hoping.
const GIVE_UP_AFTER_ATTEMPTS: usize = 120;

// Ephemeral metadata rebuilt for every persistence batch, not a persisted
// schema. One Multi operation deliberately owns several rows, so operation_id
// is non-unique while pos is the unique association back to the ops vector.
worktable! (
    name: BatchInner,
    columns: {
        id: u64 primary_key autoincrement,
        operation_id: OperationId,
        page_id: PageId,
        link: Link,
        op_type: OperationType,
        pos: usize,
    },
    indexes: {
        operation_id_idx: operation_id using worktables_index,
        page_id_idx: page_id using worktables_index,
        link_idx: link using worktables_index,
        op_type_idx: op_type using worktables_index,
        pos_idx: pos unique,
    },
    queries: {
        update: {
            PosById(pos) by id,
        }
    }
);

impl BatchInnerWorkTable {
    pub fn iter_links(&self) -> impl Iterator<Item = Link> {
        self.0
            .indexes
            .link_idx
            .iter()
            .map(|(l, _)| l)
            .collect::<Vec<_>>()
            .into_iter()
    }
}

impl From<QueueInnerRow> for BatchInnerRow {
    fn from(value: QueueInnerRow) -> Self {
        BatchInnerRow {
            id: value.id,
            operation_id: value.operation_id,
            page_id: value.page_id,
            link: value.link,
            op_type: Default::default(),
            pos: 0,
        }
    }
}

/// Coalesces durable row writes by physical storage slot and preserves their
/// creation order.
///
/// `Link::length` can change when an unsized row is reinserted into a reused
/// `(page_id, offset)`. Treating the two lengths as different keys leaves
/// overlapping writes in the same batch. The newest operation must be the only
/// write for an identical physical start, and writes at different starts must
/// still be applied oldest-to-newest: range splitting can make them overlap.
/// WorkTable-generated operation IDs use `Uuid::now_v7`, whose shared process
/// context guarantees creation-order sorting even within one millisecond;
/// callers constructing `Operation` values manually must preserve that
/// ordering contract.
fn latest_data_writes<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>(
    ops: &[Operation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>],
) -> BatchData {
    type PhysicalSlot = (PageId, u32);

    fn collect_in_order<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>(
        ops: &[Operation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>],
        order: impl Iterator<Item = usize> + Clone,
    ) -> BatchData {
        let mut latest: HashMap<PhysicalSlot, usize> = HashMap::with_capacity(ops.len());
        for sequence in order.clone() {
            let op = &ops[sequence];
            if op.bytes().is_some() {
                let link = op.link();
                latest.insert((link.page_id, link.offset), sequence);
            }
        }

        let mut ordered = HashMap::new();
        for sequence in order {
            let op = &ops[sequence];
            let Some(bytes) = op.bytes() else {
                continue;
            };
            let link = op.link();
            if latest.get(&(link.page_id, link.offset)) != Some(&sequence) {
                continue;
            }
            ordered
                .entry(link.page_id)
                .or_insert_with(Vec::new)
                .push((link, bytes.to_vec()));
        }
        ordered
    }

    // The analyzer already establishes this order. Keep that production path
    // linear; only defensive callers that construct an unsorted BatchOperation
    // pay for an index sort.
    if ops
        .windows(2)
        .all(|pair| pair[0].operation_id() <= pair[1].operation_id())
    {
        collect_in_order(ops, 0..ops.len())
    } else {
        let mut order = (0..ops.len()).collect::<Vec<_>>();
        order.sort_unstable_by_key(|sequence| (ops[*sequence].operation_id(), *sequence));
        collect_in_order(ops, order.into_iter())
    }
}

#[derive(Debug)]
pub struct BatchOperation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents, AvailableIndexes> {
    ops: Vec<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>>,
    info_wt: BatchInnerWorkTable,
    /// Event bookkeeping shared with the queue that produced `ops`, read by
    /// the event-gap guard in `validate` so a stall names its own cause.
    /// Diagnostics only, and `None` for batches built outside the analyzer.
    event_ledger: Option<Arc<EventLedger>>,
    prepared_index_evs: Option<PreparedIndexEvents<PrimaryKey, SecondaryEvents>>,
    phantom_data: PhantomData<AvailableIndexes>,
}

#[derive(Debug)]
pub struct PreparedIndexEvents<PrimaryKey, SecondaryEvents> {
    primary_evs: Vec<ChangeEvent<Pair<PrimaryKey, Link>>>,
    secondary_evs: SecondaryEvents,
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryEvents, AvailableIndexes>
    BatchOperation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents, AvailableIndexes>
where
    PrimaryKeyGenState: Debug + Clone,
    PrimaryKey: Debug + Clone,
    SecondaryEvents: Debug,
{
    pub fn new(
        ops: Vec<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>>,
        info_wt: BatchInnerWorkTable,
    ) -> Self {
        Self {
            ops,
            info_wt,
            event_ledger: None,
            prepared_index_evs: None,
            phantom_data: PhantomData,
        }
    }

    /// Attaches the analyzer's event bookkeeping, so the gap guard below can
    /// say which ids in a gap ever reached the persistence queue.
    ///
    /// A builder method rather than a `new` parameter, so every existing
    /// caller of `new` keeps working unchanged and the batch stays usable
    /// without any bookkeeping at all.
    pub fn with_event_ledger(mut self, ledger: Arc<EventLedger>) -> Self {
        self.event_ledger = Some(ledger);
        self
    }

    /// Remove metadata immediately after `self.ops.remove(removed_pos)`.
    ///
    /// At entry, `self.ops.len()` is already one shorter while `info_wt` still
    /// has the old positions. Shifting upward positions in ascending order
    /// keeps every unique destination vacant as it is filled.
    async fn remove_info_at_pos(&self, removed_pos: usize) -> eyre::Result<()> {
        let row = self
            .info_wt
            .select_by_pos(removed_pos)
            .ok_or_else(|| eyre::eyre!("batch metadata position {removed_pos} is missing"))?;
        self.info_wt.delete_without_lock::<_>(row.id).await?;

        for old_pos in (removed_pos + 1)..=self.ops.len() {
            let row = self
                .info_wt
                .select_by_pos(old_pos)
                .ok_or_else(|| eyre::eyre!("batch metadata position {old_pos} is missing during reindex"))?;
            self.info_wt
                .update_pos_by_id(PosByIdQuery { pos: old_pos - 1 }, row.id)
                .await?;
        }
        Ok(())
    }
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryEvents, AvailableIndexes>
    BatchOperation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents, AvailableIndexes>
where
    PrimaryKeyGenState: Debug + Clone,
    PrimaryKey: Debug + Clone,
    SecondaryEvents: Debug + Default + Clone + TableSecondaryIndexEventsOps<AvailableIndexes>,
    AvailableIndexes: Debug + Clone + Copy + Hash + Eq,
{
    pub fn ops(self) -> Vec<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>> {
        self.ops
    }

    async fn remove_operations_from_events(
        &mut self,
        invalid_events: PreparedIndexEvents<PrimaryKey, SecondaryEvents>,
    ) -> eyre::Result<Vec<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>>> {
        let mut removed_ops = Vec::new();

        for ev in &invalid_events.primary_evs {
            if let Some(operation_pos_rev) = self.ops.iter().rev().position(|op| {
                if let Some(evs) = op.primary_key_events() {
                    for inner_ev in evs {
                        if inner_ev.id() == ev.id() {
                            return true;
                        }
                    }
                    false
                } else {
                    false
                }
            }) {
                let pos = self.ops.len() - (operation_pos_rev + 1);
                let op = self.ops.remove(pos);
                self.remove_info_at_pos(pos).await?;
                removed_ops.push(op);
            }
        }
        let secondary_event_ids = invalid_events.secondary_evs.iter_event_ids().collect::<Vec<_>>();
        for (index, id) in secondary_event_ids {
            if let Some(operation_pos_rev) = self.ops.iter().rev().position(|op| {
                let evs = op.secondary_key_events();
                evs.contains_event(index, id)
            }) {
                let pos = self.ops.len() - (operation_pos_rev + 1);
                let op = self.ops.remove(pos);
                self.remove_info_at_pos(pos).await?;
                removed_ops.push(op);
            };
            // else it was already removed with primary
        }
        for op in &removed_ops {
            let prepared_evs = self
                .prepared_index_evs
                .as_mut()
                .expect("should be set before 0 iteration");
            if let Some(primary_evs) = op.primary_key_events() {
                for ev in primary_evs {
                    if let Ok(pos) = prepared_evs
                        .primary_evs
                        .binary_search_by(|inner_ev| inner_ev.id().cmp(&ev.id()))
                    {
                        prepared_evs.primary_evs.remove(pos);
                    }
                }
            }
            let op_secondary = op.secondary_key_events();
            prepared_evs.secondary_evs.remove(op_secondary);
        }

        self.record_stage(&removed_ops, Stages::TRIMMED);

        Ok(removed_ops)
    }

    /// Records `stage` against every event id carried by `ops`.
    ///
    /// Diagnostics only. Skipped entirely when bookkeeping is off, which keeps
    /// the `Debug` formatting of secondary index labels out of release builds.
    fn record_stage(&self, ops: &[Operation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>], stage: Stages) {
        let Some(ledger) = &self.event_ledger else {
            return;
        };
        if !event_ledger::enabled() {
            return;
        }
        for op in ops {
            if let Some(evs) = op.primary_key_events() {
                ledger.record_stage_for_events(EventStream::Primary, evs, stage);
            }
            // See the matching note in `QueueAnalyzer::record_ops_stage`: the
            // producer side records primary ids only, so an id observed on a
            // secondary stream here is known to have been queued.
            for (index, id) in op.secondary_key_events().iter_event_ids() {
                ledger.record_stage(
                    EventStream::Secondary(format!("{index:?}")),
                    id.inner(),
                    stage.union(Stages::QUEUED),
                );
            }
        }
    }

    /// The bookkeeping's account of a gap, or a note saying there is none.
    fn gap_report(
        &self,
        stream: &EventStream,
        last_applied: Option<IndexChangeEventId>,
        next_available: IndexChangeEventId,
    ) -> String {
        match &self.event_ledger {
            // Nothing applied yet reports as `0`, which is what the ledger
            // means by "everything from the start is missing": the first id is
            // 0, so there is no id below it to name.
            Some(ledger) => ledger.gap_report(stream, last_applied.map_or(0, |id| id.inner()), next_available.inner()),
            None => {
                " This batch was built without event bookkeeping attached, so the gap cannot be attributed.".to_owned()
            }
        }
    }

    pub fn get_last_event_ids(&self) -> LastEventIds<AvailableIndexes> {
        let prepared_evs = self
            .prepared_index_evs
            .as_ref()
            .expect("should be set before 0 iteration");

        // `None` where a stream contributed no events, rather than `default()`.
        // Event ids start at 0 and so does `default()`, so collapsing the two
        // reported "applied up to event 0" for a batch that applied nothing.
        let primary_id = prepared_evs.primary_evs.last().map(|ev| ev.id());
        let secondary_ids = prepared_evs.secondary_evs.last_evs().into_iter().collect();
        LastEventIds {
            primary_id,
            secondary_ids,
        }
    }

    pub async fn validate(
        &mut self,
        last_ids: &LastEventIds<AvailableIndexes>,
        attempts: usize,
    ) -> eyre::Result<Option<Vec<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>>>> {
        let mut valid = false;

        self.prepared_index_evs = Some(self.prepare_indexes_evs()?);
        let mut ops_to_remove = vec![];

        {
            let prepared_evs = self
                .prepared_index_evs
                .as_mut()
                .expect("should be set before 0 iteration");
            if prepared_evs.primary_evs.is_empty() && prepared_evs.secondary_evs.is_empty() {
                return Ok(Some(vec![]));
            }
        }

        while !valid {
            let prepared_evs = self
                .prepared_index_evs
                .as_mut()
                .expect("should be set before 0 iteration");
            let primary_invalid_events = validate_events(&mut prepared_evs.primary_evs);
            let secondary_invalid_events = prepared_evs.secondary_evs.validate();

            valid = if SecondaryEvents::is_unit() {
                primary_invalid_events.is_empty()
            } else {
                primary_invalid_events.is_empty() && secondary_invalid_events.is_empty()
            };

            if valid {
                break;
            }

            let events_to_remove = PreparedIndexEvents {
                primary_evs: primary_invalid_events,
                secondary_evs: secondary_invalid_events,
            };
            let ops = self.remove_operations_from_events(events_to_remove).await?;
            ops_to_remove.extend(ops);
        }

        {
            let prepared_evs = self
                .prepared_index_evs
                .as_ref()
                .expect("should be set before 0 iteration");
            // No exemption for the first batch any more. It used to carry
            // `&& last_ids.primary_id != IndexChangeEventId::default()`, so a
            // stream with nothing applied accepted *any* starting id, and that
            // is exactly where the ids can be wrong: event ids are allocated
            // during the index mutation while the operation is enqueued
            // afterwards, so two concurrent writers invert the two orders (see
            // `COLLECT_WHOLE_QUEUE_AFTER_ATTEMPTS`). Measured, a first batch of
            // ids 3..=28 was internally gapless, passed the exemption, and
            // advanced a node's maximum to key 28; events 0..=2 then arrived
            // naming a node whose maximum was still 1, resolved against
            // nothing, and failed with a missing page having already written
            // the file.
            //
            // The exemption was not gratuitous, which is why the fix is in
            // `LastEventIds` rather than here: ids start at 0 and `default()`
            // is 0, so the old representation could not tell "nothing applied"
            // from "applied event 0" and had to wave the first batch through.
            // `follows` asks the question that representation could not.
            if let Some(id) = prepared_evs.primary_evs.first().map(|ev| ev.id())
                && !LastEventIds::<AvailableIndexes>::follows(last_ids.primary_id, id)
            {
                // Change events are positional (InsertAt/RemoveAt carry node
                // indices), so a stream with a missing id must never be applied:
                // the disk index would apply later events against node state the
                // missing event was supposed to produce. Always defer. A gap is
                // transient (the op carrying the missing event has not been
                // batched yet) unless an event was discarded after its id was
                // assigned — only non-CDC index mutations do that — so a gap
                // that persists is a bug upstream of the analyzer; report it
                // loudly instead of force-applying and corrupting the file.
                if attempts > GIVE_UP_AFTER_ATTEMPTS {
                    let report = self.gap_report(&EventStream::Primary, last_ids.primary_id, id);
                    return Err(eyre::eyre!(
                        "persistence stalled on primary index event gap: last applied {:?}, next available {:?} after {attempts} attempts, with {} operations queued.{report}",
                        last_ids.primary_id,
                        id,
                        self.ops.len()
                    ));
                }
                self.ops.extend(ops_to_remove);
                return Ok(None);
            }
            let secondary_first = prepared_evs.secondary_evs.first_evs();
            for (index, id) in secondary_first {
                let Some(last) = last_ids.secondary_ids.get(&index) else {
                    continue;
                };
                // Same rule as the primary above, including the absence of a
                // first-batch exemption. A stream with no entry at all is
                // skipped by the `continue` above; an entry holding `None` is
                // a stream nothing has been applied to yet, which is the case
                // that needs checking rather than the case to wave through.
                if let Some(id) = id
                    && !LastEventIds::<AvailableIndexes>::follows(*last, id)
                {
                    // Same rule as the primary index above: never apply a gapped
                    // stream, defer until the missing event arrives, and report
                    // a persistent gap as the bug it is.
                    if attempts > GIVE_UP_AFTER_ATTEMPTS {
                        let report = self.gap_report(&EventStream::Secondary(format!("{index:?}")), *last, id);
                        return Err(eyre::eyre!(
                            "persistence stalled on secondary index {index:?} event gap: last applied {last:?}, next available {id:?} after {attempts} attempts, with {} operations queued.{report}",
                            self.ops.len()
                        ));
                    }
                    self.ops.extend(ops_to_remove);
                    return Ok(None);
                }
            }
        }

        {
            let prepared_evs = self
                .prepared_index_evs
                .as_ref()
                .expect("should be set before 0 iteration");
            if prepared_evs.primary_evs.is_empty() && prepared_evs.secondary_evs.is_empty() && self.ops.is_empty() {
                // Every operation was removed as invalid: there is nothing to
                // apply, so defer, handing the removed operations back through
                // `ops()` for requeueing. When `self.ops` still holds
                // survivors — data-only operations whose event vectors are
                // empty, generated for updates touching no indexed column —
                // the batch stays valid for them and falls through to
                // `Ok(Some(..))` below: overwriting `self.ops` here would
                // silently discard those accepted writes.
                self.ops = ops_to_remove;
                return Ok(None);
            }
        }

        Ok(Some(ops_to_remove))
    }

    fn prepare_indexes_evs(&self) -> eyre::Result<PreparedIndexEvents<PrimaryKey, SecondaryEvents>> {
        let mut primary_evs = vec![];
        let mut secondary_evs = SecondaryEvents::default();

        for op in &self.ops {
            if let Some(evs) = op.primary_key_events() {
                primary_evs.extend(evs.iter().cloned())
            }
            let secondary_new = op.secondary_key_events();
            secondary_evs.extend(secondary_new.clone());
        }

        // is used to make all events id's monotonically grow
        primary_evs.sort_by_key(|ev1| ev1.id());
        secondary_evs.sort();

        Ok(PreparedIndexEvents {
            primary_evs,
            secondary_evs,
        })
    }

    pub fn get_pk_gen_state(&self) -> eyre::Result<Option<PrimaryKeyGenState>> {
        let row = self
            .info_wt
            .select_by_op_type(OperationType::Insert)
            .order_on(BatchInnerRowFields::OperationId, Order::Desc)
            .limit(1)
            .execute()?;
        Ok(row.into_iter().next().map(|r| {
            let pos = r.pos;
            let op = self.ops.get(pos).expect("available as pos in wt");
            op.pk_gen_state().expect("is insert operation").clone()
        }))
    }

    pub fn get_indexes_evs(&self) -> eyre::Result<(BatchChangeEvent<PrimaryKey>, SecondaryEvents)> {
        let prepared_evs = self
            .prepared_index_evs
            .as_ref()
            .expect("prepared_index_evs should be set by validate() before calling get_indexes_evs");

        // Clone the prepared events (already sorted in validate())
        let mut primary_evs = prepared_evs.primary_evs.clone();
        let mut secondary_evs = prepared_evs.secondary_evs.clone();

        // Remove events from Acknowledge operations
        for op in &self.ops {
            if let Operation::Acknowledge(ack) = op {
                // Remove primary events from ack
                for ack_ev in &ack.primary_key_events {
                    if let Ok(pos) = primary_evs.binary_search_by(|ev| ev.id().cmp(&ack_ev.id())) {
                        primary_evs.remove(pos);
                    }
                }
                // Remove secondary events from ack using the trait's remove method
                secondary_evs.remove(&ack.secondary_keys_events);
            }
        }

        Ok((primary_evs, secondary_evs))
    }

    pub fn get_batch_data_op(&self) -> eyre::Result<BatchData> {
        Ok(latest_data_writes(&self.ops))
    }
}

#[cfg(test)]
mod tests {
    use hashbrown::HashMap;

    use data_bucket::Link;
    use data_bucket::page::PageId;
    use indexset::core::pair::Pair;
    use uuid::Uuid;

    use super::{BatchInnerRow, BatchInnerWorkTable, BatchOperation, latest_data_writes};
    use crate::persistence::OperationType;
    use crate::persistence::operation::{InsertOperation, Operation, OperationId};
    use crate::persistence::task::LastEventIds;
    use crate::prelude::{IndexChangeEvent, IndexChangeEventId, TableSecondaryIndexEventsOps};

    fn insert(id: u128, link: Link, bytes: Vec<u8>) -> Operation<(), u64, ()> {
        Operation::Insert(InsertOperation {
            id: OperationId::Single(Uuid::from_u128(id)),
            primary_key_events: vec![],
            secondary_keys_events: (),
            pk_gen_state: (),
            bytes,
            link,
        })
    }

    fn multi_insert(id: u128, link: Link, bytes: Vec<u8>) -> Operation<(), u64, ()> {
        Operation::Insert(InsertOperation {
            id: OperationId::Multi(Uuid::from_u128(id)),
            primary_key_events: vec![],
            secondary_keys_events: (),
            pk_gen_state: (),
            bytes,
            link,
        })
    }

    #[test]
    fn variable_length_link_reuse_keeps_only_the_newest_physical_write() {
        let old_link = Link {
            page_id: 1.into(),
            offset: 128,
            length: 4,
        };
        let new_link = Link {
            page_id: 1.into(),
            offset: 128,
            length: 6,
        };

        // Deliberately reverse vector order: operation ids, not incidental
        // collection order, define which bytes are newest.
        let batch = latest_data_writes(&[insert(2, new_link, vec![2; 6]), insert(1, old_link, vec![1; 4])]);
        let writes = batch.get(&PageId::from(1u32)).unwrap();

        assert_eq!(writes, &vec![(new_link, vec![2; 6])]);
    }

    #[test]
    fn overlapping_reused_ranges_remain_in_creation_order() {
        let older_link = Link {
            page_id: 1.into(),
            offset: 128,
            length: 8,
        };
        let newer_link = Link {
            page_id: 1.into(),
            offset: 132,
            length: 8,
        };

        for _ in 0..128 {
            let batch = latest_data_writes(&[insert(2, newer_link, vec![2; 8]), insert(1, older_link, vec![1; 8])]);
            let writes = batch.get(&PageId::from(1u32)).unwrap();

            assert_eq!(writes, &vec![(older_link, vec![1; 8]), (newer_link, vec![2; 8])]);
        }
    }

    #[test]
    fn equal_id_overlapping_writes_preserve_batch_order() {
        let older_link = Link {
            page_id: 1.into(),
            offset: 128,
            length: 8,
        };
        let newer_link = Link {
            page_id: 1.into(),
            offset: 132,
            length: 8,
        };

        let batch = latest_data_writes(&[
            multi_insert(1, older_link, vec![1; 8]),
            multi_insert(1, newer_link, vec![2; 8]),
        ]);

        assert_eq!(
            batch.get(&PageId::from(1u32)).unwrap(),
            &vec![(older_link, vec![1; 8]), (newer_link, vec![2; 8])]
        );
    }

    #[test]
    fn equal_id_physical_slot_reuse_keeps_later_batch_write() {
        let old_link = Link {
            page_id: 1.into(),
            offset: 128,
            length: 4,
        };
        let new_link = Link {
            page_id: 1.into(),
            offset: 128,
            length: 6,
        };

        let batch = latest_data_writes(&[
            multi_insert(1, old_link, vec![1; 4]),
            multi_insert(1, new_link, vec![2; 6]),
        ]);

        assert_eq!(batch.get(&PageId::from(1u32)).unwrap(), &vec![(new_link, vec![2; 6])]);
    }

    #[tokio::test]
    async fn missing_batch_metadata_returns_an_error_instead_of_panicking() {
        let batch: BatchOperation<(), u64, (), ()> = BatchOperation::new(vec![], Default::default());

        let error = batch.remove_info_at_pos(0).await.unwrap_err();

        assert!(error.to_string().contains("batch metadata position 0 is missing"));
    }

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

    fn primary_event(id: u64) -> IndexChangeEvent<Pair<u64, Link>> {
        IndexChangeEvent::InsertAt {
            event_id: id.into(),
            max_value: Pair {
                key: id,
                value: Link::default(),
            },
            value: Pair {
                key: id,
                value: Link::default(),
            },
            index: 0,
        }
    }

    fn event_insert(id: u128, link: Link, bytes: Vec<u8>, event_ids: Vec<u64>) -> Operation<(), u64, TestEvents> {
        Operation::Insert(InsertOperation {
            id: OperationId::Single(Uuid::from_u128(id)),
            primary_key_events: event_ids.into_iter().map(primary_event).collect(),
            secondary_keys_events: TestEvents,
            pk_gen_state: (),
            bytes,
            link,
        })
    }

    async fn batch_of(op: Operation<(), u64, TestEvents>) -> BatchOperation<(), u64, TestEvents, TestIndex> {
        let info_wt = BatchInnerWorkTable::default();
        info_wt
            .insert(BatchInnerRow {
                id: 0,
                operation_id: op.operation_id(),
                page_id: op.link().page_id,
                link: op.link(),
                op_type: OperationType::Insert,
                pos: 0,
            })
            .await
            .unwrap();
        BatchOperation::new(vec![op], info_wt)
    }

    fn link_at(offset: u32) -> Link {
        Link {
            page_id: 1.into(),
            offset,
            length: 4,
        }
    }

    /// A first batch that does not start at the head of the stream must defer.
    ///
    /// The gap check used to exempt the first batch outright, because event
    /// ids start at 0 and so does `IndexChangeEventId::default()`: the
    /// watermark could not tell "nothing applied yet" from "applied event 0",
    /// so asking whether the batch followed what came before would have
    /// deferred every stream's opening batch forever.
    ///
    /// The cost of that exemption is this: a first batch of ids 3.. is
    /// internally gapless, so event validation passes it and nothing else
    /// looks. It gets applied, advancing the on-disk node maxima, and events
    /// 0..=2 then arrive naming nodes whose maxima no longer exist. Making the
    /// watermark an `Option` lets the question be asked of the first batch too.
    #[tokio::test]
    async fn a_first_batch_that_skips_the_head_of_the_stream_defers() {
        let op = event_insert(1, link_at(0), vec![1; 4], vec![3, 4, 5]);
        let mut batch = batch_of(op).await;

        let outcome = batch.validate(&LastEventIds::default(), 0).await.unwrap();

        assert!(
            outcome.is_none(),
            "a first batch starting at event 3 must be deferred until events 0..=2 arrive"
        );
    }

    /// The other half: the exemption existed for a reason, and removing it
    /// must not deadlock a legitimate opening batch. Event 0 is a real id, not
    /// the absence of one.
    #[tokio::test]
    async fn a_first_batch_starting_at_event_zero_applies() {
        let op = event_insert(1, link_at(0), vec![1; 4], vec![0, 1, 2]);
        let mut batch = batch_of(op).await;

        let outcome = batch.validate(&LastEventIds::default(), 0).await.unwrap();

        assert!(
            outcome.is_some(),
            "a first batch starting at the first event must be applied, not deferred"
        );
    }

    /// Regression: removing the last event-carrying operation from a batch
    /// discarded the surviving data-only operations.
    ///
    /// When invalid-event removal emptied the prepared event set, `validate`
    /// unconditionally did `self.ops = ops_to_remove` and deferred. Any
    /// operation still in `self.ops` — a data-only write with an empty event
    /// vector, as generated for updates touching no indexed column — was
    /// silently dropped: never applied, never requeued, though the caller had
    /// been told the row was accepted.
    #[tokio::test]
    async fn removing_the_last_event_carrying_op_keeps_surviving_data_only_writes() {
        let invalid_link = Link {
            page_id: 1.into(),
            offset: 0,
            length: 4,
        };
        let survivor_link = Link {
            page_id: 1.into(),
            offset: 64,
            length: 4,
        };
        // Events 100 and 102 have an interior gap, so event validation removes
        // the op carrying them and the prepared event set ends up empty.
        let invalid_op = event_insert(1, invalid_link, vec![1; 4], vec![100, 102]);
        // No events at all: a pure data write that must survive.
        let survivor_op = event_insert(2, survivor_link, vec![7; 4], vec![]);

        let info_wt = BatchInnerWorkTable::default();
        for (pos, op) in [&invalid_op, &survivor_op].into_iter().enumerate() {
            info_wt
                .insert(BatchInnerRow {
                    id: pos as u64,
                    operation_id: op.operation_id(),
                    page_id: op.link().page_id,
                    link: op.link(),
                    op_type: OperationType::Insert,
                    pos,
                })
                .await
                .unwrap();
        }

        let mut batch: BatchOperation<(), u64, TestEvents, TestIndex> =
            BatchOperation::new(vec![invalid_op, survivor_op], info_wt);

        let removed = batch
            .validate(&LastEventIds::default(), 0)
            .await
            .unwrap()
            .expect("a batch with surviving data-only ops must be applied, not deferred");

        assert_eq!(removed.len(), 1, "exactly the invalid op is requeued");
        assert_eq!(removed[0].operation_id(), OperationId::Single(Uuid::from_u128(1)));

        let data = batch.get_batch_data_op().unwrap();
        assert_eq!(
            data.get(&PageId::from(1u32)).unwrap(),
            &vec![(survivor_link, vec![7; 4])],
            "the surviving data-only write must stay in the applied batch"
        );
    }
}
