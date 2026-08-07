use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

use data_bucket::page::PageId;
use data_bucket::{Link, SizeMeasurable};
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;
use worktable_codegen::{MemStat, worktable};

use crate::persistence::OperationType;
use crate::persistence::space::{BatchChangeEvent, BatchData};
use crate::persistence::task::{LastEventIds, QueueInnerRow};
use crate::prelude::*;
use crate::prelude::{From, Order, SelectQueryExecutor};

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
        operation_id_idx: operation_id,
        page_id_idx: page_id,
        link_idx: link,
        op_type_idx: op_type,
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
            .map(|(l, _)| *l)
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
            prepared_index_evs: None,
            phantom_data: PhantomData,
        }
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

        Ok(removed_ops)
    }

    pub fn get_last_event_ids(&self) -> LastEventIds<AvailableIndexes> {
        let prepared_evs = self
            .prepared_index_evs
            .as_ref()
            .expect("should be set before 0 iteration");

        let primary_id = prepared_evs.primary_evs.last().map(|ev| ev.id()).unwrap_or_default();
        let secondary_ids = prepared_evs.secondary_evs.last_evs();
        let secondary_ids = secondary_ids
            .into_iter()
            .map(|(i, v)| (i, v.unwrap_or_default()))
            .collect();
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
            if let Some(id) = prepared_evs.primary_evs.first().map(|ev| ev.id())
                && !id.is_next_for(last_ids.primary_id)
                && last_ids.primary_id != IndexChangeEventId::default()
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
                if attempts > 8 {
                    return Err(eyre::eyre!(
                        "persistence stalled on primary index event gap: last applied {:?}, next available {:?} (attempt {attempts}); an event id was likely consumed without its event being queued",
                        last_ids.primary_id,
                        id
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
                if let Some(id) = id
                    && !id.is_next_for(*last)
                    && *last != IndexChangeEventId::default()
                {
                    // Same rule as the primary index above: never apply a gapped
                    // stream, defer until the missing event arrives, and report
                    // a persistent gap as the bug it is.
                    if attempts > 8 {
                        return Err(eyre::eyre!(
                            "persistence stalled on secondary index {index:?} event gap: last applied {last:?}, next available {id:?} (attempt {attempts}); an event id was likely consumed without its event being queued"
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
            if prepared_evs.primary_evs.is_empty() && prepared_evs.secondary_evs.is_empty() {
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
    use data_bucket::Link;
    use uuid::Uuid;

    use super::{BatchOperation, latest_data_writes};
    use crate::persistence::operation::{InsertOperation, Operation, OperationId};

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
        let writes = batch.get(&1.into()).unwrap();

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
            let writes = batch.get(&1.into()).unwrap();

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
            batch.get(&1.into()).unwrap(),
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

        assert_eq!(batch.get(&1.into()).unwrap(), &vec![(new_link, vec![2; 6])]);
    }

    #[tokio::test]
    async fn missing_batch_metadata_returns_an_error_instead_of_panicking() {
        let batch: BatchOperation<(), u64, (), ()> = BatchOperation::new(vec![], Default::default());

        let error = batch.remove_info_at_pos(0).await.unwrap_err();

        assert!(error.to_string().contains("batch metadata position 0 is missing"));
    }
}
