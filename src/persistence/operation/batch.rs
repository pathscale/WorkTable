use std::collections::{HashMap, HashSet};
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
        operation_id_idx: operation_id unique,
        page_id_idx: page_id,
        link_idx: link,
        op_type_idx: op_type,
    },
    queries: {
        update: {
            PosByOpId(pos) by operation_id,
        },
        delete: {
            ByOpId() by operation_id,
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

/// Coalesces durable row writes by physical storage slot.
///
/// `Link::length` can change when an unsized row is reinserted into a reused
/// `(page_id, offset)`. Treating the two lengths as different keys leaves
/// overlapping writes in the same batch, whose eventual application order is
/// derived from a hash map. The newest operation must be the only write for a
/// physical slot. WorkTable-generated operation IDs use `Uuid::now_v7`, whose
/// shared process context guarantees creation-order sorting even within one
/// millisecond; callers constructing `Operation` values manually must preserve
/// that ordering contract.
fn latest_data_writes<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>(
    ops: &[Operation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>],
) -> BatchData {
    let mut latest: HashMap<(PageId, u32), (OperationId, Link, Vec<u8>)> = HashMap::new();
    for op in ops {
        let Some(bytes) = op.bytes() else {
            continue;
        };
        let link = op.link();
        let operation_id = op.operation_id();
        let key = (link.page_id, link.offset);
        match latest.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if operation_id > entry.get().0 {
                    entry.insert((operation_id, link, bytes.to_vec()));
                }
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert((operation_id, link, bytes.to_vec()));
            }
        }
    }

    let mut data = HashMap::new();
    for (_, (_, link, bytes)) in latest {
        data.entry(link.page_id).or_insert_with(Vec::new).push((link, bytes));
    }
    data
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
    ) -> HashSet<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>> {
        let mut removed_ops = HashSet::new();

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
                let op = self.ops.remove(self.ops.len() - (operation_pos_rev + 1));
                removed_ops.insert(op);
            }
        }
        for (index, id) in invalid_events.secondary_evs.iter_event_ids() {
            if let Some(operation_pos_rev) = self.ops.iter().rev().position(|op| {
                let evs = op.secondary_key_events();
                evs.contains_event(index, id)
            }) {
                let op = self.ops.remove(self.ops.len() - (operation_pos_rev + 1));
                removed_ops.insert(op);
            };
            // else it was already removed with primary
        }
        for op in &removed_ops {
            let pk = self
                .info_wt
                .select_by_operation_id(op.operation_id())
                .expect("exists as all should be inserted on prepare step")
                .id;
            self.info_wt.delete_without_lock::<_>(pk).await.unwrap();
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

        removed_ops
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
            let ops = self.remove_operations_from_events(events_to_remove).await;
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

        for (pos, op) in self.ops.iter().enumerate() {
            let op_id = op.operation_id();
            let q = PosByOpIdQuery { pos };
            self.info_wt.update_pos_by_op_id(q, op_id).await?
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

    use super::latest_data_writes;
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
}
