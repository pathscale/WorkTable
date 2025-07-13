use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use data_bucket::page::PageId;
use data_bucket::{Link, SizeMeasurable};
use derive_more::Display;
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;
use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;
use worktable_codegen::{worktable, MemStat};

use crate::persistence::space::{BatchChangeEvent, BatchData};
use crate::persistence::task::{QueueInnerRow, QueueInnerWorkTable};
use crate::persistence::OperationType;
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
        operation_id_idx: operation_id,
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

#[derive(Debug)]
pub struct BatchOperation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents> {
    ops: Vec<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>>,
    info_wt: BatchInnerWorkTable,
    prepared_index_evs: Option<PreparedIndexEvents<PrimaryKey, SecondaryEvents>>,
}

#[derive(Debug)]
pub struct PreparedIndexEvents<PrimaryKey, SecondaryEvents> {
    primary_evs: Vec<ChangeEvent<Pair<PrimaryKey, Link>>>,
    secondary_evs: SecondaryEvents,
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>
    BatchOperation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>
where
    PrimaryKeyGenState: Debug + Clone,
    PrimaryKey: Debug + Clone,
    SecondaryEvents: Debug,
{
    pub fn new(
        ops: Vec<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>>,
        info_wt: BatchInnerWorkTable,
    ) -> Self {
        let ixd = ops.len() - 40;

        Self {
            ops,
            info_wt,
            prepared_index_evs: None,
        }
    }
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>
    BatchOperation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>
where
    PrimaryKeyGenState: Debug + Clone,
    PrimaryKey: Debug + Clone,
    SecondaryEvents: Debug + Default + Clone + TableSecondaryIndexEventsOps,
{
    fn remove_operations_from_events(
        &mut self,
        invalid_events: PreparedIndexEvents<PrimaryKey, SecondaryEvents>,
    ) {
        let mut removed_ops = HashSet::new();

        for ev in &invalid_events.primary_evs {
            let operation_pos_rev = self
                .ops
                .iter()
                .rev()
                .position(|op| {
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
                })
                .expect("Should exist as event was returned from validation");
            let op = self.ops.remove(self.ops.len() - (operation_pos_rev + 1));
            removed_ops.insert(op);
        }

        println!("Found some ops");
        for op in removed_ops {
            println!("{:?}", op);
        }
    }

    pub fn validate(
        &mut self,
    ) -> eyre::Result<Vec<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>>> {
        let mut prepared_evs = self.prepare_indexes_evs()?;
        let primary_invalid_events = validate_events(&mut prepared_evs.primary_evs);
        let secondary_invalid_events = prepared_evs.secondary_evs.validate();

        let valid = if SecondaryEvents::is_unit() {
            primary_invalid_events.is_empty()
        } else {
            primary_invalid_events.is_empty() && secondary_invalid_events.is_empty()
        };

        println!("{:?}", primary_invalid_events);
        println!("{:?}", secondary_invalid_events);

        self.prepared_index_evs = Some(prepared_evs);
        let events_to_remove = PreparedIndexEvents {
            primary_evs: primary_invalid_events,
            secondary_evs: secondary_invalid_events,
        };
        self.remove_operations_from_events(events_to_remove);

        panic!("just");

        Ok(vec![])
    }

    fn prepare_indexes_evs(
        &self,
    ) -> eyre::Result<PreparedIndexEvents<PrimaryKey, SecondaryEvents>> {
        let mut primary_evs = vec![];
        let mut secondary_evs = SecondaryEvents::default();

        let mut rows = self.info_wt.select_all().execute()?;
        rows.sort_by(|l, r| l.operation_id.cmp(&r.operation_id));
        for row in rows {
            let pos = row.pos;
            let op = self
                .ops
                .get(pos)
                .expect("pos should be correct as was set while batch build");
            if let Some(evs) = op.primary_key_events() {
                primary_evs.extend(evs.iter().cloned())
            }
            let secondary_new = op.secondary_key_events();
            secondary_evs.extend(secondary_new.clone());
        }

        // is used to make all events id's monotonically grow
        primary_evs.sort_by(|ev1, ev2| ev1.id().cmp(&ev2.id()));
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
        if let Some(evs) = &self.prepared_index_evs {
            Ok((evs.primary_evs.clone(), evs.secondary_evs.clone()))
        } else {
            tracing::warn!(
                "Index events are not validated and it can cause errors while applying batch"
            );
            let evs = self.prepare_indexes_evs()?;
            Ok((evs.primary_evs.clone(), evs.secondary_evs.clone()))
        }
    }

    pub fn get_batch_data_op(&self) -> eyre::Result<BatchData> {
        let mut data = HashMap::new();
        for link in self.info_wt.iter_links() {
            let last_op = self
                .info_wt
                .select_by_link(link)
                .order_on(BatchInnerRowFields::OperationId, Order::Desc)
                .limit(1)
                .execute()?;
            let op_row = last_op
                .into_iter()
                .next()
                .expect("if link is in info_wt at least one row exists");
            let pos = op_row.pos;
            let op = self
                .ops
                .get(pos)
                .expect("pos should be correct as was set while batch build");
            if let Some(data_bytes) = op.bytes() {
                let link = op.link();
                data.entry(link.page_id)
                    .and_modify(|v: &mut Vec<_>| v.push((link, data_bytes.to_vec())))
                    .or_insert(vec![(link, data_bytes.to_vec())]);
            }
        }

        Ok(data)
    }
}
