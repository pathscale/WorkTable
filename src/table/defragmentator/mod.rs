use data_bucket::page::PageId;
use data_bucket::{Link, SizeMeasurable};
use indexset::core::node::NodeLike;
use indexset::core::pair::Pair;
use rkyv::api::high::HighDeserializer;
use rkyv::rancor::Strategy;
use rkyv::ser::Serializer;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::{Notify, RwLock};

use crate::in_memory::{
    DataExecutionError, EmptyLinksRegistry, GhostWrapper, RowWrapper, StorableRow,
};
use crate::lock::LockMap;
use crate::prelude::{Lock, TablePrimaryKey};
use crate::{TableRow, TableSecondaryIndex, WorkTable, WorkTableError};

pub struct DefragmentatorTask {
    task_handle: tokio::task::AbortHandle,

    /// Shared map for locking pages that are in defragmentation progress.
    lock: Arc<LockMap<Lock, PageId>>,

    /// Shared notifier for waking up [`Defragmentator`]
    notify: Arc<Notify>,
}

#[derive(Debug)]
pub struct Defragmentator<
    Row,
    PrimaryKey,
    EmptyLinks,
    AvailableTypes,
    AvailableIndexes,
    SecondaryIndexes,
    LockType,
    PkGen,
    NodeType,
    const DATA_LENGTH: usize,
> where
    PrimaryKey: Clone + Ord + Send + 'static + std::hash::Hash,
    Row: StorableRow + Send + Clone + 'static,
    NodeType: NodeLike<Pair<PrimaryKey, Link>> + Send + 'static,
{
    /// [`WorkTable`] to work with.
    table: Arc<
        WorkTable<
            Row,
            PrimaryKey,
            EmptyLinks,
            AvailableTypes,
            AvailableIndexes,
            SecondaryIndexes,
            LockType,
            PkGen,
            NodeType,
            DATA_LENGTH,
        >,
    >,

    /// Map for locking pages that are in defragmentation progress.
    lock_map: Arc<LockMap<Lock, PageId>>,

    /// Notifier for waking up [`Defragmentator`]
    notify: Arc<Notify>,
}

impl<
    Row,
    PrimaryKey,
    EmptyLinks,
    AvailableTypes,
    AvailableIndexes,
    SecondaryIndexes,
    LockType,
    PkGen,
    NodeType,
    const DATA_LENGTH: usize,
>
    Defragmentator<
        Row,
        PrimaryKey,
        EmptyLinks,
        AvailableTypes,
        AvailableIndexes,
        SecondaryIndexes,
        LockType,
        PkGen,
        NodeType,
        DATA_LENGTH,
    >
where
    PrimaryKey: Debug + Clone + Ord + Send + TablePrimaryKey + std::hash::Hash,
    EmptyLinks: Default + EmptyLinksRegistry,
    SecondaryIndexes: Default + TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>,
    PkGen: Default,
    AvailableIndexes: Debug,
    NodeType: NodeLike<Pair<PrimaryKey, Link>> + Send + 'static,
    Row: Archive + TableRow<PrimaryKey> + StorableRow + Send + Clone + 'static,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
    Row: Archive
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    <<Row as StorableRow>::WrappedRow as Archive>::Archived:
        Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    <Row as StorableRow>::WrappedRow: Archive
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        > + SizeMeasurable,
    <<Row as StorableRow>::WrappedRow as Archive>::Archived: GhostWrapper,
{
    fn defragment(&self) -> eyre::Result<()> {
        const SINGLE_LINK_RATIO_TRIGGER: f32 = 0.4;

        let empty_links = self.table.data.get_empty_links();
        let empty_links_len = empty_links.len();
        let mapped_links = Self::map_empty_links_by_pages(empty_links);
        let single_link_pages = mapped_links.values().filter(|v| v.len() == 1).count();

        let links_left =
            if single_link_pages as f32 / empty_links_len as f32 > SINGLE_LINK_RATIO_TRIGGER {
                self.defragment_if_triggered(mapped_links)
            } else {
                self.defragment_if_not_triggered(mapped_links)
            }?;

        Ok(())
    }

    fn defragment_if_triggered(
        &self,
        mapped_links: HashMap<PageId, HashSet<Link>>,
    ) -> eyre::Result<Vec<Link>> {
        let mut links_left = vec![];
        Ok(links_left)
    }

    fn defragment_if_not_triggered(
        &self,
        mapped_links: HashMap<PageId, HashSet<Link>>,
    ) -> eyre::Result<Vec<Link>> {
        let mut links_left = vec![];

        for (page_id, mut links) in mapped_links {
            // sorting `Link`s in ascending order.
            let first_link = links
                .iter()
                .min_by(|l1, l2| l1.offset.cmp(&l2.offset))
                .copied()
                .expect("links should not be empty");
            let mut temporary_removed_rows = vec![];

            let lock_id = self.lock_map.next_id();
            let lock = Arc::new(RwLock::new(Lock::new(lock_id)));
            self.lock_map.insert(page_id, lock).expect(
                "nothing should be returned as this is single defragmentation thread for table",
            );

            links.remove(&first_link);
            let mut link = first_link;
            let page = self.table.data.get_page(page_id)?;

            // removing all rows on page after first link
            loop {
                let res = page.get_next_lying_row(link);
                let (wrapped_row, res_link) = match res {
                    Ok(res) => res,
                    Err(e) => match e {
                        DataExecutionError::InvalidLink => {
                            break;
                        }
                        _ => return Err(e.into()),
                    },
                };

                if !links.remove(&res_link) {
                    let row = wrapped_row.get_inner().clone();
                    temporary_removed_rows.push(row.clone());
                    self.table
                        .pk_map
                        .remove(&row.get_primary_key())
                        .expect("should exist as current page is blocked");
                    self.table
                        .indexes
                        .delete_row(row, res_link)
                        .expect("should be ok as current page is blocked")
                }

                link = res_link;
            }

            page.set_free_offset(first_link.offset);

            for row in temporary_removed_rows {
                let wrapped_row = Row::WrappedRow::from_inner(row.clone());
                let new_link = page.save_row(&wrapped_row)?;
                self.table
                    .pk_map
                    .insert(row.get_primary_key(), new_link)
                    .expect("should not exist as current page was blocked");
                self.table
                    .indexes
                    .save_row(row, new_link)
                    .expect("should be ok as current page was blocked");
                unsafe { self.table.data.with_mut_ref(new_link, |r| r.unghost())? }
            }

            let current_offset = page.free_offset.load(Ordering::Relaxed);
            page.set_free_offset(DATA_LENGTH as u32);

            let link_left = Link {
                page_id,
                offset: current_offset,
                length: DATA_LENGTH as u32 - current_offset,
            };

            links_left.push(link_left)
        }

        Ok(links_left)
    }

    fn map_empty_links_by_pages(empty_links: Vec<Link>) -> HashMap<PageId, HashSet<Link>> {
        let mut map = HashMap::new();
        for link in empty_links {
            map.entry(link.page_id)
                .and_modify(|s: &mut HashSet<_>| {
                    s.insert(link);
                })
                .or_insert({
                    let mut s = HashSet::new();
                    s.insert(link);
                    s
                });
        }

        map
    }
}

#[cfg(test)]
mod tests {
    use crate::prelude::*;
    use worktable_codegen::worktable;

    worktable! (
        name: Test,
        columns: {
            id: u64 primary_key autoincrement,
            test: i64,
            another: u64,
            exchange: String
        },
        indexes: {
            test_idx: test unique,
            exchnage_idx: exchange,
            another_idx: another,
        }
        queries: {
            delete: {
                ByAnother() by another,
                ByExchange() by exchange,
                ByTest() by test,
            }
        }
    );
}
