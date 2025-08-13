use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use data_bucket::Link;
use data_bucket::page::PageId;
use indexset::core::node::NodeLike;
use indexset::core::pair::Pair;
use tokio::sync::{Notify, RwLock};

use crate::WorkTable;
use crate::in_memory::{EmptyLinksRegistry, RowWrapper, StorableRow};
use crate::lock::LockMap;
use crate::prelude::{Lock, TablePrimaryKey};

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
    SecondaryIndexes: Default,
    PkGen: Default,
    NodeType: NodeLike<Pair<PrimaryKey, Link>> + Send + 'static,
    Row: StorableRow + Send + Clone + 'static,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
{
    fn defragment(&self) -> eyre::Result<()> {
        const SINGLE_LINK_RATIO_TRIGGER: f32 = 0.4;

        let empty_links = self.table.data.get_empty_links();
        let empty_links_len = empty_links.len();
        let mapped_links = Self::map_empty_links_by_pages(empty_links);
        let single_link_pages = mapped_links.values().filter(|v| v.len() == 1).count();

        if single_link_pages as f32 / empty_links_len as f32 > SINGLE_LINK_RATIO_TRIGGER {
            self.defragment_if_triggered(mapped_links)
        } else {
            self.defragment_if_not_triggered(mapped_links)
        }
    }

    fn defragment_if_triggered(
        &self,
        mapped_links: HashMap<PageId, Vec<Link>>,
    ) -> eyre::Result<()> {
        Ok(())
    }

    fn defragment_if_not_triggered(
        &self,
        mapped_links: HashMap<PageId, Vec<Link>>,
    ) -> eyre::Result<()> {
        for (page_id, mut links) in mapped_links {
            // sorting `Link`s in ascending order.
            links.sort_by(|l1, l2| l1.offset.cmp(&l2.offset));

            let lock_id = self.lock_map.next_id();
            let lock = Arc::new(RwLock::new(Lock::new(lock_id)));
            self.lock_map.insert(page_id, lock).expect(
                "nothing should be returned as this is single defragmentation thread for table",
            );
        }

        Ok(())
    }

    fn map_empty_links_by_pages(empty_links: Vec<Link>) -> HashMap<PageId, Vec<Link>> {
        let mut map = HashMap::new();
        for link in empty_links {
            map.entry(link.page_id)
                .and_modify(|v: &mut Vec<_>| v.push(link))
                .or_insert(vec![link]);
        }
        map
    }
}
