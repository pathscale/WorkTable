mod fragmentation_info;
mod lock;
mod page;

use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use data_bucket::Link;
use indexset::core::node::NodeLike;
use indexset::core::pair::Pair;
use rkyv::rancor::Strategy;
use rkyv::ser::Serializer;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Serialize};

use crate::in_memory::{DataPages, GhostWrapper, RowWrapper, StorableRow};
use crate::lock::{FullRowLock, LockMap};
use crate::prelude::TablePrimaryKey;
use crate::vacuum::fragmentation_info::PageFragmentationInfo;
use crate::vacuum::lock::VacuumLock;
use crate::{AvailableIndex, IndexMap, TableRow, TableSecondaryIndex, TableSecondaryIndexCdc};

#[derive(Debug)]
pub struct EmptyDataVacuum<
    Row,
    PrimaryKey,
    PkNodeType,
    SecondaryIndexes,
    SecondaryEvents,
    AvailableTypes,
    AvailableIndexes,
    const DATA_LENGTH: usize,
> where
    PrimaryKey: Clone + Ord + Send + 'static + std::hash::Hash,
    Row: StorableRow + Send + Clone + 'static,
    PkNodeType: NodeLike<Pair<PrimaryKey, Link>> + Send + 'static,
{
    data_pages: DataPages<Row, DATA_LENGTH>,
    vacuum_lock: Arc<VacuumLock>,

    primary_index: Arc<IndexMap<PrimaryKey, Link, PkNodeType>>,
    secondary_indexes: Arc<SecondaryIndexes>,

    phantom_data: PhantomData<(SecondaryEvents, AvailableTypes, AvailableIndexes)>,
}

impl<
    Row,
    PrimaryKey,
    PkNodeType,
    SecondaryIndexes,
    SecondaryEvents,
    AvailableTypes,
    AvailableIndexes,
    const DATA_LENGTH: usize,
>
    EmptyDataVacuum<
        Row,
        PrimaryKey,
        PkNodeType,
        SecondaryIndexes,
        SecondaryEvents,
        AvailableTypes,
        AvailableIndexes,
        DATA_LENGTH,
    >
where
    Row: TableRow<PrimaryKey> + StorableRow + Send + Clone + 'static,
    PrimaryKey: Debug + Clone + Ord + Send + TablePrimaryKey + std::hash::Hash,
    PkNodeType: NodeLike<Pair<PrimaryKey, Link>> + Send + 'static,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
    Row: Archive
        + Clone
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    <Row as StorableRow>::WrappedRow: Archive
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    <<Row as StorableRow>::WrappedRow as Archive>::Archived: GhostWrapper,
    SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes>
        + TableSecondaryIndexCdc<Row, AvailableTypes, SecondaryEvents, AvailableIndexes>,
    AvailableIndexes: Debug + AvailableIndex,
{
    async fn defragment_page(&self, info: PageFragmentationInfo<DATA_LENGTH>) {
        let lock = self.vacuum_lock.lock_page(info.page_id);

        let mut page_empty_links = self
            .data_pages
            .empty_links_registry()
            .page_links_map
            .get(&info.page_id)
            .map(|(_, l)| *l)
            .collect::<Vec<_>>();
        page_empty_links.sort_by(|l1, l2| l1.offset.cmp(&l2.offset));
    }

    // pub fn new(data_pages: DataPages<Row, DATA_LENGTH>) -> Self {
    //     Self {
    //         data_pages,
    //         vacuum_lock: Arc::new(Default::default()),
    //     }
    // }
    //
    // pub fn vacuum_pages() -> eyre::Result<()> {}
}
