//! Background logical-to-structural CDC translation for WorkTablesIndex.
//!
//! The disk shadow is reconstructed from the existing DataBucket index pages.
//! Foreground tables can therefore emit compact logical Set/Remove events while
//! this persistence-worker-owned index derives the structural events required
//! by the unchanged WTI disk format.

use std::fmt::Debug;
use std::hash::Hash;
use std::path::{Path, PathBuf};

use data_bucket::{Link, SizeMeasurable, SpaceId, VariableSizeMeasurable};
use indexset::cdc::change::ChangeEvent;
use indexset::concurrent::map::BTreeMap;
use indexset::concurrent::multimap::BTreeMultiMap;
use indexset::core::multipair::MultiPair;
use indexset::core::node::NodeLike;
use indexset::core::pair::Pair;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::ser::Serializer;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize, rancor};
use tokio::fs::File;

use crate::UnsizedNode;
use crate::convert_multi_change_events;
use crate::persistence::space::BatchChangeEvent;
use crate::persistence::{PersistenceIndexCorruption, SpaceIndex, SpaceIndexOps, SpaceIndexUnsized};
use crate::prelude::WT_INDEX_EXTENSION;

fn translate_logical_event<T, Node>(
    index_path: &Path,
    shadow: &BTreeMap<T, Link, Node>,
    event: ChangeEvent<Pair<T, Link>>,
) -> Result<Vec<ChangeEvent<Pair<T, Link>>>, PersistenceIndexCorruption>
where
    T: Debug + Eq + Hash + Clone + Send + Ord + 'static,
    Node: NodeLike<Pair<T, Link>> + Send + 'static,
{
    match event {
        ChangeEvent::InsertAt {
            max_value,
            value,
            index: 0,
            ..
        } if max_value.key == value.key && max_value.value == value.value => {
            let (_, events) = shadow.insert_cdc(value.key, value.value);
            Ok(events)
        }
        ChangeEvent::RemoveAt {
            max_value,
            value,
            index: 0,
            ..
        } if max_value.key == value.key && max_value.value == value.value => {
            let found = shadow.get(&value.key).map(|entry| entry.get().value);
            if found != Some(value.value) {
                return Err(PersistenceIndexCorruption::new(
                    index_path,
                    format!(
                        "logical WTI shadow diverged while removing key {:?}: expected {:?}, found {:?}",
                        value.key, value.value, found,
                    ),
                ));
            }
            let (_, events) = shadow.remove_cdc(&value.key);
            Ok(events)
        }
        _ => Err(PersistenceIndexCorruption::new(
            index_path,
            "logical WTI persistence received a structural or malformed event",
        )),
    }
}

fn translate_logical_batch<T, Node>(
    index_path: &Path,
    shadow: &BTreeMap<T, Link, Node>,
    mut events: BatchChangeEvent<T>,
) -> Result<BatchChangeEvent<T>, PersistenceIndexCorruption>
where
    T: Debug + Eq + Hash + Clone + Send + Ord + 'static,
    Node: NodeLike<Pair<T, Link>> + Send + 'static,
{
    // BatchOperation already sorts every per-index stream by event id before
    // dispatch. Sort again at this logical/structural boundary so direct
    // SpaceIndexOps callers and future batching changes cannot reorder a
    // same-key Set/Remove pair after the foreground stripe guard is released.
    events.sort_by_key(ChangeEvent::id);
    let mut structural = Vec::new();
    for event in events {
        structural.extend(translate_logical_event(index_path, shadow, event)?);
    }
    Ok(structural)
}

fn translate_logical_multi_event<T, Node>(
    index_path: &Path,
    shadow: &BTreeMultiMap<T, Link, Node>,
    event: ChangeEvent<Pair<T, Link>>,
) -> Result<Vec<ChangeEvent<Pair<T, Link>>>, PersistenceIndexCorruption>
where
    T: Debug + Eq + Hash + Clone + Send + Ord + 'static,
    Node: NodeLike<MultiPair<T, Link>> + Send + 'static,
{
    match event {
        ChangeEvent::InsertAt {
            max_value,
            value,
            index: 0,
            ..
        } if max_value.key == value.key && max_value.value == value.value => {
            let (_, events) = shadow.insert_cdc(value.key, value.value);
            Ok(convert_multi_change_events(events))
        }
        ChangeEvent::RemoveAt {
            max_value,
            value,
            index: 0,
            ..
        } if max_value.key == value.key && max_value.value == value.value => {
            let (found, events) = shadow.remove_cdc(&value.key, &value.value);
            if found.is_none() {
                return Err(PersistenceIndexCorruption::new(
                    index_path,
                    format!("logical WTI multimap shadow diverged while removing pair {:?}", value,),
                ));
            }
            Ok(convert_multi_change_events(events))
        }
        _ => Err(PersistenceIndexCorruption::new(
            index_path,
            "logical WTI multimap persistence received a structural or malformed event",
        )),
    }
}

fn translate_logical_multi_batch<T, Node>(
    index_path: &Path,
    shadow: &BTreeMultiMap<T, Link, Node>,
    mut events: BatchChangeEvent<T>,
) -> Result<BatchChangeEvent<T>, PersistenceIndexCorruption>
where
    T: Debug + Eq + Hash + Clone + Send + Ord + 'static,
    Node: NodeLike<MultiPair<T, Link>> + Send + 'static,
{
    events.sort_by_key(ChangeEvent::id);
    let mut structural = Vec::new();
    for event in events {
        structural.extend(translate_logical_multi_event(index_path, shadow, event)?);
    }
    Ok(structural)
}

/// Sized-key WTI persistence with foreground logical CDC and a background
/// structural shadow. The wrapped `SpaceIndex` retains the existing file
/// layout byte-for-byte.
pub struct SpaceLogicalIndex<T, const INNER_PAGE_SIZE: u32>
where
    T: Send + Ord + Eq + Clone + 'static,
{
    index_path: PathBuf,
    shadow: BTreeMap<T, Link>,
    disk: SpaceIndex<T, INNER_PAGE_SIZE>,
}

impl<T, const INNER_PAGE_SIZE: u32> Debug for SpaceLogicalIndex<T, INNER_PAGE_SIZE>
where
    T: Send + Ord + Eq + Clone + 'static,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("SpaceLogicalIndex").finish_non_exhaustive()
    }
}

impl<T, const INNER_PAGE_SIZE: u32> SpaceLogicalIndex<T, INNER_PAGE_SIZE>
where
    T: Archive
        + Ord
        + Eq
        + Hash
        + Clone
        + Default
        + Debug
        + SizeMeasurable
        + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>>
        + Send
        + Sync
        + 'static,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>>
        + Ord
        + Eq
        + Debug
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rancor::Error>>,
{
    async fn new(path: String, version: u32) -> eyre::Result<Self> {
        let index_path = PathBuf::from(&path);
        let mut disk = SpaceIndex::new(path, SpaceId::from(0), version).await?;
        let shadow = disk.parse_indexset().await?;
        Ok(Self {
            index_path,
            shadow,
            disk,
        })
    }
}

impl<T, const INNER_PAGE_SIZE: u32> SpaceIndexOps<T> for SpaceLogicalIndex<T, INNER_PAGE_SIZE>
where
    T: Archive
        + Ord
        + Eq
        + Hash
        + Clone
        + Default
        + Debug
        + SizeMeasurable
        + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>>
        + Send
        + Sync
        + 'static,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>>
        + Ord
        + Eq
        + Debug
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rancor::Error>>,
{
    async fn primary_from_table_files_path<S: AsRef<str> + Send>(path: S, version: u32) -> eyre::Result<Self> {
        Self::new(format!("{}/primary{}", path.as_ref(), WT_INDEX_EXTENSION), version).await
    }

    async fn secondary_from_table_files_path<S1: AsRef<str> + Send, S2: AsRef<str> + Send>(
        path: S1,
        name: S2,
        version: u32,
    ) -> eyre::Result<Self> {
        Self::new(
            format!("{}/{}{}", path.as_ref(), name.as_ref(), WT_INDEX_EXTENSION),
            version,
        )
        .await
    }

    async fn bootstrap(file: &mut File, table_name: String, version: u32) -> eyre::Result<()> {
        SpaceIndex::<T, INNER_PAGE_SIZE>::bootstrap(file, table_name, version).await
    }

    async fn process_change_event(&mut self, event: ChangeEvent<Pair<T, Link>>) -> eyre::Result<()> {
        let events = translate_logical_event(&self.index_path, &self.shadow, event)?;
        self.disk.process_change_event_batch(events).await
    }

    async fn process_change_event_batch(&mut self, events: BatchChangeEvent<T>) -> eyre::Result<()> {
        let events = translate_logical_batch(&self.index_path, &self.shadow, events)?;
        self.disk.process_change_event_batch(events).await
    }
}

/// Variable-sized-key counterpart to [`SpaceLogicalIndex`].
pub struct SpaceLogicalIndexUnsized<T, const INNER_PAGE_SIZE: u32>
where
    T: Send + Ord + Eq + Clone + Default + Debug + SizeMeasurable + VariableSizeMeasurable + 'static,
{
    index_path: PathBuf,
    shadow: BTreeMap<T, Link, UnsizedNode<Pair<T, Link>>>,
    disk: SpaceIndexUnsized<T, INNER_PAGE_SIZE>,
}

impl<T, const INNER_PAGE_SIZE: u32> Debug for SpaceLogicalIndexUnsized<T, INNER_PAGE_SIZE>
where
    T: Send + Ord + Eq + Clone + Default + Debug + SizeMeasurable + VariableSizeMeasurable + 'static,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SpaceLogicalIndexUnsized")
            .finish_non_exhaustive()
    }
}

impl<T, const INNER_PAGE_SIZE: u32> SpaceLogicalIndexUnsized<T, INNER_PAGE_SIZE>
where
    T: Archive
        + Ord
        + Eq
        + Hash
        + Clone
        + Default
        + Debug
        + SizeMeasurable
        + VariableSizeMeasurable
        + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>>
        + Send
        + Sync
        + 'static,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>>
        + Ord
        + Eq
        + Debug
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rancor::Error>>,
{
    async fn new(path: String, version: u32) -> eyre::Result<Self> {
        let index_path = PathBuf::from(&path);
        let mut disk = SpaceIndexUnsized::new(path, SpaceId::from(0), version).await?;
        let shadow = disk.parse_indexset().await?;
        Ok(Self {
            index_path,
            shadow,
            disk,
        })
    }
}

impl<T, const INNER_PAGE_SIZE: u32> SpaceIndexOps<T> for SpaceLogicalIndexUnsized<T, INNER_PAGE_SIZE>
where
    T: Archive
        + Ord
        + Eq
        + Hash
        + Clone
        + Default
        + Debug
        + SizeMeasurable
        + VariableSizeMeasurable
        + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>>
        + Send
        + Sync
        + 'static,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>>
        + Ord
        + Eq
        + Debug
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rancor::Error>>,
{
    async fn primary_from_table_files_path<S: AsRef<str> + Send>(path: S, version: u32) -> eyre::Result<Self> {
        Self::new(format!("{}/primary{}", path.as_ref(), WT_INDEX_EXTENSION), version).await
    }

    async fn secondary_from_table_files_path<S1: AsRef<str> + Send, S2: AsRef<str> + Send>(
        path: S1,
        name: S2,
        version: u32,
    ) -> eyre::Result<Self> {
        Self::new(
            format!("{}/{}{}", path.as_ref(), name.as_ref(), WT_INDEX_EXTENSION),
            version,
        )
        .await
    }

    async fn bootstrap(file: &mut File, table_name: String, version: u32) -> eyre::Result<()> {
        SpaceIndexUnsized::<T, INNER_PAGE_SIZE>::bootstrap(file, table_name, version).await
    }

    async fn process_change_event(&mut self, event: ChangeEvent<Pair<T, Link>>) -> eyre::Result<()> {
        let events = translate_logical_event(&self.index_path, &self.shadow, event)?;
        self.disk.process_change_event_batch(events).await
    }

    async fn process_change_event_batch(&mut self, events: BatchChangeEvent<T>) -> eyre::Result<()> {
        let events = translate_logical_batch(&self.index_path, &self.shadow, events)?;
        self.disk.process_change_event_batch(events).await
    }
}

/// Sized-key WTI persistence for a non-unique runtime backend that emits
/// logical `(key, link)` mutations. The WTI file layout and its node topology
/// remain compatible with earlier WorkTable releases.
pub struct SpaceLogicalMultiIndex<T, const INNER_PAGE_SIZE: u32>
where
    T: Debug + Send + Ord + Eq + Clone + 'static,
{
    index_path: PathBuf,
    shadow: BTreeMultiMap<T, Link>,
    disk: SpaceIndex<T, INNER_PAGE_SIZE>,
}

impl<T, const INNER_PAGE_SIZE: u32> Debug for SpaceLogicalMultiIndex<T, INNER_PAGE_SIZE>
where
    T: Debug + Send + Ord + Eq + Clone + 'static,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("SpaceLogicalMultiIndex").finish_non_exhaustive()
    }
}

impl<T, const INNER_PAGE_SIZE: u32> SpaceLogicalMultiIndex<T, INNER_PAGE_SIZE>
where
    T: Archive
        + Ord
        + Eq
        + Hash
        + Clone
        + Default
        + Debug
        + SizeMeasurable
        + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>>
        + Send
        + Sync
        + 'static,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>>
        + Ord
        + Eq
        + Debug
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rancor::Error>>,
{
    async fn new(path: String, version: u32) -> eyre::Result<Self> {
        let index_path = PathBuf::from(&path);
        let mut disk = SpaceIndex::new(path, SpaceId::from(0), version).await?;
        let shadow = disk.parse_index_multimap(index_path.to_string_lossy().as_ref()).await?;
        Ok(Self {
            index_path,
            shadow,
            disk,
        })
    }
}

impl<T, const INNER_PAGE_SIZE: u32> SpaceIndexOps<T> for SpaceLogicalMultiIndex<T, INNER_PAGE_SIZE>
where
    T: Archive
        + Ord
        + Eq
        + Hash
        + Clone
        + Default
        + Debug
        + SizeMeasurable
        + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>>
        + Send
        + Sync
        + 'static,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>>
        + Ord
        + Eq
        + Debug
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rancor::Error>>,
{
    async fn primary_from_table_files_path<S: AsRef<str> + Send>(path: S, version: u32) -> eyre::Result<Self> {
        Self::new(format!("{}/primary{}", path.as_ref(), WT_INDEX_EXTENSION), version).await
    }

    async fn secondary_from_table_files_path<S1: AsRef<str> + Send, S2: AsRef<str> + Send>(
        path: S1,
        name: S2,
        version: u32,
    ) -> eyre::Result<Self> {
        Self::new(
            format!("{}/{}{}", path.as_ref(), name.as_ref(), WT_INDEX_EXTENSION),
            version,
        )
        .await
    }

    async fn bootstrap(file: &mut File, table_name: String, version: u32) -> eyre::Result<()> {
        SpaceIndex::<T, INNER_PAGE_SIZE>::bootstrap(file, table_name, version).await
    }

    async fn process_change_event(&mut self, event: ChangeEvent<Pair<T, Link>>) -> eyre::Result<()> {
        let events = translate_logical_multi_event(&self.index_path, &self.shadow, event)?;
        self.disk.process_change_event_batch(events).await
    }

    async fn process_change_event_batch(&mut self, events: BatchChangeEvent<T>) -> eyre::Result<()> {
        let events = translate_logical_multi_batch(&self.index_path, &self.shadow, events)?;
        self.disk.process_change_event_batch(events).await
    }
}

/// Variable-sized-key counterpart to [`SpaceLogicalMultiIndex`].
pub struct SpaceLogicalMultiIndexUnsized<T, const INNER_PAGE_SIZE: u32>
where
    T: Send + Ord + Eq + Clone + Default + Debug + SizeMeasurable + VariableSizeMeasurable + 'static,
{
    index_path: PathBuf,
    shadow: BTreeMultiMap<T, Link, UnsizedNode<MultiPair<T, Link>>>,
    disk: SpaceIndexUnsized<T, INNER_PAGE_SIZE>,
}

impl<T, const INNER_PAGE_SIZE: u32> Debug for SpaceLogicalMultiIndexUnsized<T, INNER_PAGE_SIZE>
where
    T: Send + Ord + Eq + Clone + Default + Debug + SizeMeasurable + VariableSizeMeasurable + 'static,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SpaceLogicalMultiIndexUnsized")
            .finish_non_exhaustive()
    }
}

impl<T, const INNER_PAGE_SIZE: u32> SpaceLogicalMultiIndexUnsized<T, INNER_PAGE_SIZE>
where
    T: Archive
        + Ord
        + Eq
        + Hash
        + Clone
        + Default
        + Debug
        + SizeMeasurable
        + VariableSizeMeasurable
        + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>>
        + Send
        + Sync
        + 'static,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>>
        + Ord
        + Eq
        + Debug
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rancor::Error>>,
{
    async fn new(path: String, version: u32) -> eyre::Result<Self> {
        let index_path = PathBuf::from(&path);
        let mut disk = SpaceIndexUnsized::new(path, SpaceId::from(0), version).await?;
        let shadow = disk.parse_index_multimap(index_path.to_string_lossy().as_ref()).await?;
        Ok(Self {
            index_path,
            shadow,
            disk,
        })
    }
}

impl<T, const INNER_PAGE_SIZE: u32> SpaceIndexOps<T> for SpaceLogicalMultiIndexUnsized<T, INNER_PAGE_SIZE>
where
    T: Archive
        + Ord
        + Eq
        + Hash
        + Clone
        + Default
        + Debug
        + SizeMeasurable
        + VariableSizeMeasurable
        + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>>
        + Send
        + Sync
        + 'static,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>>
        + Ord
        + Eq
        + Debug
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rancor::Error>>,
{
    async fn primary_from_table_files_path<S: AsRef<str> + Send>(path: S, version: u32) -> eyre::Result<Self> {
        Self::new(format!("{}/primary{}", path.as_ref(), WT_INDEX_EXTENSION), version).await
    }

    async fn secondary_from_table_files_path<S1: AsRef<str> + Send, S2: AsRef<str> + Send>(
        path: S1,
        name: S2,
        version: u32,
    ) -> eyre::Result<Self> {
        Self::new(
            format!("{}/{}{}", path.as_ref(), name.as_ref(), WT_INDEX_EXTENSION),
            version,
        )
        .await
    }

    async fn bootstrap(file: &mut File, table_name: String, version: u32) -> eyre::Result<()> {
        SpaceIndexUnsized::<T, INNER_PAGE_SIZE>::bootstrap(file, table_name, version).await
    }

    async fn process_change_event(&mut self, event: ChangeEvent<Pair<T, Link>>) -> eyre::Result<()> {
        let events = translate_logical_multi_event(&self.index_path, &self.shadow, event)?;
        self.disk.process_change_event_batch(events).await
    }

    async fn process_change_event_batch(&mut self, events: BatchChangeEvent<T>) -> eyre::Result<()> {
        let events = translate_logical_multi_batch(&self.index_path, &self.shadow, events)?;
        self.disk.process_change_event_batch(events).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap as StdBTreeMap;

    use data_bucket::page::PageId;

    use super::*;

    fn link(offset: u32) -> Link {
        Link {
            page_id: PageId::from(1),
            offset,
            length: 8,
        }
    }

    #[test]
    fn logical_events_rebuild_structural_events_on_the_shadow() {
        let shadow = BTreeMap::<u64, Link>::default();
        let pair = Pair { key: 7, value: link(7) };
        let events = translate_logical_event(
            Path::new("test.wt.idx"),
            &shadow,
            ChangeEvent::InsertAt {
                event_id: 0.into(),
                max_value: pair.clone(),
                value: pair,
                index: 0,
            },
        )
        .unwrap();
        assert!(!events.is_empty());
        assert_eq!(shadow.get(&7).map(|entry| entry.get().value), Some(link(7)));
    }

    #[test]
    fn logical_event_requires_an_exact_key_and_link_sentinel() {
        let shadow = BTreeMap::<u64, Link>::default();
        let result = translate_logical_event(
            Path::new("test.wt.idx"),
            &shadow,
            ChangeEvent::InsertAt {
                event_id: 0.into(),
                max_value: Pair { key: 7, value: link(8) },
                value: Pair { key: 7, value: link(7) },
                index: 0,
            },
        );

        assert!(result.is_err());
        assert!(shadow.get(&7).is_none());
    }

    #[test]
    fn logical_batch_restores_event_id_order_after_reversed_delivery() {
        let shadow = BTreeMap::<u64, Link>::default();
        let pair = Pair { key: 7, value: link(7) };
        let insert = ChangeEvent::InsertAt {
            event_id: 0.into(),
            max_value: pair.clone(),
            value: pair.clone(),
            index: 0,
        };
        let remove = ChangeEvent::RemoveAt {
            event_id: 1.into(),
            max_value: pair.clone(),
            value: pair,
            index: 0,
        };

        let structural = translate_logical_batch(Path::new("test.wt.idx"), &shadow, vec![remove, insert]).unwrap();

        assert!(!structural.is_empty());
        assert!(shadow.get(&7).is_none());
    }

    #[test]
    fn divergence_is_typed_and_does_not_mutate_the_shadow() {
        let shadow = BTreeMap::<u64, Link>::default();
        shadow.insert(7, link(7));
        let pair = Pair { key: 7, value: link(8) };

        let error = translate_logical_event(
            Path::new("test.wt.idx"),
            &shadow,
            ChangeEvent::RemoveAt {
                event_id: 1.into(),
                max_value: pair.clone(),
                value: pair,
                index: 0,
            },
        )
        .unwrap_err();

        assert_eq!(error.path(), Path::new("test.wt.idx"));
        assert!(error.reason().contains("shadow diverged"));
        assert_eq!(shadow.get(&7).map(|entry| entry.get().value), Some(link(7)));
    }

    #[test]
    fn logical_set_replacement_derives_the_new_structural_link() {
        let shadow = BTreeMap::<u64, Link>::default();
        shadow.insert(7, link(7));
        let replacement = Pair { key: 7, value: link(8) };

        let structural = translate_logical_event(
            Path::new("test.wt.idx"),
            &shadow,
            ChangeEvent::InsertAt {
                event_id: 1.into(),
                max_value: replacement.clone(),
                value: replacement,
                index: 0,
            },
        )
        .unwrap();

        assert!(!structural.is_empty());
        assert_eq!(shadow.get(&7).map(|entry| entry.get().value), Some(link(8)));
    }

    #[test]
    fn logical_multi_batch_preserves_each_pair_and_event_order() {
        let shadow = BTreeMultiMap::<u64, Link>::default();
        let first = Pair { key: 7, value: link(1) };
        let second = Pair { key: 7, value: link(2) };
        let remove_first = ChangeEvent::RemoveAt {
            event_id: 2.into(),
            max_value: first.clone(),
            value: first.clone(),
            index: 0,
        };
        let insert_second = ChangeEvent::InsertAt {
            event_id: 1.into(),
            max_value: second.clone(),
            value: second,
            index: 0,
        };
        let insert_first = ChangeEvent::InsertAt {
            event_id: 0.into(),
            max_value: first.clone(),
            value: first,
            index: 0,
        };

        let structural = translate_logical_multi_batch(
            Path::new("multi.wt.idx"),
            &shadow,
            vec![remove_first, insert_second, insert_first],
        )
        .unwrap();

        assert!(!structural.is_empty());
        assert_eq!(shadow.iter().collect::<Vec<_>>(), vec![(7, link(2))]);
    }

    #[test]
    fn logical_multi_remove_detects_a_diverged_shadow() {
        let shadow = BTreeMultiMap::<u64, Link>::default();
        let pair = Pair { key: 7, value: link(9) };
        let error = translate_logical_multi_event(
            Path::new("multi.wt.idx"),
            &shadow,
            ChangeEvent::RemoveAt {
                event_id: 0.into(),
                max_value: pair.clone(),
                value: pair,
                index: 0,
            },
        )
        .unwrap_err();

        assert_eq!(error.path(), Path::new("multi.wt.idx"));
        assert!(error.reason().contains("shadow diverged"));
    }

    #[test]
    fn shuffled_large_logical_batch_replays_in_event_id_order() {
        let shadow = BTreeMap::<u64, Link>::default();
        let mut expected = StdBTreeMap::<u64, Link>::new();
        let mut events = Vec::new();

        for event_id in 0_u64..2_000 {
            let key = event_id.wrapping_mul(17) % 97;
            let event = if event_id % 5 == 0 {
                if let Some(old_link) = expected.remove(&key) {
                    let pair = Pair { key, value: old_link };
                    ChangeEvent::RemoveAt {
                        event_id: event_id.into(),
                        max_value: pair.clone(),
                        value: pair,
                        index: 0,
                    }
                } else {
                    let new_link = link(event_id as u32);
                    expected.insert(key, new_link);
                    let pair = Pair { key, value: new_link };
                    ChangeEvent::InsertAt {
                        event_id: event_id.into(),
                        max_value: pair.clone(),
                        value: pair,
                        index: 0,
                    }
                }
            } else {
                let new_link = link(event_id as u32);
                expected.insert(key, new_link);
                let pair = Pair { key, value: new_link };
                ChangeEvent::InsertAt {
                    event_id: event_id.into(),
                    max_value: pair.clone(),
                    value: pair,
                    index: 0,
                }
            };
            events.push(event);
        }

        fastrand::Rng::with_seed(0x10_91ca1).shuffle(&mut events);
        let structural = translate_logical_batch(Path::new("test.wt.idx"), &shadow, events).unwrap();

        assert!(!structural.is_empty());
        for key in 0..97 {
            assert_eq!(
                shadow.get(&key).map(|entry| entry.get().value),
                expected.get(&key).copied(),
                "key {key}"
            );
        }
    }
}
