//! Background logical-to-structural CDC translation for WorkTablesIndex.
//!
//! The disk shadow is reconstructed from the existing DataBucket index pages.
//! Foreground tables can therefore emit compact logical Set/Remove events while
//! this persistence-worker-owned index derives the structural events required
//! by the unchanged WTI disk format.

use std::fmt::Debug;
use std::hash::Hash;

use data_bucket::{Link, SizeMeasurable, SpaceId, VariableSizeMeasurable};
use eyre::bail;
use indexset::cdc::change::ChangeEvent;
use indexset::concurrent::map::BTreeMap;
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
use crate::persistence::space::BatchChangeEvent;
use crate::persistence::{SpaceIndex, SpaceIndexOps, SpaceIndexUnsized};
use crate::prelude::WT_INDEX_EXTENSION;

fn translate_logical_event<T, Node>(
    shadow: &BTreeMap<T, Link, Node>,
    event: ChangeEvent<Pair<T, Link>>,
) -> eyre::Result<Vec<ChangeEvent<Pair<T, Link>>>>
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
            let (removed, events) = shadow.remove_cdc(&value.key);
            if removed.as_ref().map(|(_, link)| *link) != Some(value.value) {
                bail!(
                    "logical WTI shadow diverged while removing key {:?}: expected {:?}, found {:?}",
                    value.key,
                    value.value,
                    removed.map(|(_, link)| link),
                );
            }
            Ok(events)
        }
        _ => bail!("logical WTI persistence received a structural or malformed event"),
    }
}

fn translate_logical_batch<T, Node>(
    shadow: &BTreeMap<T, Link, Node>,
    events: BatchChangeEvent<T>,
) -> eyre::Result<BatchChangeEvent<T>>
where
    T: Debug + Eq + Hash + Clone + Send + Ord + 'static,
    Node: NodeLike<Pair<T, Link>> + Send + 'static,
{
    let mut structural = Vec::new();
    for event in events {
        structural.extend(translate_logical_event(shadow, event)?);
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
        let mut disk = SpaceIndex::new(path, SpaceId::from(0), version).await?;
        let shadow = disk.parse_indexset().await?;
        Ok(Self { shadow, disk })
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
        let events = translate_logical_event(&self.shadow, event)?;
        self.disk.process_change_event_batch(events).await
    }

    async fn process_change_event_batch(&mut self, events: BatchChangeEvent<T>) -> eyre::Result<()> {
        let events = translate_logical_batch(&self.shadow, events)?;
        self.disk.process_change_event_batch(events).await
    }
}

/// Variable-sized-key counterpart to [`SpaceLogicalIndex`].
pub struct SpaceLogicalIndexUnsized<T, const INNER_PAGE_SIZE: u32>
where
    T: Send + Ord + Eq + Clone + Default + Debug + SizeMeasurable + VariableSizeMeasurable + 'static,
{
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
        let mut disk = SpaceIndexUnsized::new(path, SpaceId::from(0), version).await?;
        let shadow = disk.parse_indexset().await?;
        Ok(Self { shadow, disk })
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
        let events = translate_logical_event(&self.shadow, event)?;
        self.disk.process_change_event_batch(events).await
    }

    async fn process_change_event_batch(&mut self, events: BatchChangeEvent<T>) -> eyre::Result<()> {
        let events = translate_logical_batch(&self.shadow, events)?;
        self.disk.process_change_event_batch(events).await
    }
}

#[cfg(test)]
mod tests {
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
}
