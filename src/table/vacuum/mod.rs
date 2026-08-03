use async_trait::async_trait;

use data_bucket::Link;
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;

use crate::persistence::PersistenceResult;
use crate::vacuum::fragmentation_info::FragmentationInfo;

mod fragmentation_info;
mod manager;
#[allow(clippy::module_inception)]
mod vacuum;

pub use manager::{VacuumManager, VacuumManagerConfig};
pub use vacuum::EmptyDataVacuum;

/// Sink for persisting vacuum row moves.
///
/// Vacuum relocates rows between data pages, which changes the [`Link`] stored
/// in the primary and secondary indexes. On persisted tables those index
/// mutations must go through the CDC event stream, and the moved row bytes must
/// be written at the new link — otherwise the on-disk state goes stale and the
/// event-id sequence gets a permanent gap that stalls persistence. Implementors
/// receive everything needed to queue a proper persistence operation for one
/// moved row.
/// Not intended for downstream implementation: this is macro-support API for
/// the generated persisted-table `vacuum()`, and it leaks low-level CDC event
/// types. Hidden from docs; semver stability is not promised for it.
#[doc(hidden)]
pub trait VacuumPersistence<PrimaryKey, SecondaryEvents>: Send + Sync {
    /// Queue a persistence operation for a row moved to `new_link`, carrying
    /// the row bytes and the CDC events produced by the index updates.
    fn apply_move(
        &self,
        bytes: Vec<u8>,
        new_link: Link,
        primary_key_events: Vec<ChangeEvent<Pair<PrimaryKey, Link>>>,
        secondary_keys_events: SecondaryEvents,
    ) -> PersistenceResult;
}

/// Trait for unifying different [`WorkTable`] related [`EmptyDataVacuum`]'s.
///
/// [`WorkTable`]: crate::prelude::WorkTable
/// [`EmptyDataVacuum`]: vacuum::EmptyDataVacuum
#[async_trait]
pub trait WorkTableVacuum {
    /// Get table name for diagnostics
    fn table_name(&self) -> &str;
    /// Analyze current fragmentation state
    fn analyze_fragmentation(&self) -> FragmentationInfo;
    /// Run vacuum operation
    async fn vacuum(&self) -> eyre::Result<VacuumStats>;
}

/// Represents vacuum statistics after a vacuum operation
#[derive(Debug, Clone)]
pub struct VacuumStats {
    pub pages_processed: usize,
    pub pages_freed: usize,
    pub bytes_freed: u64,
    pub duration_ns: u128,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum VacuumPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}
