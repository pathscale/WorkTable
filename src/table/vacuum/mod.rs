use async_trait::async_trait;

use crate::vacuum::fragmentation_info::FragmentationInfo;

mod fragmentation_info;
mod manager;
#[allow(clippy::module_inception)]
mod vacuum;

pub use manager::{VacuumManager, VacuumManagerConfig};
pub use vacuum::EmptyDataVacuum;

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
