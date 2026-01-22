use crate::vacuum::WorkTableVacuum;
use parking_lot::RwLock;
use smart_default::SmartDefault;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

/// Configuration for [`VacuumManager`].
#[derive(Debug, Clone, SmartDefault)]
pub struct VacuumManagerConfig {
    #[default(Duration::from_secs(60))]
    pub check_interval: Duration,
    #[default(3.0)]
    pub low_fragmentation_threshold: f64,
    #[default(1.5)]
    pub normal_fragmentation_threshold: f64,
    #[default(1.0)]
    pub high_fragmentation_threshold: f64,
    #[default(0.7)]
    pub critical_fragmentation_threshold: f64,
}

#[derive(derive_more::Debug)]
pub struct VacuumManager {
    pub config: VacuumManagerConfig,
    pub id_gen: AtomicU64,
    #[debug(ignore)]
    pub vacuums: Arc<RwLock<HashMap<u64, Box<dyn WorkTableVacuum>>>>,
}

impl VacuumManager {
    /// Creates a new vacuum manager with default configuration.
    pub fn new() -> Self {
        Self::with_config(VacuumManagerConfig::default())
    }

    /// Creates a new vacuum manager with the given configuration.
    pub fn with_config(config: VacuumManagerConfig) -> Self {
        Self {
            config,
            id_gen: Default::default(),
            vacuums: Arc::default(),
        }
    }
}
