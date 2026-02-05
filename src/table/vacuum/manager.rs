use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::task::AbortHandle;

use parking_lot::RwLock;
use smart_default::SmartDefault;

use crate::vacuum::WorkTableVacuum;

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

#[derive(derive_more::Debug, Default)]
pub struct VacuumManager {
    pub config: VacuumManagerConfig,
    pub id_gen: AtomicU64,
    #[debug(ignore)]
    pub vacuums: Arc<RwLock<HashMap<u64, Arc<dyn WorkTableVacuum + Send + Sync>>>>,
}

impl VacuumManager {
    /// Creates a new vacuum manager with default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new vacuum manager with the given configuration.
    pub fn with_config(config: VacuumManagerConfig) -> Self {
        Self {
            config,
            id_gen: Default::default(),
            vacuums: Arc::default(),
        }
    }

    /// Registers a new vacuum with the manager and returns its unique ID.
    pub fn register(&self, table: Arc<dyn WorkTableVacuum + Send + Sync>) -> u64 {
        let id = self.id_gen.fetch_add(1, Ordering::AcqRel);
        let mut vacuums = self.vacuums.write();
        vacuums.insert(id, table);
        id
    }

    /// Starts a background task that periodically checks fragmentation and runs
    /// vacuum.
    ///
    /// Returns an `AbortHandle` that can be used to cancel the task.
    pub fn run_vacuum_task(self: Arc<Self>) -> AbortHandle {
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(self.config.check_interval).await;

                let vacuums_to_check: Vec<_> = {
                    let vacuums_read = self.vacuums.read();
                    vacuums_read
                        .iter()
                        .map(|(id, v)| (*id, v.table_name().to_string()))
                        .collect()
                };

                for (id, table_name) in vacuums_to_check {
                    let vacuum_opt = {
                        let vacuums_read = self.vacuums.read();
                        vacuums_read.get(&id).cloned()
                    };

                    if let Some(vacuum) = vacuum_opt {
                        let info = vacuum.analyze_fragmentation();

                        log::debug!("vacuum info: {:?}", info);
                        // println!("vacuum info: {:?}", info);
                        if info.overall_fragmentation_ratio
                            < self.config.low_fragmentation_threshold
                            && info.overall_fragmentation_ratio != 0.0
                        {
                            log::debug!("Vacuuming {}", info.table_name);
                            match vacuum.vacuum().await {
                                Ok(stats) => {
                                    // println!(
                                    //     "Vacuum completed for table '{}': {} pages processed, {} bytes freed in {:.2}ms",
                                    //     table_name,
                                    //     stats.pages_processed,
                                    //     stats.bytes_freed,
                                    //     stats.duration_ns as f64 / 1_000_000.0
                                    // );
                                    log::debug!(
                                        "Vacuum completed for table '{}': {} pages processed, {} bytes freed in {:.2}ms",
                                        table_name,
                                        stats.pages_processed,
                                        stats.bytes_freed,
                                        stats.duration_ns as f64 / 1_000_000.0
                                    );
                                }
                                Err(e) => {
                                    // println!("Vacuum failed for table '{}': {}", table_name, e);
                                    log::debug!("Vacuum failed for table '{}': {}", table_name, e);
                                }
                            }
                        }
                    }
                }
            }
        });

        handle.abort_handle()
    }
}
