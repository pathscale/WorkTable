use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::task::AbortHandle;

use parking_lot::RwLock;
use smart_default::SmartDefault;

use crate::vacuum::WorkTableVacuum;

/// How long a sweep waits before checking a table nothing has woken it about.
///
/// Not configurable, and deliberately so. Sweeps are triggered by tables
/// actually freeing space; this only bounds how long a table whose threshold is
/// never reached goes unlooked-at. Exposing it invited callers to turn it down
/// and get the polling behaviour this design replaced, where the timer wins
/// every wake and neither the threshold nor the settle does anything.
const FALLBACK_INTERVAL: Duration = Duration::from_secs(60);

/// Configuration for [`VacuumManager`].
#[derive(Debug, Clone, SmartDefault)]
pub struct VacuumManagerConfig {
    /// Reclaimable bytes at which a table wakes the sweep task.
    ///
    /// Small enough to react while the fragmentation is still cheap to
    /// reclaim — measured, a sweep at 25% fragmentation costs between nothing
    /// and 10% of insert throughput, and one at 60% costs 25-49%.
    #[default(1024 * 1024)]
    pub wake_threshold_bytes: u64,
    #[default(3.0)]
    pub low_fragmentation_threshold: f64,
    #[default(1.5)]
    pub normal_fragmentation_threshold: f64,
    #[default(1.0)]
    pub high_fragmentation_threshold: f64,
    #[default(0.7)]
    pub critical_fragmentation_threshold: f64,
}

/// What the manager has actually done, cumulatively.
///
/// Cost is unreadable without it. A sweep that stands down so hard it never
/// runs reports no overhead at all, which looks like a win and is a
/// regression in the thing vacuum exists for. This is also how you tell a
/// reactive sweep that keeps firing from one that fired once and never
/// again, which a snapshot of the table cannot distinguish.
#[derive(Debug, Default)]
pub struct VacuumManagerStats {
    pub sweeps: AtomicU64,
    pub pages_freed: AtomicU64,
    pub bytes_freed: AtomicU64,
}

impl VacuumManagerStats {
    pub fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.sweeps.load(Ordering::Relaxed),
            self.pages_freed.load(Ordering::Relaxed),
            self.bytes_freed.load(Ordering::Relaxed),
        )
    }
}

#[derive(derive_more::Debug, Default)]
pub struct VacuumManager {
    pub config: VacuumManagerConfig,
    pub id_gen: AtomicU64,
    /// Cumulative record of sweeps run. See [`VacuumManagerStats`].
    pub stats: VacuumManagerStats,
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
            stats: Default::default(),
            vacuums: Arc::default(),
        }
    }

    /// Registers a new vacuum with the manager and returns its unique ID.
    pub fn register(&self, table: Arc<dyn WorkTableVacuum + Send + Sync>) -> u64 {
        table.arm_wake(self.config.wake_threshold_bytes);
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
                self.wait_for_work().await;

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
                        if info.overall_fragmentation_ratio < self.config.low_fragmentation_threshold
                            && info.overall_fragmentation_ratio != 0.0
                        {
                            log::debug!("Vacuuming {}", info.table_name);
                            match vacuum.vacuum().await {
                                Ok(stats) => {
                                    self.stats.sweeps.fetch_add(1, Ordering::Relaxed);
                                    self.stats
                                        .pages_freed
                                        .fetch_add(stats.pages_freed as u64, Ordering::Relaxed);
                                    self.stats.bytes_freed.fetch_add(stats.bytes_freed, Ordering::Relaxed);
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

    /// Blocks until some registered table has freed enough space to be worth
    /// a sweep, or the fallback interval elapses.
    async fn wait_for_work(&self) {
        let registered: Vec<_> = {
            let vacuums = self.vacuums.read();
            vacuums.values().cloned().collect()
        };
        if registered.is_empty() {
            tokio::time::sleep(FALLBACK_INTERVAL).await;
            return;
        }

        let waits: Vec<_> = registered.iter().map(|v| v.wait_until_worth_running()).collect();
        tokio::select! {
            _ = futures::future::select_all(waits) => {}
            _ = tokio::time::sleep(FALLBACK_INTERVAL) => {}
        }
    }
}
