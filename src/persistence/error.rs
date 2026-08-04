use std::error::Error;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A persisted table could not be loaded without risking invalid data.
///
/// WorkTable persistence is best-effort rather than crash-atomic. Abrupt
/// process or power loss may therefore leave a partial batch on disk. `load`
/// reports that condition with this concrete error type instead of exposing
/// torn bytes as rows. Callers using the `eyre::Result`-based
/// [`PersistedWorkTable`](crate::persistence::PersistedWorkTable) API can
/// identify it with [`eyre::Report::downcast_ref`].
#[derive(Debug)]
pub struct PersistenceLoadError {
    path: PathBuf,
    reason: String,
}

impl PersistenceLoadError {
    pub fn corrupt(path: impl AsRef<Path>, reason: impl Display) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            reason: reason.to_string(),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl Display for PersistenceLoadError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "torn or corrupt persisted table at {}: {}",
            self.path.display(),
            self.reason
        )
    }
}

impl Error for PersistenceLoadError {}

/// Terminal and lifecycle errors reported by a persistence task.
#[derive(Debug)]
pub enum PersistenceError {
    /// New work was submitted after graceful shutdown began.
    Closing,
    /// New work was submitted after graceful shutdown completed.
    Closed,
    /// The persistence engine or its queue analyzer failed permanently.
    Engine(eyre::Report),
}

impl Display for PersistenceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closing => formatter.write_str("persistence task is closing"),
            Self::Closed => formatter.write_str("persistence task is closed"),
            Self::Engine(error) => write!(formatter, "persistence engine failed: {error:#}"),
        }
    }
}

impl Error for PersistenceError {}

pub type PersistenceResult<T = ()> = Result<T, Arc<PersistenceError>>;

/// Observable state of the persistence worker.
#[derive(Clone, Debug)]
pub enum PersistenceState {
    Running,
    Closing,
    Failed(Arc<PersistenceError>),
    Closed,
}
