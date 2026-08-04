use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::FutureExt;

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

/// Contains dependency panics while decoding an existing persisted store.
///
/// Some lower-level page decoders still panic when torn bytes violate their
/// internal invariants. A persisted table is not exposed until the entire
/// decode and validation pipeline succeeds, so the future and all partially
/// decoded state are discarded on unwind. `AssertUnwindSafe` is used only to
/// establish that containment boundary; no value from the failed future is
/// reused.
#[doc(hidden)]
pub async fn load_persisted_state<T, F>(path: impl AsRef<Path>, future: F) -> Result<T, PersistenceLoadError>
where
    F: Future<Output = eyre::Result<T>>,
{
    let path = path.as_ref();
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(PersistenceLoadError::corrupt(path, format!("{error:#}"))),
        Err(payload) => {
            let reason = payload
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("persisted-state loader panicked");
            Err(PersistenceLoadError::corrupt(path, reason))
        }
    }
}

/// A persisted index no longer agrees with the logical mutation stream.
///
/// The safe quarantine boundary is the table's entire persistence engine: a
/// worker must not continue writing row data or sibling indexes after one
/// index diverges, because that would knowingly create a store that cannot be
/// recovered consistently. The in-memory table remains inspectable, while
/// `wait_for_ops`, `close`, and all later persistence submissions return this
/// terminal error through [`PersistenceError::IndexCorruption`].
#[derive(Debug)]
pub struct PersistenceIndexCorruption {
    path: PathBuf,
    reason: String,
}

impl PersistenceIndexCorruption {
    pub fn new(path: impl AsRef<Path>, reason: impl Display) -> Self {
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

impl Display for PersistenceIndexCorruption {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "persisted index at {} was quarantined: {}",
            self.path.display(),
            self.reason
        )
    }
}

impl Error for PersistenceIndexCorruption {}

/// Terminal and lifecycle errors reported by a persistence task.
#[derive(Debug)]
pub enum PersistenceError {
    /// New work was submitted after graceful shutdown began.
    Closing,
    /// New work was submitted after graceful shutdown completed.
    Closed,
    /// An index diverged from its validated logical mutation stream.
    IndexCorruption(PersistenceIndexCorruption),
    /// The persistence engine or its queue analyzer failed permanently.
    Engine(eyre::Report),
}

impl Display for PersistenceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closing => formatter.write_str("persistence task is closing"),
            Self::Closed => formatter.write_str("persistence task is closed"),
            Self::IndexCorruption(error) => Display::fmt(error, formatter),
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

#[cfg(test)]
mod tests {
    use super::{PersistenceLoadError, load_persisted_state};

    #[tokio::test]
    async fn persisted_state_panics_are_typed_load_errors() {
        let error = load_persisted_state("table/path", async {
            panic!("dependency decoder rejected torn bytes");
            #[allow(unreachable_code)]
            Ok::<(), eyre::Report>(())
        })
        .await
        .unwrap_err();

        assert_eq!(error.path(), std::path::Path::new("table/path"));
        assert_eq!(error.reason(), "dependency decoder rejected torn bytes");
    }

    #[tokio::test]
    async fn persisted_state_errors_are_typed_load_errors() {
        let error: PersistenceLoadError = load_persisted_state("table/path", async {
            Err::<(), _>(eyre::eyre!("invalid persisted page"))
        })
        .await
        .unwrap_err();

        assert_eq!(error.path(), std::path::Path::new("table/path"));
        assert_eq!(error.reason(), "invalid persisted page");
    }
}
