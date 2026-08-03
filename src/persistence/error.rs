use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

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
