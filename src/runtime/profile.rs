//! Named profiles used by owned query execution.
use crate::runtime::{Runtime, Tuning};

/// A named executor identity. The backend includes the Nagoya flavor and must
/// match the table declaration. It selects owned task submission, not the
/// table's portable lock implementation or its private persistence I/O pool.
/// Profiles emitted by `runtimes!` describe their backend with `tuning()`;
/// overriding tuning metadata alone does not reconfigure that backend.
pub trait Profile: 'static {
    /// The runtime this profile runs on. Must equal the table's, always.
    type Backend: Runtime;

    /// The pool settings this profile asks for.
    fn tuning() -> Tuning;

    /// Submission used by owned asynchronous select execution.
    fn dispatcher() -> super::Dispatch
    where
        <Self::Backend as Runtime>::JoinHandle<()>: Unpin,
    {
        super::dispatcher::<Self::Backend>
    }
}

/// Executor identity carried by generated hosted paged row types.
#[diagnostic::on_unimplemented(
    message = "{Self} has no hosted WorkTable runtime",
    label = "runtime selection needs a generated paged row with the std feature"
)]
pub trait TableRuntime {
    /// The declared backend and flavor, defaulting to Nagoya shared_slot.
    type Backend: Runtime;
}

/// A row whose select builders admit an explicit matching profile.
/// Generated hosted paged rows implement this. Mutation section annotations
/// govern their own methods, not selects, and therefore do not suppress it.
#[diagnostic::on_unimplemented(
    message = "{Self} does not permit select runtime selection",
    label = "this row must implement RuntimeUnpinned"
)]
pub trait RuntimeUnpinned {}
