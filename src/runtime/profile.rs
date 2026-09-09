//! Named runtime profiles: what `runtimes!` produces and what `.runtime()`
//! accepts.

use crate::runtime::{Runtime, Tuning};

/// One named runtime profile.
///
/// A profile is a **type**, not a value, and that is load-bearing. The backend
/// is fixed at the table by its `runtime:` declaration, because it selects the
/// `RwLock`, `Notify` and `JoinHandle` that `LockMap` and `PersistenceTask` are
/// built from, so nothing downstream can change it. Carrying the backend as
/// [`Profile::Backend`] makes naming a `tokio` profile on a `nagoya` table an
/// equality that fails to hold, and the compiler then prints both backends. A
/// profile that were a bare name, or an enum variant, could only fail later and
/// further away.
///
/// # Room to grow
///
/// `runtimes!` emits each profile as a **unit struct**, not as a variant of an
/// enum, so that the parameters the design defers can arrive as fields:
///
/// ```ignore
/// runtimes! {
///     wide:    nagoya(spread),
///     wide_12: nagoya(spread) { workers: 12, backoff_spins: 4096 },
/// }
/// ```
///
/// That is a change to the generated struct and to [`Profile::tuning`], and no
/// call site moves. The same parameters passed positionally to `.runtime()`
/// would be an arity change, which breaks every existing call, which is why
/// `.runtime()` takes exactly one argument and any future knob arrives as a
/// further builder link (`.runtime(wide).workers(12)`) instead.
pub trait Profile: 'static {
    /// The runtime this profile runs on. Must equal the table's, always.
    type Backend: Runtime;

    /// The pool settings this profile asks for.
    fn tuning() -> Tuning;
}

/// The backend a table's queries run on, hung off the table's row type.
///
/// The row type is the one type every select builder for a table carries, so it
/// is where the table's half of the `.runtime()` equality has to live.
/// Generated code emits this for **every** table, whatever its `runtime:` says
/// and whether or not it has one.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a WorkTable row type, so it has no runtime to match",
    label = "`.runtime()` needs a table's row type here"
)]
pub trait TableRuntime {
    /// The backend fixed by the table's `runtime:` declaration, defaulting to
    /// `nagoya(locality)` when it has none.
    ///
    /// # An open question this type decides
    ///
    /// `.runtime()` requires `P::Backend == Self::Backend` exactly, so what is
    /// written here also decides whether a call site may change the *flavor*.
    /// `NagoyaRt<Spread>` here admits only spread profiles; a single flavor
    /// marker for every nagoya table admits all three, since the `RwLock`,
    /// `Notify` and `JoinHandle` a nagoya table is built from do not vary with
    /// the flavor. The contract's section 4 emits the declared flavor for the
    /// table type, and its section 6 asks for exact equality here; the design
    /// note also says the flavor is selectable at the call site. Those three
    /// cannot all hold. Nothing in this file picks: whatever the codegen lane
    /// writes here is what the compiler will enforce.
    type Backend: Runtime;
}

/// A table whose runtime the schema left open, so a call site may choose one.
///
/// Generated code emits `impl RuntimeUnpinned for MyRow {}` beside the
/// [`TableRuntime`] impl, and **withholds it** for a table whose section
/// annotation already named a profile. Pinning is the absence of this impl.
///
/// # Why absence, and why on the row type
///
/// This exists so the both-defined case is a **bound that does not hold**
/// rather than a missing method. Omitting `.runtime()` from a pinned builder
/// would report "no method named `runtime` found for struct
/// `SelectQueryBuilder`", which points at the builder instead of at the two
/// declarations that disagree.
///
/// Two things then force the shape. A blanket impl carrying the condition in a
/// where clause does not work: rustc reports the innermost unsatisfied
/// obligation, so the message becomes a complaint about whatever marker the
/// clause named, or a type mismatch, and `#[diagnostic::on_unimplemented]`
/// never fires. Only a missing impl on the bound's own `Self` produces the
/// message below. And that `Self` cannot be the builder: `SelectQueryBuilder`
/// is foreign to the generated code and a local row type nested inside it does
/// not make the impl local, so the orphan rule rejects it. The row type is what
/// is left, and it is also the more useful name to print, being the table.
#[diagnostic::on_unimplemented(
    message = "`{Self}` already has a runtime pinned by the schema",
    label = "remove this `.runtime()`, or remove `runtime` from the section"
)]
pub trait RuntimeUnpinned {}
