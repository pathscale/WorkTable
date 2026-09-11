//! `runtimes!` and the call-site `.runtime()` builder link.
//!
//! The cases that must **fail** to compile are drafted in `tests/ui-drafts/`
//! and belong to the trybuild lane; this file covers only what compiles, since
//! a test that a bound holds is a test that this file builds at all.

use worktable::prelude::*;
use worktable::runtimes;

runtimes! {
    fast_local: nagoya(locality),
    wide:       nagoya(spread),
    batch:      nagoya(throughput),
    bare:       nagoya,
}

// A tokio profile only resolves when the backend is in the graph. `TokioRt`
// lives behind `tokio-runtime`, which is off by default so that taking
// WorkTable off tokio stays true for anyone who does not ask for it back.
#[cfg(feature = "tokio-runtime")]
runtimes! {
    tokio_max: tokio,
}

/// Check the concrete type emitted for a profile. Callsite compatibility
/// separately admits different Nagoya flavors from the same backend family.
fn assert_backend<P, B>()
where
    P: Profile<Backend = B>,
    B: Runtime,
{
}

#[test]
fn each_profile_resolves_to_its_backend() {
    #[cfg(feature = "tokio-runtime")]
    assert_backend::<tokio_max, TokioRt>();
    assert_backend::<fast_local, NagoyaRt<Locality>>();
    assert_backend::<wide, NagoyaRt<Spread>>();
    assert_backend::<batch, NagoyaRt<Throughput>>();
}

#[test]
fn a_bare_backend_is_its_default_flavor() {
    assert_backend::<bare, NagoyaRt<Locality>>();
    assert_eq!(<bare as Profile>::tuning(), <Locality as FlavorMarker>::tuning());
}

#[test]
fn each_profile_resolves_to_its_tuning() {
    assert_eq!(<fast_local as Profile>::tuning(), Tuning::locality());
    assert_eq!(<wide as Profile>::tuning(), Tuning::spread());
    assert_eq!(<batch as Profile>::tuning(), Tuning::throughput());
    #[cfg(feature = "tokio-runtime")]
    assert_eq!(<tokio_max as Profile>::tuning(), Tuning::default());
}

#[test]
fn a_profile_is_a_value_as_well_as_a_type() {
    // What lets `.runtime(wide)` and `runtime wide:` spell the profile the same
    // way. A unit struct occupies both namespaces, so there is no case
    // convention between the schema and the call site.
    let _ = wide;
    assert_eq!(wide, <wide as Default>::default());
}

/// Stands in for a generated row type. The codegen lane emits both of these for
/// every table; a table without the first has no `.runtime()` at all, and one
/// without the second has it pinned by the schema.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Trade {
    id: u64,
}

impl TableRuntime for Trade {
    type Backend = NagoyaRt<Spread>;
}

impl RuntimeUnpinned for Trade {}

fn trades() -> SelectQueryBuilder<Trade, std::vec::IntoIter<Trade>, (), ()> {
    SelectQueryBuilder::new(vec![Trade { id: 1 }, Trade { id: 2 }].into_iter())
}

#[test]
fn a_matching_profile_compiles_and_records_its_tuning() {
    let builder = trades().limit(2).runtime(wide);
    assert_eq!(builder.params.tuning, Some(Tuning::spread()));
    assert_eq!(builder.params.limit, Some(2));
}

#[test]
fn no_runtime_call_records_no_tuning() {
    assert_eq!(trades().limit(2).params.tuning, None);
}

#[test]
fn runtime_chains_rather_than_widens() {
    // One argument, and the link sits among the others rather than replacing
    // any of them. A knob added later becomes a further link, never a second
    // argument here.
    let builder = trades().offset(1).runtime(wide).limit(1);
    assert_eq!(builder.params.tuning, Some(Tuning::spread()));
    assert_eq!(builder.params.offset, Some(1));
    assert_eq!(builder.params.limit, Some(1));
}
