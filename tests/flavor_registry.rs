//! The registry and its mirror, held together.
//!
//! `worktable::runtime::Flavor` is the registry: stable discriminants, the
//! tuning each name selects, and the spelling `WT_DEFAULT_RUNTIME` parses.
//! `worktable_dsl::model::Flavor` is a mirror of it, and exists so the parser
//! can read a flavor without the runtime crate's dependencies.
//!
//! Two lists of the same thing drift. Nothing about adding a flavor to one and
//! not the other is a compile error: the DSL would simply reject a spelling
//! the runtime accepts, or the runtime would refuse a spelling a schema is
//! allowed to write. Both failures land at run time, on a benchmark arm, as a
//! panic or a silent fallback.

use worktable::prelude::Flavor;
use worktable_dsl::model::Flavor as Mirror;

#[test]
fn the_mirror_has_the_same_flavors_in_the_same_order() {
    let registry: Vec<&str> = Flavor::ALL.iter().map(|flavor| flavor.name()).collect();
    let mirror: Vec<&str> = Mirror::ALL.iter().map(|flavor| flavor.name()).collect();
    assert_eq!(
        registry, mirror,
        "the DSL's flavor mirror and the runtime registry disagree; \
         a flavor was added to one and not the other"
    );
}

#[test]
fn every_spelling_the_mirror_accepts_the_registry_accepts() {
    for mirrored in Mirror::ALL {
        let flavor = Flavor::from_name(mirrored.name())
            .unwrap_or_else(|error| panic!("the registry rejects `{}`: {error}", mirrored.name()));
        assert_eq!(flavor.name(), mirrored.name());
    }
}

#[test]
fn every_spelling_the_registry_accepts_the_mirror_accepts() {
    for flavor in Flavor::ALL {
        assert_eq!(
            Mirror::from_name(flavor.name()).map(|m| m.name()),
            Some(flavor.name()),
            "the DSL rejects `{}`, which the runtime accepts",
            flavor.name()
        );
    }
}

/// The marker type the macro emits has to be a real export, or a table that
/// names the flavor fails to compile in the consumer's crate with an error
/// pointing at generated code.
#[test]
fn every_mirrored_marker_type_is_spelled_the_way_the_prelude_exports_it() {
    // Named rather than derived, because the point is to check the string the
    // macro emits against the identifier that actually exists.
    let exported: &[(&str, &str)] = &[
        ("locality", "Locality"),
        ("spread", "Spread"),
        ("throughput", "Throughput"),
        ("low_latency", "LowLatency"),
        ("wide_injector", "WideInjector"),
        ("shared_slot", "SharedSlot"),
    ];
    assert_eq!(exported.len(), Flavor::ALL.len(), "a flavor has no marker listed here");
    for (name, type_name) in exported {
        let mirrored = Mirror::from_name(name).expect("the mirror knows every listed flavor");
        assert_eq!(mirrored.type_name(), *type_name);
    }
    // And that each of those identifiers resolves. A name that does not exist
    // is a compile error in this file, which is the point.
    let _: fn() -> worktable::prelude::Tuning =
        <worktable::prelude::Locality as worktable::prelude::FlavorMarker>::tuning;
    let _: fn() -> worktable::prelude::Tuning =
        <worktable::prelude::Spread as worktable::prelude::FlavorMarker>::tuning;
    let _: fn() -> worktable::prelude::Tuning =
        <worktable::prelude::Throughput as worktable::prelude::FlavorMarker>::tuning;
    let _: fn() -> worktable::prelude::Tuning =
        <worktable::prelude::LowLatency as worktable::prelude::FlavorMarker>::tuning;
    let _: fn() -> worktable::prelude::Tuning =
        <worktable::prelude::WideInjector as worktable::prelude::FlavorMarker>::tuning;
    let _: fn() -> worktable::prelude::Tuning =
        <worktable::prelude::SharedSlot as worktable::prelude::FlavorMarker>::tuning;
}

/// A marker's `FLAVOR` byte and its `tuning()` have to agree, or the pool a
/// table dispatches to is not the pool its declared flavor names.
#[test]
fn each_marker_resolves_to_its_own_registry_row() {
    use worktable::prelude::{FlavorMarker, Locality, LowLatency, SharedSlot, Spread, Throughput, WideInjector};

    assert_eq!(Locality::FLAVOR, Flavor::Locality);
    assert_eq!(Spread::FLAVOR, Flavor::Spread);
    assert_eq!(Throughput::FLAVOR, Flavor::Throughput);
    assert_eq!(LowLatency::FLAVOR, Flavor::LowLatency);
    assert_eq!(WideInjector::FLAVOR, Flavor::WideInjector);
    assert_eq!(SharedSlot::FLAVOR, Flavor::SharedSlot);

    assert_eq!(Locality::tuning(), Flavor::Locality.tuning());
    assert_eq!(Spread::tuning(), Flavor::Spread.tuning());
    assert_eq!(Throughput::tuning(), Flavor::Throughput.tuning());
    assert_eq!(LowLatency::tuning(), Flavor::LowLatency.tuning());
    assert_eq!(WideInjector::tuning(), Flavor::WideInjector.tuning());
    assert_eq!(SharedSlot::tuning(), Flavor::SharedSlot.tuning());
}
