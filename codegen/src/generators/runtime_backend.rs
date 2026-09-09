use proc_macro2::TokenStream;
use quote::quote;

use crate::common::model::{Flavor, RuntimeBackend};

/// Generates the concrete runtime type selected by the DSL.
///
/// The twin of `index_backend::unique_index_type`, and deliberately shaped like
/// it: the DSL carries an enum, this turns the enum into a type the generated
/// code names, and nothing between the parser and the expansion has to know
/// which backend was chosen. The flavor is a type parameter rather than a
/// separate token because `NagoyaRt` is generic over it, so a table that picks
/// a tuning picks it at the type level and pays nothing at run time.
///
/// All four names, plus `Locality` / `Spread` / `Throughput`, are re-exported
/// from `worktable::prelude`, so the expansion needs no import of its own.
pub(crate) fn runtime_type(backend: RuntimeBackend) -> TokenStream {
    match backend {
        RuntimeBackend::Nagoya(Flavor::Locality) => quote! { NagoyaRt<Locality> },
        RuntimeBackend::Nagoya(Flavor::Spread) => quote! { NagoyaRt<Spread> },
        RuntimeBackend::Nagoya(Flavor::Throughput) => quote! { NagoyaRt<Throughput> },
        RuntimeBackend::Tokio => quote! { TokioRt },
    }
}

/// Resolves which backend applies at one site.
///
/// The chain is: a section's own annotation, then the table's `runtime:`, then
/// the built-in default. The middle step is the one worth stating: a table that
/// declares `runtime: tokio` and has an unannotated `update` section must give
/// that section tokio, not the built-in nagoya, or the table would silently run
/// two runtimes.
///
/// `None` for both arguments must produce exactly what `runtime: nagoya`
/// produces, because every declaration written before this existed omits the
/// key and none of them may change.
pub(crate) fn resolve_runtime(section: Option<RuntimeBackend>, table: Option<RuntimeBackend>) -> RuntimeBackend {
    section.or(table).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rendered(backend: RuntimeBackend) -> String {
        runtime_type(backend).to_string()
    }

    #[test]
    fn every_backend_maps_to_its_contract_type() {
        assert_eq!(
            rendered(RuntimeBackend::Nagoya(Flavor::Locality)),
            "NagoyaRt < Locality >"
        );
        assert_eq!(rendered(RuntimeBackend::Nagoya(Flavor::Spread)), "NagoyaRt < Spread >");
        assert_eq!(
            rendered(RuntimeBackend::Nagoya(Flavor::Throughput)),
            "NagoyaRt < Throughput >"
        );
        assert_eq!(rendered(RuntimeBackend::Tokio), "TokioRt");
    }

    #[test]
    fn the_default_backend_is_nagoya_locality() {
        assert_eq!(
            rendered(RuntimeBackend::default()),
            rendered(RuntimeBackend::Nagoya(Flavor::Locality))
        );
    }

    #[test]
    fn a_section_annotation_wins_over_the_table() {
        assert_eq!(
            resolve_runtime(
                Some(RuntimeBackend::Nagoya(Flavor::Spread)),
                Some(RuntimeBackend::Tokio)
            ),
            RuntimeBackend::Nagoya(Flavor::Spread)
        );
    }

    #[test]
    fn an_unannotated_section_falls_back_to_the_table_not_the_default() {
        assert_eq!(
            resolve_runtime(None, Some(RuntimeBackend::Tokio)),
            RuntimeBackend::Tokio
        );
        assert_eq!(
            resolve_runtime(None, Some(RuntimeBackend::Nagoya(Flavor::Throughput))),
            RuntimeBackend::Nagoya(Flavor::Throughput)
        );
    }

    #[test]
    fn neither_declared_resolves_to_the_built_in_default() {
        assert_eq!(resolve_runtime(None, None), RuntimeBackend::Nagoya(Flavor::Locality));
    }
}
