use proc_macro2::TokenStream;
use quote::quote;

use crate::common::model::RuntimeBackend;

/// Generates the concrete runtime type selected by the DSL.
///
/// The twin of `index_backend::unique_index_type`, and deliberately shaped like
/// it: the DSL carries an enum, this turns the enum into a type the generated
/// code names, and nothing between the parser and the expansion has to know
/// which backend was chosen. The flavor is a type parameter rather than a
/// separate token because `NagoyaRt` is generic over it, so a table that picks
/// a tuning picks it at the type level and pays nothing at run time.
///
/// Every marker type, plus `NagoyaRt` and `TokioRt`, is re-exported from
/// `worktable::prelude`, so the expansion needs no import of its own.
///
/// The marker's spelling comes from [`Flavor::type_name`] rather than from a
/// match written here: a flavor added to the registry and missed here would
/// emit `NagoyaRt<Locality>` for a table that asked for something else, which
/// compiles and then silently measures the wrong pool.
pub(crate) fn runtime_type(backend: RuntimeBackend) -> TokenStream {
    match backend {
        RuntimeBackend::Nagoya(flavor) => {
            let marker = proc_macro2::Ident::new(flavor.type_name(), proc_macro2::Span::call_site());
            quote! { NagoyaRt<#marker> }
        }
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
    use crate::common::model::Flavor;

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
        assert_eq!(
            rendered(RuntimeBackend::Nagoya(Flavor::LowLatency)),
            "NagoyaRt < LowLatency >"
        );
        assert_eq!(
            rendered(RuntimeBackend::Nagoya(Flavor::WideInjector)),
            "NagoyaRt < WideInjector >"
        );
    }

    /// Every flavor has to emit a distinct type. A missing arm used to fall
    /// through to `Locality`, which compiles and then measures the wrong pool
    /// under the right name, so it is asserted rather than assumed.
    #[test]
    fn every_flavor_emits_its_own_marker() {
        let mut rendered: Vec<String> = Flavor::ALL
            .into_iter()
            .map(|flavor| super::runtime_type(RuntimeBackend::Nagoya(flavor)).to_string())
            .collect();
        let before = rendered.len();
        rendered.sort();
        rendered.dedup();
        assert_eq!(before, rendered.len(), "two flavors emit the same type: {rendered:?}");
    }

    /// An omitted `runtime:` and a bare `runtime: nagoya` are the same table,
    /// whichever flavor is currently the default. Asserted against
    /// `Flavor::default()` rather than a named flavor, so that moving the
    /// default is one edit rather than a hunt through the tests.
    #[test]
    fn the_default_backend_is_nagoya_at_the_default_flavor() {
        assert_eq!(
            rendered(RuntimeBackend::default()),
            rendered(RuntimeBackend::Nagoya(Flavor::default()))
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
        assert_eq!(resolve_runtime(None, None), RuntimeBackend::Nagoya(Flavor::default()));
    }
}
