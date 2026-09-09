use proc_macro2::{Delimiter, Ident, TokenTree};
use syn::spanned::Spanned as _;

use crate::model::{Flavor, RuntimeBackend};
use crate::parser::Parser;

/// Backends whose name the parser knows but whose code it cannot generate.
///
/// Kept as strings rather than [`RuntimeBackend`] variants for the reason
/// given on that enum: a variant is a promise that something downstream can
/// switch on it. Kept at all so that the message can say "not implemented"
/// rather than "no such backend", which are different mistakes and want
/// different next steps from the reader.
const RECOGNISED_UNIMPLEMENTED: &[&str] = &["forte", "blocking", "bwos"];

/// Duplicate `runtime:` at the table level.
///
/// Public because the free-order dispatch is what notices a repeat, and there
/// is more than one of those loops. The text lives here with the rest of the
/// runtime diagnostics so the three sites cannot drift apart.
pub const DUPLICATE_RUNTIME: &str = "duplicate `runtime` section; a declaration selects a runtime at most once";

const EXPECTED_BACKEND: &str = "expected a runtime backend after `runtime:`: `nagoya`, optionally flavored as \
     `nagoya(locality)`, `nagoya(spread)` or `nagoya(throughput)`, or `tokio`";

const EXPECTED_FLAVOR: &str = "expected a flavor inside the parentheses: `locality`, `spread` or `throughput`";

const TOKIO_HAS_NO_FLAVORS: &str =
    "`tokio` has no flavors; write `runtime: tokio`, or select a flavored runtime with `runtime: nagoya(spread)`";

const EXPECTED_PROFILE: &str = "expected a profile name after `runtime`, as in `update runtime fast_local:`; \
     profiles are declared with `runtimes!`";

impl Parser {
    /// Parse a table-level `runtime: <backend>` section.
    ///
    /// This is an arm of the free-order section loop, beside `columns`,
    /// `indexes`, `queries` and `config`, so it consumes its own keyword the
    /// way [`Parser::parse_indexes`] does. Duplicate detection belongs to the
    /// caller, which is the only thing that knows whether one was already
    /// read; [`DUPLICATE_RUNTIME`] is the message to use.
    pub fn parse_runtime(&mut self) -> syn::Result<RuntimeBackend> {
        let ident = self.input_iter.next().ok_or(syn::Error::new(
            self.input.span(),
            "Expected `runtime` field in declaration",
        ))?;
        if let TokenTree::Ident(ident) = &ident {
            if ident.to_string().as_str() != "runtime" {
                return Err(syn::Error::new(ident.span(), "Expected `runtime` field"));
            }
        } else {
            return Err(syn::Error::new(ident.span(), "Expected field name identifier."));
        };

        self.parse_colon()?;

        let backend = self
            .input_iter
            .next()
            .ok_or(syn::Error::new(self.input.span(), EXPECTED_BACKEND))?;
        let TokenTree::Ident(backend) = backend else {
            return Err(syn::Error::new_spanned(backend, EXPECTED_BACKEND));
        };

        let selected = self.parse_backend(&backend)?;

        self.try_parse_comma()?;

        Ok(selected)
    }

    /// The backend keyword and its optional postfix flavor.
    ///
    /// The postfix form is the house one: `columnar(chunk_rows(32_768))` in the
    /// `config` block reads the same way, and so does `using <backend>` on an
    /// index. A flavor is therefore an argument to the backend rather than a
    /// second key, which is what keeps `nagoya` alone meaning the default
    /// flavor rather than meaning "unset".
    fn parse_backend(&mut self, backend: &Ident) -> syn::Result<RuntimeBackend> {
        match backend.to_string().as_str() {
            "nagoya" => Ok(RuntimeBackend::Nagoya(self.try_parse_flavor()?.unwrap_or_default())),
            "tokio" => {
                if let Some(TokenTree::Group(group)) = self.input_iter.peek()
                    && group.delimiter() == Delimiter::Parenthesis
                {
                    let group = group.clone();
                    return Err(syn::Error::new_spanned(group, TOKIO_HAS_NO_FLAVORS));
                }
                Ok(RuntimeBackend::Tokio)
            }
            other if RECOGNISED_UNIMPLEMENTED.contains(&other) => Err(syn::Error::new_spanned(
                backend,
                format!(
                    "runtime backend `{other}` is recognised but not implemented; the implemented backends are \
                     `nagoya` and `tokio`"
                ),
            )),
            other => Err(syn::Error::new_spanned(
                backend,
                format!("unknown runtime backend `{other}`; expected `nagoya` or `tokio`"),
            )),
        }
    }

    /// `(locality)`, `(spread)` or `(throughput)`, if one was written.
    fn try_parse_flavor(&mut self) -> syn::Result<Option<Flavor>> {
        let Some(TokenTree::Group(group)) = self.input_iter.peek() else {
            return Ok(None);
        };
        if group.delimiter() != Delimiter::Parenthesis {
            return Ok(None);
        }
        let group = group.clone();
        self.input_iter.next();

        let mut inner = group.stream().into_iter();
        let flavor = inner
            .next()
            .ok_or_else(|| syn::Error::new_spanned(&group, EXPECTED_FLAVOR))?;
        let TokenTree::Ident(flavor) = flavor else {
            return Err(syn::Error::new_spanned(flavor, EXPECTED_FLAVOR));
        };
        if let Some(extra) = inner.next() {
            return Err(syn::Error::new_spanned(
                extra,
                "`nagoya` takes a single flavor; write one of `locality`, `spread` or `throughput`",
            ));
        }

        match flavor.to_string().as_str() {
            "locality" => Ok(Some(Flavor::Locality)),
            "spread" => Ok(Some(Flavor::Spread)),
            "throughput" => Ok(Some(Flavor::Throughput)),
            other => Err(syn::Error::new_spanned(
                &flavor,
                format!("unknown nagoya flavor `{other}`; expected `locality`, `spread` or `throughput`"),
            )),
        }
    }

    /// The optional `runtime <profile>` between a query section's keyword and
    /// its colon, as in `update runtime fast_local: { .. }`.
    ///
    /// The token after `runtime` is a profile name, never a backend literal.
    /// A section names a profile because a profile carries tuning as well as a
    /// backend, and because the backend is a property of the table rather than
    /// of one of its query blocks. Naming a backend here is therefore rejected
    /// rather than quietly treated as a profile that happens to be called
    /// `nagoya`.
    pub fn try_parse_section_runtime(&mut self) -> syn::Result<Option<Ident>> {
        let Some(TokenTree::Ident(keyword)) = self.input_iter.peek() else {
            return Ok(None);
        };
        if keyword != "runtime" {
            return Ok(None);
        }
        let keyword = keyword.clone();
        self.input_iter.next();

        let profile = self
            .input_iter
            .next()
            .ok_or_else(|| syn::Error::new_spanned(&keyword, EXPECTED_PROFILE))?;
        let TokenTree::Ident(profile) = profile else {
            return Err(syn::Error::new_spanned(profile, EXPECTED_PROFILE));
        };

        let name = profile.to_string();
        if name == "nagoya" || name == "tokio" || RECOGNISED_UNIMPLEMENTED.contains(&name.as_str()) {
            return Err(syn::Error::new_spanned(
                &profile,
                format!(
                    "`{name}` is a runtime backend, not a profile name; a query section names a profile declared \
                     with `runtimes!`, and the backend is selected once for the table with `runtime: {name}`"
                ),
            ));
        }

        Ok(Some(profile))
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use crate::Parser;
    use crate::model::{Flavor, RuntimeBackend};

    #[test]
    fn parses_bare_nagoya_as_locality() {
        let mut parser = Parser::new(quote! { runtime: nagoya, });
        assert_eq!(
            parser.parse_runtime().unwrap(),
            RuntimeBackend::Nagoya(Flavor::Locality)
        );
    }

    #[test]
    fn bare_nagoya_equals_the_default() {
        let mut parser = Parser::new(quote! { runtime: nagoya, });
        assert_eq!(parser.parse_runtime().unwrap(), RuntimeBackend::default());
    }

    #[test]
    fn parses_all_backends() {
        for (tokens, expected) in [
            (quote! { runtime: nagoya, }, RuntimeBackend::Nagoya(Flavor::Locality)),
            (
                quote! { runtime: nagoya(locality), },
                RuntimeBackend::Nagoya(Flavor::Locality),
            ),
            (
                quote! { runtime: nagoya(spread), },
                RuntimeBackend::Nagoya(Flavor::Spread),
            ),
            (
                quote! { runtime: nagoya(throughput), },
                RuntimeBackend::Nagoya(Flavor::Throughput),
            ),
            (quote! { runtime: tokio, }, RuntimeBackend::Tokio),
        ] {
            let mut parser = Parser::new(tokens);
            assert_eq!(parser.parse_runtime().unwrap(), expected);
        }
    }

    #[test]
    fn trailing_comma_is_optional() {
        let mut parser = Parser::new(quote! { runtime: nagoya(spread) });
        assert_eq!(parser.parse_runtime().unwrap(), RuntimeBackend::Nagoya(Flavor::Spread));
        assert!(!parser.has_next());
    }

    #[test]
    fn leaves_the_next_section_for_the_dispatch_loop() {
        let mut parser = Parser::new(quote! { runtime: tokio, columns: { id: u64 primary_key } });
        assert_eq!(parser.parse_runtime().unwrap(), RuntimeBackend::Tokio);
        assert_eq!(parser.peek_next().unwrap().to_string(), "columns");
    }

    #[test]
    fn rejects_recognised_but_unimplemented_backends() {
        for backend in ["forte", "blocking", "bwos"] {
            let tokens: proc_macro2::TokenStream = format!("runtime: {backend},").parse().unwrap();
            let error = Parser::new(tokens).parse_runtime().unwrap_err().to_string();
            assert!(
                error.contains(&format!("`{backend}` is recognised but not implemented")),
                "{error}"
            );
            assert!(error.contains("`nagoya` and `tokio`"), "{error}");
        }
    }

    #[test]
    fn rejects_unknown_backend() {
        let error = Parser::new(quote! { runtime: banana, })
            .parse_runtime()
            .unwrap_err()
            .to_string();
        assert_eq!(error, "unknown runtime backend `banana`; expected `nagoya` or `tokio`");
    }

    #[test]
    fn rejects_unknown_flavor() {
        let error = Parser::new(quote! { runtime: nagoya(banana), })
            .parse_runtime()
            .unwrap_err()
            .to_string();
        assert_eq!(
            error,
            "unknown nagoya flavor `banana`; expected `locality`, `spread` or `throughput`"
        );
    }

    #[test]
    fn rejects_a_flavor_on_tokio() {
        let error = Parser::new(quote! { runtime: tokio(spread), })
            .parse_runtime()
            .unwrap_err()
            .to_string();
        assert!(error.contains("`tokio` has no flavors"), "{error}");
    }

    #[test]
    fn rejects_two_flavors() {
        let error = Parser::new(quote! { runtime: nagoya(spread, locality), })
            .parse_runtime()
            .unwrap_err()
            .to_string();
        assert!(error.contains("takes a single flavor"), "{error}");
    }

    #[test]
    fn rejects_an_empty_flavor_list() {
        let error = Parser::new(quote! { runtime: nagoya(), })
            .parse_runtime()
            .unwrap_err()
            .to_string();
        assert!(error.contains("expected a flavor inside the parentheses"), "{error}");
    }

    #[test]
    fn rejects_a_missing_backend() {
        let error = Parser::new(quote! { runtime: })
            .parse_runtime()
            .unwrap_err()
            .to_string();
        assert!(error.contains("expected a runtime backend after `runtime:`"), "{error}");
    }

    #[test]
    fn section_annotation_reads_a_profile_name() {
        let mut parser = Parser::new(quote! { runtime fast_local: });
        let profile = parser.try_parse_section_runtime().unwrap().expect("annotated");
        assert_eq!(profile, "fast_local");
        assert_eq!(parser.peek_next().unwrap().to_string(), ":");
    }

    #[test]
    fn section_annotation_is_optional() {
        let mut parser = Parser::new(quote! { : { Fill(qty) by id } });
        assert!(parser.try_parse_section_runtime().unwrap().is_none());
        assert_eq!(parser.peek_next().unwrap().to_string(), ":");
    }

    #[test]
    fn section_annotation_rejects_a_backend_literal() {
        for backend in ["nagoya", "tokio", "forte"] {
            let tokens: proc_macro2::TokenStream = format!("runtime {backend}:").parse().unwrap();
            let error = Parser::new(tokens).try_parse_section_runtime().unwrap_err().to_string();
            assert!(
                error.contains(&format!("`{backend}` is a runtime backend, not a profile name")),
                "{error}"
            );
        }
    }

    #[test]
    fn section_annotation_rejects_a_missing_profile_name() {
        let error = Parser::new(quote! { runtime })
            .try_parse_section_runtime()
            .unwrap_err()
            .to_string();
        assert!(error.contains("expected a profile name after `runtime`"), "{error}");
    }

    /// The free-order dispatch, exercised through the whole declaration
    /// rather than through `parse_runtime` alone. Position is a property of
    /// the loop, so a test that calls the section parser directly cannot see
    /// it.
    fn schema(source: &str) -> crate::Schema {
        crate::Schema::parse(source).unwrap_or_else(|error| panic!("{error}\n{source}"))
    }

    #[test]
    fn an_omitted_runtime_is_the_default() {
        let schema = schema("name: Bare, columns: { id: u64 primary_key }");
        assert_eq!(schema.runtime, RuntimeBackend::default());
    }

    #[test]
    fn runtime_may_precede_columns() {
        let schema = schema(
            "
            name: First,
            runtime: nagoya(spread),
            columns: { id: u64 primary_key },
            ",
        );
        assert_eq!(schema.runtime, RuntimeBackend::Nagoya(Flavor::Spread));
    }

    #[test]
    fn runtime_may_follow_queries() {
        let schema = schema(
            "
            name: Last,
            columns: { id: u64 primary_key, qty: u64 },
            queries: { update: { Fill(qty) by id } },
            runtime: tokio,
            ",
        );
        assert_eq!(schema.runtime, RuntimeBackend::Tokio);
    }

    #[test]
    fn runtime_may_sit_between_indexes_and_config() {
        let schema = schema(
            "
            name: Middle,
            columns: { id: u64 primary_key, qty: u64 },
            indexes: { qty_idx: qty },
            runtime: nagoya(throughput),
            config: { page_size: 4096 },
            ",
        );
        assert_eq!(schema.runtime, RuntimeBackend::Nagoya(Flavor::Throughput));
    }

    #[test]
    fn rejects_a_second_runtime() {
        let error = crate::Schema::parse(
            "
            name: Twice,
            runtime: tokio,
            columns: { id: u64 primary_key },
            runtime: nagoya,
            ",
        )
        .unwrap_err()
        .to_string();
        assert_eq!(
            error,
            "duplicate `runtime` section; a declaration selects a runtime at most once"
        );
    }

    #[test]
    fn a_section_annotation_survives_the_whole_declaration() {
        let schema = schema(
            "
            name: Annotated,
            columns: { id: u64 primary_key, qty: u64, symbol: u64 },
            queries: {
                update runtime fast_local: { Fill(qty) by id },
                delete runtime wide: { BySymbol() by symbol },
                in_place: { Bump(qty) by id },
            },
            ",
        );
        assert_eq!(schema.queries.update_runtime.as_deref(), Some("fast_local"));
        assert_eq!(schema.queries.delete_runtime.as_deref(), Some("wide"));
        assert_eq!(schema.queries.in_place_runtime, None);
    }

    #[test]
    fn a_declared_runtime_survives_the_round_trip() {
        let source = "
            name: RoundTrip,
            columns: { id: u64 primary_key, qty: u64 },
            runtime: nagoya(spread),
            queries: { update runtime wide: { Fill(qty) by id } },
            ";
        let once = schema(source);
        let twice = schema(&once.to_dsl());
        assert_eq!(once, twice);
        assert_eq!(twice.runtime, RuntimeBackend::Nagoya(Flavor::Spread));
        assert_eq!(twice.queries.update_runtime.as_deref(), Some("wide"));
    }

    #[test]
    fn the_default_runtime_is_not_written_back_out() {
        // An omitted `runtime` and an explicit `runtime: nagoya` are the same
        // table, so the emitter writes neither.
        let dsl = schema("name: Quiet, runtime: nagoya, columns: { id: u64 primary_key }").to_dsl();
        assert!(!dsl.contains("runtime"), "{dsl}");
    }

    #[test]
    fn backend_names_round_trip() {
        assert_eq!(RuntimeBackend::Nagoya(Flavor::Spread).name(), "nagoya");
        assert_eq!(RuntimeBackend::Tokio.name(), "tokio");
        assert_eq!(Flavor::Locality.name(), "locality");
        assert_eq!(Flavor::Spread.name(), "spread");
        assert_eq!(Flavor::Throughput.name(), "throughput");
    }
}
