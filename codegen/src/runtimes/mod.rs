//! The `runtimes!` macro: the process's named runtime profiles, in one block.
//!
//! ```ignore
//! runtimes! {
//!     tokio_max:  tokio,
//!     fast_local: nagoya(locality),
//!     wide:       nagoya(spread),
//! }
//! ```
//!
//! Each entry becomes a unit struct implementing `worktable::prelude::Profile`,
//! named exactly as written. One identifier serves as both the type and the
//! value, which is what lets a call site write `.runtime(wide)` and a schema
//! section write `runtime wide:` without a case convention between them.
//!
//! The struct's `Backend` associated type is the load-bearing part. A call site
//! naming a profile from the wrong backend then fails as an equality that does
//! not hold, and the compiler prints both backend types; without it the same
//! mistake would surface much further downstream, as whatever the mismatched
//! `RwLock` or `JoinHandle` broke first.

use indexmap::IndexMap;
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Error, Token, parenthesized};

/// Backends named in the DSL but not built. Recognised only so the message can
/// say what happened, per the rule that an inert declaration is an error rather
/// than a thing that silently does nothing.
const NOT_IMPLEMENTED: &[&str] = &["forte", "blocking", "bwos"];

/// What is built, in the order the message should list them.
const IMPLEMENTED: &[&str] = &["nagoya", "tokio"];

/// The three nagoya flavors, in the order the message should list them.
const FLAVORS: &[&str] = &["locality", "spread", "throughput"];

/// One `name: backend(flavor)` entry, resolved.
struct ProfileEntry {
    name: Ident,
    /// `Some` for nagoya, `None` for tokio. Kept as the source ident rather
    /// than an enum so the emitted marker type and the span both come from
    /// what was written.
    backend: Ident,
    flavor: Option<Ident>,
}

struct Runtimes {
    profiles: IndexMap<String, ProfileEntry>,
}

impl Parse for Runtimes {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut profiles: IndexMap<String, ProfileEntry> = IndexMap::new();

        while !input.is_empty() {
            let name: Ident = input.parse()?;
            input.parse::<Token![:]>()?;
            let backend: Ident = input.parse()?;

            let flavor = if input.peek(syn::token::Paren) {
                let inner;
                parenthesized!(inner in input);
                let flavor: Ident = inner.parse()?;
                if !inner.is_empty() {
                    return Err(Error::new(
                        inner.span(),
                        "a runtime takes one flavor and nothing else; worker counts and durations are not \
                         call-site parameters",
                    ));
                }
                Some(flavor)
            } else {
                None
            };

            let entry = resolve(name, backend, flavor)?;

            if let Some(previous) = profiles.get(&entry.name.to_string()) {
                let mut err = Error::new(entry.name.span(), format!("duplicate runtime profile `{}`", entry.name));
                err.combine(Error::new(
                    previous.name.span(),
                    format!("`{}` was already declared here", previous.name),
                ));
                return Err(err);
            }
            profiles.insert(entry.name.to_string(), entry);

            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
        }

        Ok(Self { profiles })
    }
}

/// Checks one entry against the backends and flavors that exist.
///
/// Everything rejected here is rejected at expansion rather than accepted inert,
/// so a declaration that reads as if it selected something either did or failed
/// to build.
fn resolve(name: Ident, backend: Ident, flavor: Option<Ident>) -> syn::Result<ProfileEntry> {
    let backend_name = backend.to_string();

    if NOT_IMPLEMENTED.contains(&backend_name.as_str()) {
        return Err(Error::new(
            backend.span(),
            format!(
                "runtime backend `{backend_name}` is not implemented; the backends that are: {}",
                IMPLEMENTED.join(", ")
            ),
        ));
    }

    match backend_name.as_str() {
        "nagoya" => {
            let flavor = match flavor {
                None => Ident::new("locality", backend.span()),
                Some(flavor) => {
                    let flavor_name = flavor.to_string();
                    if !FLAVORS.contains(&flavor_name.as_str()) {
                        return Err(Error::new(
                            flavor.span(),
                            format!(
                                "unknown nagoya flavor `{flavor_name}`; expected one of: {}",
                                FLAVORS.join(", ")
                            ),
                        ));
                    }
                    flavor
                }
            };
            Ok(ProfileEntry {
                name,
                backend,
                flavor: Some(flavor),
            })
        }
        "tokio" => {
            if let Some(flavor) = flavor {
                return Err(Error::new(
                    flavor.span(),
                    "tokio has no flavors; write `tokio`. Flavors belong to nagoya, whose pool they tune",
                ));
            }
            Ok(ProfileEntry {
                name,
                backend,
                flavor: None,
            })
        }
        _ => Err(Error::new(
            backend.span(),
            format!(
                "unknown runtime backend `{backend_name}`; expected one of: {}",
                IMPLEMENTED.join(", ")
            ),
        )),
    }
}

impl ProfileEntry {
    /// The concrete backend type and the expression that yields its tuning.
    ///
    /// Mirrors the emitted type tokens in the contract's section 4, which is
    /// also what `runtime_backend.rs` emits for the table-level declaration;
    /// the two have to agree or a table and a profile that both say
    /// `nagoya(spread)` would not compare equal.
    fn backend_tokens(&self) -> (TokenStream, TokenStream) {
        match &self.flavor {
            Some(flavor) => {
                let marker = Ident::new(
                    match flavor.to_string().as_str() {
                        "spread" => "Spread",
                        "throughput" => "Throughput",
                        _ => "Locality",
                    },
                    flavor.span(),
                );
                (
                    quote! { worktable::prelude::NagoyaRt<worktable::prelude::#marker> },
                    quote! { <worktable::prelude::#marker as worktable::prelude::FlavorMarker>::tuning() },
                )
            }
            None => (
                quote! { worktable::prelude::TokioRt },
                // Tokio's pool is not this crate's to tune, so the profile
                // reports the defaults rather than inventing numbers nothing
                // reads.
                quote! { worktable::prelude::Tuning::default() },
            ),
        }
    }

    fn expand(&self) -> TokenStream {
        let name = &self.name;
        let (backend_type, tuning) = self.backend_tokens();

        let declaration = match &self.flavor {
            Some(flavor) => format!("`{}({})`", self.backend, flavor),
            None => format!("`{}`", self.backend),
        };
        let doc = format!(
            "Runtime profile `{name}`: {declaration}.\n\n\
             Generated by `runtimes!`. Named at a call site as `.runtime({name})` and at a schema \
             section as `runtime {name}:`.\n\n\
             A unit struct rather than an enum variant so that deferred parameters (a worker count, \
             a backoff) can arrive later as fields, which changes this type and `tuning` and moves no \
             call site.",
        );

        quote! {
            #[doc = #doc]
            // The profile's name is the surface syntax, at the call site and in
            // the schema alike, so it is spelled as written rather than
            // converted into a type convention nothing else here uses.
            #[allow(non_camel_case_types)]
            #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
            pub struct #name;

            impl worktable::prelude::Profile for #name {
                type Backend = #backend_type;

                fn tuning() -> worktable::prelude::Tuning {
                    #tuning
                }
            }
        }
    }
}

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let runtimes: Runtimes = syn::parse2(input)?;

    if runtimes.profiles.is_empty() {
        return Err(Error::new(
            Span::call_site(),
            "`runtimes!` with no profiles declares nothing; remove it or name a profile",
        ));
    }

    let profiles = runtimes.profiles.values().map(ProfileEntry::expand);
    Ok(quote! { #(#profiles)* })
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::expand;

    fn expanded(input: proc_macro2::TokenStream) -> String {
        expand(input).unwrap().to_string()
    }

    fn rejected(input: proc_macro2::TokenStream) -> String {
        expand(input).unwrap_err().to_string()
    }

    #[test]
    fn profiles_resolve_to_their_backend_and_tuning() {
        let out = expanded(quote! {
            tokio_max:  tokio,
            fast_local: nagoya(locality),
            wide:       nagoya(spread),
            batch:      nagoya(throughput),
        });

        assert!(out.contains("pub struct tokio_max"), "{out}");
        assert!(out.contains("type Backend = worktable :: prelude :: TokioRt"), "{out}");
        assert!(
            out.contains("type Backend = worktable :: prelude :: NagoyaRt < worktable :: prelude :: Locality >"),
            "{out}"
        );
        assert!(
            out.contains("type Backend = worktable :: prelude :: NagoyaRt < worktable :: prelude :: Spread >"),
            "{out}"
        );
        assert!(
            out.contains("type Backend = worktable :: prelude :: NagoyaRt < worktable :: prelude :: Throughput >"),
            "{out}"
        );
        assert!(
            out.contains("< worktable :: prelude :: Spread as worktable :: prelude :: FlavorMarker > :: tuning ()"),
            "{out}"
        );
        assert!(out.contains("worktable :: prelude :: Tuning :: default ()"), "{out}");
    }

    #[test]
    fn bare_nagoya_is_locality() {
        let bare = expanded(quote! { p: nagoya });
        let explicit = expanded(quote! { p: nagoya(locality) });
        assert_eq!(bare, explicit);
    }

    #[test]
    fn duplicate_profile_names_are_rejected() {
        let err = rejected(quote! {
            wide: nagoya(spread),
            wide: tokio,
        });
        assert!(err.contains("duplicate runtime profile `wide`"), "{err}");
    }

    #[test]
    fn unknown_backend_is_rejected() {
        let err = rejected(quote! { p: smol });
        assert!(err.contains("unknown runtime backend `smol`"), "{err}");
        assert!(err.contains("nagoya, tokio"), "{err}");
    }

    #[test]
    fn not_implemented_backends_are_rejected_rather_than_accepted_inert() {
        for backend in ["forte", "blocking", "bwos"] {
            let input: proc_macro2::TokenStream = format!("p: {backend}").parse().unwrap();
            let err = rejected(input);
            assert!(err.contains(backend), "{err}");
            assert!(err.contains("is not implemented"), "{err}");
            assert!(err.contains("nagoya, tokio"), "{err}");
        }
    }

    #[test]
    fn tokio_takes_no_flavor() {
        let err = rejected(quote! { p: tokio(spread) });
        assert!(err.contains("tokio has no flavors"), "{err}");
    }

    #[test]
    fn unknown_flavor_lists_the_three() {
        let err = rejected(quote! { p: nagoya(banana) });
        assert!(err.contains("unknown nagoya flavor `banana`"), "{err}");
        assert!(err.contains("locality, spread, throughput"), "{err}");
    }

    #[test]
    fn a_flavor_takes_no_parameters() {
        let err = rejected(quote! { p: nagoya(spread, 12) });
        assert!(err.contains("one flavor and nothing else"), "{err}");
    }

    #[test]
    fn an_empty_block_is_rejected() {
        let err = rejected(quote! {});
        assert!(err.contains("declares nothing"), "{err}");
    }

    #[test]
    fn a_trailing_comma_is_optional() {
        assert_eq!(expanded(quote! { p: tokio, }), expanded(quote! { p: tokio }));
    }
}
