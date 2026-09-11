//! Wrap explicitly scheduled mutations in owned, cancellable tasks.
use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use syn::{FnArg, GenericParam, ImplItem, Pat, parse_quote};

pub(crate) fn wrap(methods: TokenStream, profile: Option<&Ident>, row: &Ident) -> syn::Result<TokenStream> {
    let Some(profile) = profile else { return Ok(methods) };
    if !cfg!(feature = "std") {
        return Err(syn::Error::new(
            profile.span(),
            "query runtime profiles require WorkTable's std feature",
        ));
    }
    let parsed: syn::ItemImpl = syn::parse2(quote! { impl Placeholder { #methods } })?;
    let mut out = TokenStream::new();
    for item in parsed.items {
        let ImplItem::Fn(mut inline) = item else {
            out.extend(quote! { #item });
            continue;
        };
        let mut scheduled = inline.clone();
        let inline_name = format_ident!("__wt_inline_{}", inline.sig.ident);
        inline.sig.ident = inline_name.clone();
        inline.vis = syn::Visibility::Inherited;
        inline.attrs.push(parse_quote!(#[allow(dead_code)]));
        let mut arguments = Vec::new();
        for argument in &mut scheduled.sig.inputs {
            match argument {
                FnArg::Receiver(receiver) => *receiver = parse_quote!(self: &worktable::prelude::Arc<Self>),
                FnArg::Typed(argument) => {
                    let Pat::Ident(pattern) = &mut *argument.pat else {
                        return Err(syn::Error::new_spanned(argument, "query argument must be named"));
                    };
                    pattern.mutability = None;
                    arguments.push(pattern.ident.clone());
                }
            }
        }
        for parameter in &mut scheduled.sig.generics.params {
            if let GenericParam::Type(parameter) = parameter {
                parameter.bounds.push(parse_quote!(Send));
                parameter.bounds.push(parse_quote!('static));
            }
        }
        scheduled.block = parse_quote!({
            fn check_profile<P: worktable::runtime::Profile>()
            where P::Backend: worktable::runtime::RuntimeCompatibleWith<<#row as worktable::runtime::TableRuntime>::Backend> {}
            check_profile::<#profile>();
            let table = worktable::prelude::Arc::clone(self);
            worktable::runtime::run_profile::<#profile, _>(async move {
                table.#inline_name(#(#arguments),*).await
            }).await?
        });
        out.extend(quote! { #inline #scheduled });
    }
    Ok(out)
}
