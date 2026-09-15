use convert_case::{Case, Casing};
use indexmap::{IndexMap, IndexSet};
use proc_macro2::{Ident, Span, TokenStream};
use quote::{format_ident, quote};
use worktable_dsl::model::{Columns, Operation, Queries};

fn selector_name(operation: &Operation) -> String {
    operation
        .columns
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("_and_")
}

fn validate_collisions(operations: &IndexMap<Ident, Operation>, family: &str) -> syn::Result<()> {
    let mut generated: IndexMap<(String, String), &Ident> = IndexMap::new();
    for (name, operation) in operations {
        let key = (operation.by.to_string(), selector_name(operation));
        if let Some(previous) = generated.insert(key.clone(), name) {
            return Err(syn::Error::new(
                name.span(),
                format!(
                    "{family} queries `{previous}` and `{name}` both generate `{family}_by_{}(..., Columns::{}, ...)`",
                    key.0,
                    key.1.to_case(Case::UpperSnake)
                ),
            ));
        }
    }
    Ok(())
}

fn selector_definitions(name: &Ident, queries: &Queries) -> syn::Result<TokenStream> {
    let columns = format_ident!("{name}Columns");
    let sealed = format_ident!("__{}_mutation", name.to_string().to_case(Case::Snake));
    let mut seen: IndexMap<String, Vec<String>> = IndexMap::new();
    let mut definitions = Vec::new();
    let mut constants = Vec::new();

    for operation in queries.updates.values().chain(queries.updates_in_place.values()) {
        let field_set = operation.columns.iter().map(ToString::to_string).collect::<Vec<_>>();
        let selector = selector_name(operation);
        if let Some(previous) = seen.get(&selector) {
            if previous != &field_set {
                return Err(syn::Error::new(
                    operation.name.span(),
                    format!(
                        "declared field sets `{}` and `{}` generate the same selector name `{selector}`",
                        previous.join(", "),
                        field_set.join(", ")
                    ),
                ));
            }
            continue;
        }
        seen.insert(selector.clone(), field_set);
        let selector_pascal = selector.from_case(Case::Snake).to_case(Case::Pascal);
        let selector_type = format_ident!("{name}{selector_pascal}Selector");
        // Preserve identifier spelling: Rust's conventional constant form is
        // ASCII uppercase with the source underscores left in place. Case
        // conversion would unexpectedly turn `attr1` into `ATTR_1`.
        let constant = Ident::new(&selector.to_ascii_uppercase(), Span::mixed_site());
        definitions.push(quote! {
            #[doc(hidden)]
            #[derive(Clone, Copy, Debug, Default)]
            pub struct #selector_type;
            impl #sealed::Sealed for #selector_type {}
        });
        constants.push(quote! { pub const #constant: #selector_type = #selector_type; });
    }

    if definitions.is_empty() {
        return Ok(quote! {});
    }
    Ok(quote! {
        mod #sealed { pub trait Sealed {} }
        pub struct #columns;
        impl #columns { #(#constants)* }
        #(#definitions)*
    })
}

pub(crate) fn paged_mutation_api(
    name: &Ident,
    table: &Ident,
    columns: &Columns,
    queries: &Queries,
) -> syn::Result<TokenStream> {
    validate_collisions(&queries.updates, "update")?;
    validate_collisions(&queries.updates_in_place, "update_in_place")?;

    let selectors = selector_definitions(name, queries)?;
    let update = paged_updates(name, table, columns, &queries.updates, queries.update_runtime.is_some())?;
    let update_in_place = paged_updates_in_place(
        name,
        table,
        columns,
        &queries.updates_in_place,
        queries.update_in_place_runtime.is_some(),
    )?;
    Ok(quote! { #selectors #update #update_in_place })
}

/// The same typed selector surface for a single-writer `Vec` table.
///
/// The dispatch is monomorphized and sealed exactly like the paged surface;
/// only the receiver and result reflect the synchronous storage shape.
pub(crate) fn vec_mutation_api(
    name: &Ident,
    table: &Ident,
    columns: &Columns,
    queries: &Queries,
) -> syn::Result<TokenStream> {
    validate_collisions(&queries.updates, "update")?;
    validate_collisions(&queries.updates_in_place, "update_in_place")?;
    let selectors = selector_definitions(name, queries)?;
    let sealed = format_ident!("__{}_mutation", name.to_string().to_case(Case::Snake));
    let update = vec_updates(name, table, columns, &queries.updates, &sealed)?;
    let update_in_place = vec_updates_in_place(name, table, columns, &queries.updates_in_place, &sealed)?;
    Ok(quote! { #selectors #update #update_in_place })
}

fn vec_updates(
    name: &Ident,
    table: &Ident,
    columns: &Columns,
    operations: &IndexMap<Ident, Operation>,
    sealed: &Ident,
) -> syn::Result<TokenStream> {
    let mut by_fields = IndexSet::new();
    let mut implementations = Vec::new();
    for (query_name, operation) in operations {
        by_fields.insert(operation.by.clone());
        let by = &operation.by;
        let by_type = columns
            .columns_map
            .get(by)
            .ok_or_else(|| syn::Error::new(by.span(), format!("no column `{by}`")))?;
        let by_pascal = by.to_string().from_case(Case::Snake).to_case(Case::Pascal);
        let trait_ident = format_ident!("{name}VecUpdateBy{by_pascal}");
        let selector_pascal = selector_name(operation).from_case(Case::Snake).to_case(Case::Pascal);
        let selector = format_ident!("{name}{selector_pascal}Selector");
        let query = format_ident!("{query_name}Query");
        let hidden = format_ident!(
            "__wt_update_{}",
            query_name.to_string().from_case(Case::Pascal).to_case(Case::Snake)
        );
        let (value_type, value) = if operation.columns.len() == 1 {
            let field = &operation.columns[0];
            let ty = columns
                .columns_map
                .get(field)
                .ok_or_else(|| syn::Error::new(field.span(), format!("no column `{field}`")))?;
            (quote! { #ty }, quote! { #query { #field: value } })
        } else {
            (quote! { #query }, quote! { value })
        };
        implementations.push(quote! {
            impl #trait_ident<#value_type> for #selector {
                type Key = #by_type;
                fn apply(self, table: &mut #table, key: &Self::Key, value: #value_type) -> usize {
                    table.#hidden(#value, key)
                }
            }
        });
    }
    let mut traits_and_methods = Vec::new();
    for by in by_fields {
        let by_type = columns
            .columns_map
            .get(&by)
            .ok_or_else(|| syn::Error::new(by.span(), format!("no column `{by}`")))?;
        let by_pascal = by.to_string().from_case(Case::Snake).to_case(Case::Pascal);
        let trait_ident = format_ident!("{name}VecUpdateBy{by_pascal}");
        let method = format_ident!("update_by_{by}");
        traits_and_methods.push(quote! {
            #[doc(hidden)]
            #[allow(private_bounds)]
            pub trait #trait_ident<V>: #sealed::Sealed {
                type Key;
                fn apply(self, table: &mut #table, key: &Self::Key, value: V) -> usize;
            }
            impl #table {
                pub fn #method<S, V>(&mut self, key: #by_type, selector: S, value: V) -> usize
                where S: #trait_ident<V, Key = #by_type>
                {
                    selector.apply(self, &key, value)
                }
            }
        });
    }
    Ok(quote! { #(#traits_and_methods)* #(#implementations)* })
}

fn vec_updates_in_place(
    name: &Ident,
    table: &Ident,
    columns: &Columns,
    operations: &IndexMap<Ident, Operation>,
    sealed: &Ident,
) -> syn::Result<TokenStream> {
    let mut by_fields = IndexSet::new();
    let mut implementations = Vec::new();
    for (query_name, operation) in operations {
        by_fields.insert(operation.by.clone());
        let by = &operation.by;
        let by_type = columns
            .columns_map
            .get(by)
            .ok_or_else(|| syn::Error::new(by.span(), format!("no column `{by}`")))?;
        let by_pascal = by.to_string().from_case(Case::Snake).to_case(Case::Pascal);
        let trait_ident = format_ident!("{name}VecUpdateInPlaceBy{by_pascal}");
        let selector_pascal = selector_name(operation).from_case(Case::Snake).to_case(Case::Pascal);
        let selector = format_ident!("{name}{selector_pascal}Selector");
        let fields = &operation.columns;
        let field_types = fields
            .iter()
            .map(|field| {
                columns
                    .columns_map
                    .get(field)
                    .ok_or_else(|| syn::Error::new(field.span(), format!("no column `{field}`")))
            })
            .collect::<syn::Result<Vec<_>>>()?;
        let closure_arg = if field_types.len() == 1 {
            let ty = field_types[0];
            quote! { &mut #ty }
        } else {
            quote! { ( #(&mut #field_types),* ) }
        };
        let hidden = format_ident!(
            "__wt_update_in_place_{}",
            query_name.to_string().from_case(Case::Pascal).to_case(Case::Snake)
        );
        implementations.push(quote! {
            impl<F> #trait_ident<F> for #selector where F: FnMut(#closure_arg) {
                type Key = #by_type;
                fn apply(self, table: &mut #table, key: &Self::Key, edit: F) -> usize {
                    table.#hidden(edit, key)
                }
            }
        });
    }
    let mut traits_and_methods = Vec::new();
    for by in by_fields {
        let by_type = columns
            .columns_map
            .get(&by)
            .ok_or_else(|| syn::Error::new(by.span(), format!("no column `{by}`")))?;
        let by_pascal = by.to_string().from_case(Case::Snake).to_case(Case::Pascal);
        let trait_ident = format_ident!("{name}VecUpdateInPlaceBy{by_pascal}");
        let method = format_ident!("update_in_place_by_{by}");
        traits_and_methods.push(quote! {
            #[doc(hidden)]
            #[allow(private_bounds)]
            pub trait #trait_ident<F>: #sealed::Sealed {
                type Key;
                fn apply(self, table: &mut #table, key: &Self::Key, edit: F) -> usize;
            }
            impl #table {
                pub fn #method<S, F>(&mut self, key: #by_type, selector: S, edit: F) -> usize
                where S: #trait_ident<F, Key = #by_type>
                {
                    selector.apply(self, &key, edit)
                }
            }
        });
    }
    Ok(quote! { #(#traits_and_methods)* #(#implementations)* })
}

fn paged_updates(
    name: &Ident,
    table: &Ident,
    columns: &Columns,
    operations: &IndexMap<Ident, Operation>,
    scheduled: bool,
) -> syn::Result<TokenStream> {
    let sealed = format_ident!("__{}_mutation", name.to_string().to_case(Case::Snake));
    let mut by_fields = IndexSet::new();
    let mut implementations = Vec::new();

    for (query_name, operation) in operations {
        by_fields.insert(operation.by.clone());
        let by = &operation.by;
        let by_type = columns
            .columns_map
            .get(by)
            .ok_or_else(|| syn::Error::new(by.span(), format!("no column `{by}`")))?;
        let key_type = if columns.primary_keys.contains(by) && !columns.indexes.values().any(|index| index.field == *by)
        {
            let primary_key = format_ident!("{name}PrimaryKey");
            quote! { #primary_key }
        } else {
            quote! { #by_type }
        };
        let by_pascal = by.to_string().from_case(Case::Snake).to_case(Case::Pascal);
        let trait_ident = format_ident!("{name}UpdateBy{by_pascal}");
        let selector_pascal = selector_name(operation).from_case(Case::Snake).to_case(Case::Pascal);
        let selector = format_ident!("{name}{selector_pascal}Selector");
        let query = format_ident!("{query_name}Query");
        let hidden = format_ident!(
            "__wt_update_{}",
            query_name.to_string().from_case(Case::Pascal).to_case(Case::Snake)
        );
        let (value_type, value) = if operation.columns.len() == 1 {
            let field = &operation.columns[0];
            let ty = columns
                .columns_map
                .get(field)
                .ok_or_else(|| syn::Error::new(field.span(), format!("no column `{field}`")))?;
            (quote! { #ty }, quote! { #query { #field: value } })
        } else {
            (quote! { #query }, quote! { value })
        };
        let table_ref = if scheduled {
            quote! { &worktable::prelude::Arc<#table> }
        } else {
            quote! { &#table }
        };
        implementations.push(quote! {
            impl #trait_ident<#value_type> for #selector {
                type Key = #key_type;
                async fn apply(self, table: #table_ref, key: Self::Key, value: #value_type)
                    -> core::result::Result<(), WorkTableError>
                {
                    table.#hidden(#value, key).await
                }
            }
        });
    }

    let mut traits_and_methods = Vec::new();
    for by in by_fields {
        let by_type = columns
            .columns_map
            .get(&by)
            .ok_or_else(|| syn::Error::new(by.span(), format!("no column `{by}`")))?;
        let key_type = if columns.primary_keys.contains(&by) && !columns.indexes.values().any(|index| index.field == by)
        {
            let primary_key = format_ident!("{name}PrimaryKey");
            quote! { #primary_key }
        } else {
            quote! { #by_type }
        };
        let by_pascal = by.to_string().from_case(Case::Snake).to_case(Case::Pascal);
        let trait_ident = format_ident!("{name}UpdateBy{by_pascal}");
        let method = format_ident!("update_by_{by}");
        let table_ref = if scheduled {
            quote! { &worktable::prelude::Arc<#table> }
        } else {
            quote! { &#table }
        };
        let receiver = if scheduled {
            quote! { self: &worktable::prelude::Arc<Self> }
        } else {
            quote! { &self }
        };
        let method_impl =
            if columns.primary_keys.contains(&by) && !columns.indexes.values().any(|index| index.field == by) {
                quote! {
                    impl #table {
                        pub async fn #method<S, V, K>(#receiver, key: K, selector: S, value: V)
                            -> core::result::Result<(), WorkTableError>
                        where
                            S: #trait_ident<V, Key = #key_type>,
                            #key_type: From<K>,
                        {
                            selector.apply(self, key.into(), value).await
                        }
                    }
                }
            } else {
                quote! {
                    impl #table {
                        pub async fn #method<S, V>(#receiver, key: #by_type, selector: S, value: V)
                            -> core::result::Result<(), WorkTableError>
                        where S: #trait_ident<V, Key = #key_type>
                        {
                            selector.apply(self, key, value).await
                        }
                    }
                }
            };
        traits_and_methods.push(quote! {
            #[doc(hidden)]
            #[allow(private_bounds)]
            #[allow(async_fn_in_trait)]
            pub trait #trait_ident<V>: #sealed::Sealed {
                type Key;
                async fn apply(self, table: #table_ref, key: Self::Key, value: V)
                    -> core::result::Result<(), WorkTableError>;
            }

            #method_impl
        });
    }
    Ok(quote! { #(#traits_and_methods)* #(#implementations)* })
}

fn paged_updates_in_place(
    name: &Ident,
    table: &Ident,
    columns: &Columns,
    operations: &IndexMap<Ident, Operation>,
    scheduled: bool,
) -> syn::Result<TokenStream> {
    let sealed = format_ident!("__{}_mutation", name.to_string().to_case(Case::Snake));
    let mut by_fields = IndexSet::new();
    let mut implementations = Vec::new();

    for (query_name, operation) in operations {
        by_fields.insert(operation.by.clone());
        let by = &operation.by;
        let by_type = columns
            .columns_map
            .get(by)
            .ok_or_else(|| syn::Error::new(by.span(), format!("no column `{by}`")))?;
        let key_type = if columns.primary_keys.contains(by) && !columns.indexes.values().any(|index| index.field == *by)
        {
            let primary_key = format_ident!("{name}PrimaryKey");
            quote! { #primary_key }
        } else {
            quote! { #by_type }
        };
        let by_pascal = by.to_string().from_case(Case::Snake).to_case(Case::Pascal);
        let trait_ident = format_ident!("{name}UpdateInPlaceBy{by_pascal}");
        let selector_pascal = selector_name(operation).from_case(Case::Snake).to_case(Case::Pascal);
        let selector = format_ident!("{name}{selector_pascal}Selector");
        let field_types = operation
            .columns
            .iter()
            .map(|field| {
                columns
                    .columns_map
                    .get(field)
                    .ok_or_else(|| syn::Error::new(field.span(), format!("no column `{field}`")))
            })
            .collect::<syn::Result<Vec<_>>>()?;
        let closure_arg = if field_types.len() == 1 {
            let ty = field_types[0];
            quote! { &mut <#ty as worktable::prelude::rkyv::Archive>::Archived }
        } else {
            let archived = field_types.iter().map(|ty| {
                quote! { &mut <#ty as worktable::prelude::rkyv::Archive>::Archived }
            });
            quote! { ( #(#archived),* ) }
        };
        let hidden = format_ident!(
            "__wt_update_in_place_{}",
            query_name.to_string().from_case(Case::Pascal).to_case(Case::Snake)
        );
        let table_ref = if scheduled {
            quote! { &worktable::prelude::Arc<#table> }
        } else {
            quote! { &#table }
        };
        let send = if scheduled {
            quote! { + Send + 'static }
        } else {
            quote! {}
        };
        implementations.push(quote! {
            impl<F> #trait_ident<F> for #selector
            where F: FnMut(#closure_arg) #send
            {
                type Key = #key_type;
                async fn apply(self, table: #table_ref, key: Self::Key, edit: F)
                    -> worktable::prelude::eyre::Result<()>
                {
                    table.#hidden(edit, key).await
                }
            }
        });
    }

    let mut traits_and_methods = Vec::new();
    for by in by_fields {
        let by_type = columns
            .columns_map
            .get(&by)
            .ok_or_else(|| syn::Error::new(by.span(), format!("no column `{by}`")))?;
        let key_type = if columns.primary_keys.contains(&by) && !columns.indexes.values().any(|index| index.field == by)
        {
            let primary_key = format_ident!("{name}PrimaryKey");
            quote! { #primary_key }
        } else {
            quote! { #by_type }
        };
        let by_pascal = by.to_string().from_case(Case::Snake).to_case(Case::Pascal);
        let trait_ident = format_ident!("{name}UpdateInPlaceBy{by_pascal}");
        let method = format_ident!("update_in_place_by_{by}");
        let table_ref = if scheduled {
            quote! { &worktable::prelude::Arc<#table> }
        } else {
            quote! { &#table }
        };
        let receiver = if scheduled {
            quote! { self: &worktable::prelude::Arc<Self> }
        } else {
            quote! { &self }
        };
        let method_impl =
            if columns.primary_keys.contains(&by) && !columns.indexes.values().any(|index| index.field == by) {
                quote! {
                    impl #table {
                        pub async fn #method<S, F, K>(#receiver, key: K, selector: S, edit: F)
                            -> worktable::prelude::eyre::Result<()>
                        where
                            S: #trait_ident<F, Key = #key_type>,
                            #key_type: From<K>,
                        {
                            selector.apply(self, key.into(), edit).await
                        }
                    }
                }
            } else {
                quote! {
                    impl #table {
                        pub async fn #method<S, F>(#receiver, key: #by_type, selector: S, edit: F)
                            -> worktable::prelude::eyre::Result<()>
                        where S: #trait_ident<F, Key = #key_type>
                        {
                            selector.apply(self, key, edit).await
                        }
                    }
                }
            };
        traits_and_methods.push(quote! {
            #[doc(hidden)]
            #[allow(private_bounds)]
            #[allow(async_fn_in_trait)]
            pub trait #trait_ident<F>: #sealed::Sealed {
                type Key;
                async fn apply(self, table: #table_ref, key: Self::Key, edit: F)
                    -> worktable::prelude::eyre::Result<()>;
            }

            #method_impl
        });
    }
    Ok(quote! { #(#traits_and_methods)* #(#implementations)* })
}
