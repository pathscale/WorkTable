use proc_macro2::TokenStream;

use crate::common::Parser;
use crate::common::name_generator::WorktableNameGenerator;

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    // Keep the tokens. The declaration is read a second time at the end, as
    // data, so the generated table can carry its own schema. It happens at the
    // end rather than here so that this function's diagnostics are the ones a
    // bad declaration produces: both parses reject the same inputs, but only
    // one of them knows to say that a separate `attributes` section is not
    // part of the 1.0 grammar.
    let declaration = input.clone();

    let mut parser = Parser::new(input);
    let mut columns = None;
    let mut queries = None;
    let mut indexes = None;
    let mut config = None;

    let name = parser.parse_name()?;
    let version = parser.parse_version()?.unwrap_or(1);
    let persistence = parser.parse_persist()?;
    let partition_by = parser.parse_partition_by()?;
    while let Some(ident) = parser.peek_next() {
        match ident.to_string().as_str() {
            "columns" => {
                let res = parser.parse_columns()?;
                columns = Some(res)
            }
            "indexes" => {
                let res = parser.parse_indexes()?;
                indexes = Some(res);
            }
            "queries" => {
                let res = parser.parse_queries()?;
                queries = Some(res)
            }
            "config" => {
                let res = parser.parse_configs()?;
                config = Some(res)
            }
            "version" => {
                return Err(syn::Error::new(
                    ident.span(),
                    "version must be specified before columns/indexes/queries/config",
                ));
            }
            // Positional declarations that landed after the blocks began, or in
            // the wrong relative order, would otherwise die as a bare
            // "Unexpected identifier" and cost the next person a bisect.
            "persist" => {
                return Err(syn::Error::new(
                    ident.span(),
                    "`persist` is positional and must come before `partition_by` and the blocks; the required order is: name, version, persist, partition_by, then columns/indexes/queries/config",
                ));
            }
            "partition_by" => {
                return Err(syn::Error::new(
                    ident.span(),
                    "`partition_by` is positional and must come after `persist` and before the blocks; the required order is: name, version, persist, partition_by, then columns/indexes/queries/config",
                ));
            }
            "attributes" => {
                return Err(syn::Error::new(
                    ident.span(),
                    "a separate `attributes` section is not part of the 1.0 grammar; keep `primary_key`, `autoincrement`, `custom`, `optional`, and `using` inline on their column or index declarations",
                ));
            }
            other => {
                return Err(syn::Error::new(
                    ident.span(),
                    format!("Unexpected token `{other}`; expected one of `columns`, `indexes`, `queries`, `config`"),
                ));
            }
        }
    }

    let mut columns = columns.expect("defined");
    if let Some(i) = indexes {
        columns.indexes = i
    }

    worktable_dsl::validate::validate_index_backends(&columns, persistence)?;
    worktable_dsl::validate::validate_page_size(config.as_ref(), persistence)?;
    if let Some(q) = &queries {
        worktable_dsl::validate::validate_in_place_queries(&columns, q)?;
    }

    let mut generated = if persistence.is_persisted() {
        crate::generators::persist::expand(name.clone(), columns, queries, config, version)?
    } else {
        crate::generators::in_memory::expand_from_parsed(name.clone(), columns, queries, config)?
    };

    if let Some(key) = partition_by {
        generated.extend(crate::generators::partitions::expand(&name, &key, persistence));
    }

    generated.extend(gen_schema_const(&worktable_dsl::Schema::from_tokens(declaration)?));

    Ok(generated)
}

/// Bake the declaration into the generated code, as the text it was written in.
///
/// The point is that a compiled binary should be able to say what schema it was
/// built against, without the source. A migration planner needs it as the
/// "declared" side of a comparison against what is on disk; a designer needs it
/// to draw a diagram of an application it did not build.
///
/// The stored form is the DSL text rather than a serialised structure. It needs
/// no format decision, no serde in the dependency graph of every user's build,
/// and it is legible in a hex dump; `worktable_dsl` reads it back with the same
/// parser that read the original, and `dsl/tests/round_trip.rs` holds that
/// property against all 116 declarations in this repository.
///
/// `allow(dead_code)` because a `worktable!` inside a function body puts this
/// const inside that body, where nothing refers to it and `-D warnings` would
/// otherwise fail a user's build over a const they never asked for.
fn gen_schema_const(schema: &worktable_dsl::Schema) -> TokenStream {
    let ident = WorktableNameGenerator::from_table_name(schema.name.clone()).get_schema_const_ident();
    let text = schema.to_dsl();
    let doc = format!(
        "The `worktable!` declaration `{}` was generated from, as text. Read it with `worktable_dsl::Schema::parse`.",
        schema.name
    );

    quote::quote! {
        #[doc = #doc]
        #[allow(dead_code)]
        pub const #ident: &str = #text;
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::expand;

    #[test]
    fn separate_attributes_section_has_an_actionable_1_0_diagnostic() {
        let error = expand(quote! {
            name: AttributesSection,
            columns: {
                id: u64 primary_key,
            },
            attributes: {
                id: primary_key,
            },
        })
        .unwrap_err();

        assert!(error.to_string().contains("not part of the 1.0 grammar"));
        assert!(error.to_string().contains("keep `primary_key`"));
    }

    fn assert_composite_primary_key_field_order(output: proc_macro2::TokenStream) {
        let output = output.to_string();
        let get_primary_key = output
            .split("fn get_primary_key")
            .nth(1)
            .expect("generated TableRow implementation");
        let tenant = get_primary_key
            .find("self . tenant_id . clone")
            .expect("first primary-key field");
        let record = get_primary_key
            .find("self . record_id . clone")
            .expect("second primary-key field");

        assert!(tenant < record, "composite primary-key declaration order changed");
    }

    #[test]
    fn composite_primary_key_codegen_preserves_declaration_order() {
        for persist in [true, false] {
            let output = expand(quote! {
                name: CompositePrimaryKeyOrder,
                persist: #persist,
                columns: {
                    tenant_id: u64 primary_key,
                    record_id: u64 primary_key,
                    value: i64,
                },
            })
            .unwrap();

            assert_composite_primary_key_field_order(output);
        }
    }

    #[test]
    fn absent_using_keeps_worktables_index_default() {
        let output = expand(quote! {
            name: DefaultBackend,
            persist: true,
            columns: {
                id: u64 primary_key autoincrement,
                value: u64,
            },
            indexes: {
                value_idx: value unique,
            },
        })
        .unwrap()
        .to_string();

        if cfg!(feature = "logical-index-persistence") {
            assert!(output.contains("PersistentWtiIndex"));
        } else {
            assert!(output.contains("IndexMap"));
            assert!(!output.contains("PersistentWtiIndex"));
        }
    }

    #[test]
    fn fixed_width_update_on_unsized_table_uses_archived_swap() {
        let output = expand(quote! {
            name: MixedWidthUpdate,
            persist: false,
            columns: {
                id: u64 primary_key,
                payload: String,
                balance: f64,
            },
            queries: {
                update: {
                    Balance(balance) by id,
                }
            }
        })
        .unwrap()
        .to_string();

        let update = output
            .split("pub async fn update_balance")
            .nth(1)
            .expect("generated balance update");
        assert!(
            update.contains("data . with_mut_ref"),
            "fixed-width unindexed field must update archived storage in place"
        );
        assert!(
            !update.contains("self . reinsert"),
            "an unrelated String column must not force a fixed-width update through reinsert"
        );
        assert!(
            !update.contains("Uuid :: now_v7"),
            "non-persistent updates must not generate an unused operation id"
        );
    }

    #[cfg(feature = "logical-index-persistence")]
    #[test]
    fn logical_persistence_wraps_only_default_wti_backends() {
        let output = expand(quote! {
            name: LogicalDefaultBackend,
            persist: true,
            columns: {
                id: u64 primary_key autoincrement,
                wti_value: u64,
                congee_value: u64,
                arctic_value: u64,
            },
            indexes: {
                wti_idx: wti_value unique,
                congee_idx: congee_value unique using congee,
                arctic_idx: arctic_value unique using arctic,
            },
        })
        .unwrap()
        .to_string();

        assert!(output.contains("PersistentWtiIndex"));
        assert!(output.contains("PersistentCongeeIndex"));
        assert!(output.contains("PersistentArcticIndex"));
    }

    #[test]
    fn explicit_indexset_is_persistence_compatible() {
        let output = expand(quote! {
            name: ExplicitIndexset,
            persist: true,
            columns: {
                id: u64 primary_key autoincrement using indexset,
                value: u64,
            },
            indexes: {
                value_idx: value unique using indexset,
            },
        });

        assert!(output.is_ok());
    }

    #[test]
    fn explicit_worktables_index_is_persistence_compatible() {
        let output = expand(quote! {
            name: ExplicitWorktablesIndex,
            persist: true,
            columns: {
                id: u64 primary_key autoincrement using worktables_index,
                value: u64,
            },
            indexes: {
                value_idx: value unique using worktables_index,
            },
        });

        assert!(output.is_ok());
    }

    #[test]
    fn art_backend_requires_explicit_persistence_choice() {
        let error = expand(quote! {
            name: MissingAcknowledgement,
            columns: {
                id: u64 primary_key using congee,
            },
        })
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("explicit `persist: true` or `persist: false`")
        );
    }

    #[test]
    fn art_backend_accepts_persistence() {
        let output = expand(quote! {
            name: PersistentArctic,
            persist: true,
            columns: {
                id: u64 primary_key using arctic,
                value: u64,
            },
            indexes: {
                value_idx: value unique using congee,
            },
        });

        assert!(output.is_ok());
    }

    #[test]
    fn memory_backend_accepts_explicit_false() {
        let output = expand(quote! {
            name: ExplicitMemory,
            persist: false,
            columns: {
                id: u64 primary_key using congee,
            },
        });

        assert!(output.is_ok());
    }

    #[test]
    fn memory_tables_accept_non_unique_arctic_indexes() {
        for key_type in ["u16", "u32", "u64", "u128"] {
            let key_type: proc_macro2::TokenStream = key_type.parse().unwrap();
            let output = expand(quote! {
                name: NonUniqueArctic,
                persist: false,
                columns: {
                    id: u64 primary_key,
                    value: #key_type,
                },
                indexes: {
                    value_idx: value using arctic,
                },
            });

            assert!(output.is_ok(), "{:?}", output.err());
            assert!(output.unwrap().to_string().contains("ArcticMultiIndex"));
        }
    }

    #[test]
    fn persisted_tables_accept_non_unique_arctic_indexes() {
        let output = expand(quote! {
            name: PersistedNonUniqueArctic,
            persist: true,
            columns: {
                id: u64 primary_key,
                value: u64,
            },
            indexes: {
                value_idx: value using arctic,
            },
        });

        assert!(output.is_ok(), "{:?}", output.err());
        assert!(output.unwrap().to_string().contains("PersistentArcticMultiIndex"));
    }

    #[test]
    fn non_unique_arctic_rejects_unsupported_key_types() {
        let error = expand(quote! {
            name: StringNonUniqueArctic,
            persist: false,
            columns: {
                id: u64 primary_key,
                name: String,
            },
            indexes: {
                name_idx: name using arctic,
            },
        })
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("supported types: u16, u32, u64, u128, i16, i32, i64, i128")
        );
    }

    #[test]
    fn non_unique_indexes_reject_other_explicit_backends() {
        for backend in ["congee", "indexset"] {
            let backend: proc_macro2::TokenStream = backend.parse().unwrap();
            let error = expand(quote! {
                name: NonUniqueOther,
                persist: false,
                columns: {
                    id: u64 primary_key,
                    value: u64,
                },
                indexes: {
                    value_idx: value using #backend,
                },
            })
            .unwrap_err();

            assert!(
                error
                    .to_string()
                    .contains("non-unique indexes currently require `worktables_index` or `arctic`")
            );
        }
    }

    #[test]
    fn congee_rejects_non_machine_word_secondary_keys() {
        let error = expand(quote! {
            name: StringCongee,
            persist: false,
            columns: {
                id: u64 primary_key,
                name: String,
            },
            indexes: {
                name_idx: name unique using congee,
            },
        })
        .unwrap_err();

        assert!(error.to_string().contains("does not support key type `String`"));
    }

    #[test]
    fn arctic_rejects_unsupported_secondary_keys() {
        let error = expand(quote! {
            name: ByteArctic,
            persist: false,
            columns: {
                id: u64 primary_key,
                value: u8,
            },
            indexes: {
                value_idx: value unique using arctic,
            },
        })
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("supported types: u16, u32, u64, u128, i16, i32, i64, i128")
        );
    }

    #[test]
    fn persisted_tables_reject_non_default_page_size() {
        let error = expand(quote! {
            name: PersistedSmallPages,
            persist: true,
            columns: {
                id: u64 primary_key,
            },
            config: {
                page_size: 8192,
            }
        })
        .unwrap_err();

        assert!(
            error.to_string().contains("cannot be combined with `persist: true`"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn persisted_tables_accept_the_default_page_size() {
        expand(quote! {
            name: PersistedDefaultPages,
            persist: true,
            columns: {
                id: u64 primary_key,
            },
            config: {
                page_size: 16384,
            }
        })
        .unwrap();
    }

    #[test]
    fn in_memory_tables_keep_custom_page_sizes() {
        expand(quote! {
            name: InMemorySmallPages,
            persist: false,
            columns: {
                id: u64 primary_key,
            },
            config: {
                page_size: 1024,
            }
        })
        .unwrap();
    }

    #[test]
    fn congee_rejects_unsupported_primary_keys() {
        let error = expand(quote! {
            name: StringPrimaryCongee,
            persist: false,
            columns: {
                id: String primary_key using congee,
            },
        })
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("requires a directly named primitive primary-key type; found `String`")
        );
    }
}

#[cfg(test)]
mod position_tests {
    use quote::quote;

    use super::expand;

    #[test]
    fn persisted_partitioned_table_expands() {
        // Regression: the emitted `partition_or_create` carried a
        // `where Table: Default` bound on a concrete type, which rustc rejects
        // as a trivial bound, so `persist: true` + `partition_by` failed to
        // expand at all. The persisted facade now omits that method.
        let expanded = expand(quote! {
            name: SymbolPosting,
            persist: true,
            partition_by: generation: u32,
            columns: { id: u64 primary_key autoincrement, posting_hash: u64, records_blob: String },
            indexes: { posting_idx: posting_hash unique }
        })
        .expect("persist + partition_by must expand")
        .to_string();
        assert!(
            !expanded.contains("partition_or_create"),
            "a persisted facade must not emit the Default-bound constructor"
        );
        assert!(
            expanded.contains("partition_or_insert_with"),
            "the closure-based constructor is the persisted entry point"
        );
    }

    #[test]
    fn in_memory_partitioned_table_keeps_partition_or_create() {
        let expanded = expand(quote! {
            name: Price,
            partition_by: symbol_id: u16,
            columns: { exchange_id: u8 primary_key, bid: f64 }
        })
        .expect("in-memory partitioned table must expand")
        .to_string();
        assert!(expanded.contains("partition_or_create"));
    }

    #[test]
    fn partition_by_before_persist_names_the_required_order() {
        let error = expand(quote! {
            name: Wrong,
            partition_by: generation: u32,
            persist: true,
            columns: { id: u64 primary_key, v: u64 }
        })
        .expect_err("wrong order must be an error")
        .to_string();
        assert!(
            error.contains("name, version, persist, partition_by"),
            "the error must name the required order, got: {error}"
        );
    }

    #[test]
    fn partition_by_after_the_blocks_names_the_required_order() {
        let error = expand(quote! {
            name: Wrong,
            columns: { id: u64 primary_key, v: u64 },
            partition_by: generation: u32,
        })
        .expect_err("late partition_by must be an error")
        .to_string();
        assert!(
            error.contains("name, version, persist, partition_by"),
            "the error must name the required order, got: {error}"
        );
    }
}

/// What a designer needs from the schema IR: a declaration that has been read
/// into [`worktable_dsl::Schema`] and written back out is a declaration this
/// macro accepts.
///
/// `worktable_dsl` can test its own round trip, which shows nothing was lost
/// between its parser and its emitter. It cannot show that the text it emits
/// is a declaration *this* macro accepts, because it cannot call this macro:
/// that check has to live on the near side of the proc-macro boundary.
///
/// The stronger claim — that the emitted declaration generates the *same code*
/// — is not asserted here, and cannot be until the generator is deterministic.
/// See `the_same_declaration_expands_the_same_way_twice` below, which is
/// ignored because it currently fails on unmodified code.
#[cfg(test)]
mod emitted_declarations {
    use quote::quote;
    use worktable_dsl::Schema;

    use super::expand;

    fn survives_the_round_trip(declaration: proc_macro2::TokenStream) {
        expand(declaration.clone()).expect("the original expands");

        let schema = Schema::from_tokens(declaration).expect("the IR reads it");
        let emitted = schema.to_dsl();
        let reparsed: proc_macro2::TokenStream = syn::parse_str(&emitted)
            .unwrap_or_else(|error| panic!("emitted text does not tokenise: {error}\n{emitted}"));

        assert_eq!(
            Schema::from_tokens(reparsed.clone()).expect("the emitted text reads back"),
            schema,
            "the emitted declaration describes a different schema\n{emitted}"
        );
        expand(reparsed).unwrap_or_else(|error| panic!("the emitted declaration does not expand: {error}\n{emitted}"));
    }

    #[test]
    fn a_minimal_declaration() {
        survives_the_round_trip(quote! {
            name: Minimal,
            columns: { id: u64 primary_key },
        });
    }

    #[test]
    fn a_persisted_declaration_with_indexes_and_queries() {
        survives_the_round_trip(quote! {
            name: Account,
            version: 3,
            persist: true,
            columns: {
                id: u64 primary_key autoincrement,
                email: String,
                tenant: u64,
                nickname: String optional,
                balance: f64,
            },
            indexes: {
                email_idx: email unique,
                tenant_idx: tenant,
            },
            queries: {
                update: {
                    Nickname(nickname) by id,
                    Email(email) by tenant,
                }
                delete: {
                    ById() by id,
                }
                in_place: {
                    Balance(balance) by id,
                }
            }
        });
    }

    #[test]
    fn a_partitioned_declaration() {
        survives_the_round_trip(quote! {
            name: Price,
            partition_by: symbol_id: u16,
            columns: {
                exchange_id: u8 primary_key,
                bid: f64,
            },
        });
    }

    #[test]
    fn a_composite_key_keeps_its_column_order() {
        // The order of a composite key decides the field order of the
        // generated `get_primary_key`, and so the layout of the key type. An
        // emitter that wrote the columns back in a `HashMap`'s order would
        // change it.
        survives_the_round_trip(quote! {
            name: CompositeKey,
            persist: true,
            columns: {
                tenant_id: u64 primary_key,
                record_id: u64 primary_key,
                value: i64,
            },
        });
    }

    #[test]
    fn an_explicit_backend_and_a_custom_page_size() {
        survives_the_round_trip(quote! {
            name: Tuned,
            persist: false,
            columns: {
                id: u64 primary_key using congee,
                value: u64,
            },
            indexes: {
                value_idx: value unique using arctic,
            },
            config: {
                page_size: 1024,
                row_derives: Clone, Debug,
            }
        });
    }
}

#[cfg(test)]
mod generator_determinism {
    use quote::quote;

    use super::expand;

    /// Expanding one declaration twice must produce one program. It does not.
    ///
    /// `Columns::columns_map` is a `std::collections::HashMap`, and several
    /// generators iterate it directly to emit an ordered construct: the
    /// `RowFields` enum and the `AvaiableTypes` enum among them. `RandomState`
    /// seeds each map instance differently, so two expansions of the same
    /// declaration in the same process emit those variants in different
    /// orders, and two compilations of the same source can too.
    ///
    /// This is ignored rather than deleted because it is the evidence. It is
    /// ignored rather than failing because the fix — ordering `columns_map`,
    /// which `field_positions` already records the order for — changes the
    /// generated code of every table and is a change to review on its own,
    /// not a side effect of adding an emitter.
    ///
    /// Run it with `cargo test -p worktable_codegen -- --ignored`.
    #[test]
    #[ignore = "records a known generator bug: columns_map is a HashMap, so expansion is not deterministic"]
    fn the_same_declaration_expands_the_same_way_twice() {
        let declaration = quote! {
            name: Twice,
            persist: true,
            columns: {
                id: u64 primary_key autoincrement,
                email: String,
                tenant: u64,
                balance: f64,
            },
        };

        let first = expand(declaration.clone()).expect("expands").to_string();
        let second = expand(declaration).expect("expands").to_string();
        assert_eq!(first, second);
    }
}

/// The generated table carries its own declaration.
#[cfg(test)]
mod schema_const {
    use proc_macro2::{TokenStream, TokenTree};
    use quote::quote;
    use worktable_dsl::Schema;

    use super::expand;

    /// Pull the string out of `pub const <NAME>: &str = "..";` in generated code.
    fn baked_schema(generated: TokenStream, const_name: &str) -> String {
        let mut trees = generated
            .into_iter()
            .skip_while(|tree| !matches!(tree, TokenTree::Ident(ident) if ident == const_name));
        assert!(trees.next().is_some(), "no `{const_name}` const in the generated code");
        for tree in trees {
            if let TokenTree::Literal(literal) = tree {
                let text = literal.to_string();
                return syn::parse_str::<syn::LitStr>(&text).expect("a string literal").value();
            }
        }
        panic!("`{const_name}` has no value");
    }

    #[test]
    fn a_persisted_table_carries_the_declaration_it_was_built_from() {
        let declaration = quote! {
            name: Account,
            version: 3,
            persist: true,
            columns: {
                id: u64 primary_key autoincrement,
                email: String,
                nickname: String optional,
            },
            indexes: { email_idx: email unique },
            queries: { update: { Nickname(nickname) by id } }
        };

        let baked = baked_schema(expand(declaration.clone()).expect("expands"), "ACCOUNT_SCHEMA");

        assert_eq!(
            Schema::parse(&baked).expect("the baked text parses"),
            Schema::from_tokens(declaration).expect("the declaration parses"),
            "the baked declaration is not the one the table was generated from"
        );
    }

    #[test]
    fn an_in_memory_table_carries_it_too() {
        // A designer reading a crate wants every table, not only the persisted
        // ones, and the const costs a string either way.
        let declaration = quote! {
            name: Price,
            partition_by: symbol_id: u16,
            columns: { exchange_id: u8 primary_key, bid: f64 },
        };

        let baked = baked_schema(expand(declaration.clone()).expect("expands"), "PRICE_SCHEMA");

        assert_eq!(
            Schema::parse(&baked).expect("the baked text parses"),
            Schema::from_tokens(declaration).expect("the declaration parses"),
        );
    }

    #[test]
    fn the_baked_text_is_a_declaration_the_macro_accepts() {
        // Which is what makes it usable as the old table definition a
        // migration would otherwise need kept by hand.
        let declaration = quote! {
            name: Regenerated,
            persist: true,
            columns: { id: u64 primary_key autoincrement, payload: String },
            indexes: { payload_idx: payload unique }
        };

        let baked = baked_schema(expand(declaration).expect("expands"), "REGENERATED_SCHEMA");
        let reparsed: TokenStream = syn::parse_str(&baked).expect("tokenises");
        expand(reparsed).expect("the baked declaration expands");
    }
}
