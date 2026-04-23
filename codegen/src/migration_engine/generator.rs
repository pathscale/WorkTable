use proc_macro2::{Ident, TokenStream};
use quote::quote;

use super::parser::{MigrationEngineInput, TablePath};

pub fn generate(input: MigrationEngineInput) -> TokenStream {
    let migration = &input.migration;
    let current_table = &input.current;
    let ctx_type = &input.ctx;
    let engine_name = Ident::new(
        &format!("{}Engine", input.migration),
        input.migration.span(),
    );

    let table_name_snake = input.table_name_snake();
    let table_name_lit = proc_macro2::Literal::string(&table_name_snake);
    let persistence_engine = input.persistence_engine_type();
    let pk_gen_state = input.pk_gen_state_type_tokens();

    let current_row = MigrationEngineInput::row_type_for(&TablePath {
        module: None,
        ident: current_table.clone(),
    });

    // Collect sorted versions
    let sorted_versions: Vec<u32> = input.version_tables.keys().copied().collect();
    let current_version: u32 = sorted_versions.last().map(|v| v + 1).unwrap_or(1);

    // Generate per-version migration functions
    let version_fns = input.version_tables.iter().map(|(version, table_path)| {
        let fn_name = Ident::new(
            &format!("migrate_v{}", version),
            current_table.span(),
        );

        // Build the migration chain from this version to current
        let chain_steps = build_chain_steps(
            &sorted_versions,
            *version,
            table_path,
            migration,
            &current_row,
            &input.version_tables,
        );

        quote! {
            async fn #fn_name(
                source_path: &str,
                target: &#current_table,
                ctx: &#ctx_type,
            ) -> eyre::Result<()> {
                let config = worktable::prelude::DiskConfig::new_with_table_name(source_path, #table_name_lit, #version);
                let engine = worktable::prelude::ReadOnlyPersistenceEngine::create(config).await?;
                let source = #table_path::load(engine).await?;

                let rows = source.select_all().execute()?;
                for row in rows {
                    #chain_steps
                    target.insert(current_row)?;
                }
                Ok(())
            }
        }
    });

    // Generate the version match arms
    let match_arms = input.version_tables.iter().map(|(version, _)| {
        let fn_name = Ident::new(
            &format!("migrate_v{}", version),
            current_table.span(),
        );
        quote! {
            #version => Self::#fn_name(source_path, &target, ctx).await?,
        }
    });

    quote! {
        pub struct #engine_name;

        impl #engine_name {
            #( #version_fns )*

            pub async fn migrate(
                source_path: &str,
                target_path: &str,
                ctx: &#ctx_type,
            ) -> eyre::Result<MigrationReport> {
                let source_table_path = format!("{}/{}", source_path, #table_name_lit);
                let version = worktable::migration::detect_version::<#pk_gen_state>(&source_table_path).await?;

                let target_config = worktable::prelude::DiskConfig::new_with_table_name(target_path, #table_name_lit, #current_version);
                let target_engine = #persistence_engine::new(target_config).await?;
                let target = #current_table::load(target_engine).await?;

                match version {
                    #( #match_arms )*
                    v => return Err(eyre::eyre!("Unsupported version: {}", v)),
                };

                Ok(MigrationReport { source_version: version })
            }
        }

        pub struct MigrationReport {
            pub source_version: u32,
        }
    }
}

/// Build the chain of migration steps from a version table to the current row.
fn build_chain_steps(
    sorted_versions: &[u32],
    start_version: u32,
    start_table: &TablePath,
    migration_type: &Ident,
    current_row: &TablePath,
    version_tables: &std::collections::BTreeMap<u32, TablePath>,
) -> TokenStream {
    let start_idx = sorted_versions.iter().position(|v| *v == start_version).unwrap_or(0);
    let total = sorted_versions.len();

    if sorted_versions.is_empty() {
        let from_row = MigrationEngineInput::row_type_for(start_table);
        let from_tokens = from_row.to_tokens();
        let to_tokens = current_row.to_tokens();
        return quote! {
            let current_row = <#migration_type as Migration<#from_tokens, #to_tokens>>::migrate(row, ctx);
        };
    }

    // If starting from the last version, single step to current
    if start_idx == total - 1 {
        let from_row = MigrationEngineInput::row_type_for(start_table);
        let from_tokens = from_row.to_tokens();
        let to_tokens = current_row.to_tokens();
        return quote! {
            let current_row = <#migration_type as Migration<#from_tokens, #to_tokens>>::migrate(row, ctx);
        };
    }

    // Chain: start -> next_version -> ... -> last_version -> current
    let mut steps = TokenStream::new();
    let mut current_var = quote! { row };

    for i in (start_idx + 1)..=total {
        let from_row_tokens = if i == start_idx + 1 {
            MigrationEngineInput::row_type_for(start_table).to_tokens()
        } else {
            let from_version = sorted_versions[i - 1];
            let from_table = version_tables.get(&from_version).unwrap();
            MigrationEngineInput::row_type_for(from_table).to_tokens()
        };

        let to_var = Ident::new(
            &format!("next_{}", i),
            start_table.ident.span(),
        );

        let to_row_tokens = if i < total {
            let to_version = sorted_versions[i];
            let to_table = version_tables.get(&to_version).unwrap();
            MigrationEngineInput::row_type_for(to_table).to_tokens()
        } else {
            current_row.to_tokens()
        };

        steps = quote! {
            #steps
            let #to_var = <#migration_type as Migration<#from_row_tokens, #to_row_tokens>>::migrate(#current_var, ctx);
        };
        current_var = quote! { #to_var };
    }

    quote! {
        #steps
        let current_row = #current_var;
    }
}
