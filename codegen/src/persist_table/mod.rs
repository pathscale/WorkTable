use proc_macro2::TokenStream;
use quote::quote;

use crate::persist_table::generator::Generator;
use crate::persist_table::parser::Parser;

mod generator;
mod parser;

pub use generator::WT_INDEX_EXTENSION;

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input_fn = Parser::parse_struct(input)?;
    let pk_ident = Parser::parse_pk_ident(&input_fn);
    let attributes = Parser::parse_attributes(&input_fn.attrs);

    let generator = Generator {
        struct_def: input_fn,
        pk_ident,
        attributes,
    };

    let space_file_def = generator.gen_space_file_def();
    let persistence_engine = if generator.attributes.read_only {
        quote! {}
    } else {
        generator.get_persistence_engine_type()
    };
    let persistence_task = if generator.attributes.read_only {
        quote! {}
    } else {
        generator.get_persistence_task_type()
    };

    Ok(quote! {
        #space_file_def
        #persistence_engine
        #persistence_task
    })
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use crate::persist_table::expand;

    #[test]
    fn test_read_only_skips_engine_and_task() {
        let input = quote! {
            #[derive(Debug)]
            #[table(read_only)]
            pub struct TestReadOnlyWorkTable(WorkTable<TestRow, TestPk, (), 4096>);
        };

        let res = expand(input).unwrap();
        let output = res.to_string();

        assert!(
            !output.contains("PersistenceEngine"),
            "read_only should not generate PersistenceEngine"
        );
        assert!(
            !output.contains("PersistenceTask"),
            "read_only should not generate PersistenceTask"
        );
        assert!(
            output.contains("fn into_worktable (self)"),
            "read_only should have sync into_worktable without engine param"
        );
        assert!(
            !output.contains("async fn into_worktable"),
            "read_only into_worktable should not be async"
        );
    }

    #[test]
    fn test_normal_generates_engine_and_task() {
        let input = quote! {
            #[derive(Debug)]
            pub struct TestWorkTable(WorkTable<TestRow, TestPk, (), 4096>);
        };

        let res = expand(input).unwrap();
        let output = res.to_string();

        assert!(
            output.contains("PersistenceEngine"),
            "normal should generate PersistenceEngine"
        );
        assert!(
            output.contains("PersistenceTask"),
            "normal should generate PersistenceTask"
        );
        assert!(
            output.contains("async fn into_worktable"),
            "normal into_worktable should be async"
        );
    }
}
