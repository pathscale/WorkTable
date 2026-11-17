use proc_macro2::TokenStream;
use quote::quote;

use crate::persist_index::generator::Generator;
use crate::persist_index::parser::Parser;

mod generator;
mod parser;
mod space;

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input_struct = Parser::parse_struct(input)?;
    let attributes = Parser::parse_attributes(&input_struct.attrs)?;
    let mut generator = Generator::with_attributes(input_struct, attributes)?;

    let type_def = generator.gen_persist_type()?;
    let persistable_def = generator.gen_persistable_impl()?;
    let impl_def = generator.gen_persist_impl()?;
    let space_index = generator.gen_space_index()?;

    Ok(quote! {
        #type_def
        #impl_def
        #persistable_def
        #space_index
    })
}

#[cfg(test)]
mod tests {
    use quote::quote;
    use rkyv::{Archive, Deserialize, Serialize};

    use crate::persist_index::expand;

    #[derive(Archive, Copy, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
    pub struct Link {
        pub page_id: u32,
        pub offset: u32,
        pub length: u32,
    }

    #[test]
    fn test() {
        let input = quote! {
            #[derive(Debug, Default, Clone)]
            pub struct TestIndex {
                test_idx: TreeIndex<i64, Link>,
                exchnage_idx: TreeIndex<String, worktable::prelude::Arc<LockFreeSet<Link>>>
            }
        };

        let _res = expand(input).unwrap();
    }

    #[test]
    fn test_read_only() {
        let input = quote! {
            #[derive(Debug, Default, Clone)]
            #[index(read_only)]
            pub struct ReadOnlyIndex {
                test_idx: TreeIndex<i64, Link>,
            }
        };

        let res = expand(input).unwrap();
        let output = res.to_string();

        assert!(
            !output.contains("pub async fn persist"),
            "read_only index should not have persist method"
        );
        assert!(
            output.contains("fn get_persisted_index"),
            "read_only index should have get_persisted_index returning default"
        );
        assert!(
            output.contains("pub async fn parse_from_file"),
            "read_only index should have parse_from_file method"
        );
        assert!(
            output.contains("fn from_persisted"),
            "read_only index should have from_persisted method"
        );
    }

    #[test]
    fn skipped_derived_field_is_not_part_of_persisted_index_format() {
        let input = quote! {
            #[derive(Debug, Default)]
            pub struct DerivedIndex {
                durable: TreeIndex<i64, Link>,
                #[index(skip)]
                columnar: ParkingRwLock<DerivedColumnarData>,
            }
        };

        let output = expand(input).unwrap().to_string();
        let persisted_type = output
            .split("struct DerivedIndexPersisted")
            .nth(1)
            .expect("persisted index type");
        let persisted_fields = persisted_type.split('}').next().unwrap();
        assert!(persisted_fields.contains("durable"));
        assert!(!persisted_fields.contains("columnar"));
        assert!(output.contains("columnar : Default :: default"));
    }

    #[test]
    fn secondary_index_events_preserve_declaration_order() {
        let input = quote! {
            #[derive(Debug, Default)]
            pub struct TestOrderIndex {
                zeta_idx: TreeIndex<i64, Link>,
                alpha_idx: TreeIndex<u64, Link>,
                middle_idx: TreeIndex<u32, Link>,
                tail_idx: TreeIndex<i32, Link>,
            }
        };

        let output = expand(input).unwrap().to_string();
        let events_struct = output
            .split("struct TestOrderSpaceSecondaryIndexEvents")
            .nth(1)
            .expect("secondary index events struct")
            .split('}')
            .next()
            .expect("secondary index events fields");
        let zeta_field = events_struct.find("zeta_idx").expect("zeta_idx field");
        let alpha_field = events_struct.find("alpha_idx").expect("alpha_idx field");
        let middle_field = events_struct.find("middle_idx").expect("middle_idx field");
        let tail_field = events_struct.find("tail_idx").expect("tail_idx field");
        assert!(zeta_field < alpha_field);
        assert!(alpha_field < middle_field);
        assert!(middle_field < tail_field);

        let batch_fn = output
            .split("fn process_change_event_batch")
            .nth(1)
            .expect("process_change_event_batch fn");
        let zeta_batch = batch_fn
            .find("events . zeta_idx")
            .expect("zeta_idx batch call");
        let alpha_batch = batch_fn
            .find("events . alpha_idx")
            .expect("alpha_idx batch call");
        let middle_batch = batch_fn
            .find("events . middle_idx")
            .expect("middle_idx batch call");
        let tail_batch = batch_fn
            .find("events . tail_idx")
            .expect("tail_idx batch call");
        assert!(zeta_batch < alpha_batch);
        assert!(alpha_batch < middle_batch);
        assert!(middle_batch < tail_batch);
    }
}
