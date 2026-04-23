mod generator;
mod parser;

use proc_macro2::TokenStream;

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let parsed = parser::MigrationEngineInput::parse(input)?;
    Ok(generator::generate(parsed))
}
