use proc_macro2::{Ident, Span};

#[derive(Debug, Default)]
pub struct Config {
    pub page_size: Option<u32>,
    /// Span of the `page_size` value literal, kept for validation errors.
    pub page_size_span: Option<Span>,
    pub row_derives: Vec<Ident>,
}
