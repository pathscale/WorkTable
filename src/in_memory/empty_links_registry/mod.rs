mod sized;
mod unsized_;

use data_bucket::Link;

/// [`EmptyLinksRegistry`] is used for storing [`Link`]'s after their release in
/// [`DataPages`].
///
/// [`DataPages`]: crate::in_memory::DataPages
pub trait EmptyLinksRegistry {
    /// Stores empty [`Link`] in this registry.
    fn add_empty_link(&self, link: Link);

    /// Returns [`Link`] that will be enough to fit data with provided `size`.
    fn find_link_with_length(&self, size: u32) -> Option<Link>;
}
