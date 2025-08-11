mod defragmentator;
mod sized;
mod unsized_;

use data_bucket::Link;

pub use sized::SizedEmptyLinkRegistry;
pub use unsized_::UnsizedEmptyLinkRegistry;

/// [`EmptyLinksRegistry`] is used for storing [`Link`]'s after their release in
/// [`DataPages`].
///
/// [`DataPages`]: crate::in_memory::DataPages
pub trait EmptyLinksRegistry {
    /// Stores empty [`Link`] in this registry.
    fn add_empty_link(&self, link: Link);

    /// Returns [`Link`] that will be enough to fit data with provided `size`.
    fn find_link_with_length(&self, size: u32) -> Option<Link>;

    /// Pop's all [`Link`]'s from this [`EmptyLinksRegistry`] for further
    /// operations.
    fn as_vec(&self) -> Vec<Link>;

    /// Returns length of this [`EmptyLinksRegistry`].
    fn len(&self) -> usize;

    /// Checks if this [`EmptyLinksRegistry`] is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
