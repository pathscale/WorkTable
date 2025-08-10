use crate::in_memory::empty_links_registry::EmptyLinksRegistry;
use data_bucket::Link;

/// [`EmptyLinksRegistry`] that should be used for sized `Row`'s. It uses
/// [`lockfree::Stack`] under hood and just [`pop`]'s [`Link`] when any is needed
/// because sized `Row`'s all have same size.
///
/// [`lockfree::Stack`]: lockfree::stack::Stack
/// [`pop`]: lockfree::stack::Stack::pop
pub type SizedEmptyLinkRegistry = lockfree::stack::Stack<Link>;

impl EmptyLinksRegistry for SizedEmptyLinkRegistry {
    fn add_empty_link(&self, link: Link) {
        self.push(link)
    }

    fn find_link_with_length(&self, size: u32) -> Option<Link> {
        self.pop()
    }
}
