use crate::in_memory::empty_links_registry::EmptyLinksRegistry;
use data_bucket::Link;
use std::sync::atomic::{AtomicUsize, Ordering};

/// [`EmptyLinksRegistry`] that should be used for sized `Row`'s. It uses
/// [`lockfree::Stack`] under hood and just [`pop`]'s [`Link`] when any is needed
/// because sized `Row`'s all have same size.
///
/// [`lockfree::Stack`]: lockfree::stack::Stack
/// [`pop`]: lockfree::stack::Stack::pop
#[derive(Debug, Default)]
pub struct SizedEmptyLinkRegistry {
    stack: lockfree::stack::Stack<Link>,
    length: AtomicUsize,
}

impl EmptyLinksRegistry for SizedEmptyLinkRegistry {
    fn add_empty_link(&self, link: Link) {
        self.stack.push(link);
        self.length.fetch_add(1, Ordering::Relaxed);
    }

    fn find_link_with_length(&self, _size: u32) -> Option<Link> {
        // `size` can be ignored as sized row's all have same size.
        if let Some(val) = self.stack.pop() {
            self.length.fetch_sub(1, Ordering::Relaxed);
            Some(val)
        } else {
            None
        }
    }

    fn as_vec(&self) -> Vec<Link> {
        self.stack.pop_iter().collect()
    }

    fn len(&self) -> usize {
        self.length.load(Ordering::Relaxed)
    }
}
