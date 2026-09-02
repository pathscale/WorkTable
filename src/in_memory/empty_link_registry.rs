use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use data_bucket::Link;
use data_bucket::page::PageId;
use derive_more::Into;
use indexset::concurrent::multimap::BTreeMultiMap;
use indexset::concurrent::set::BTreeSet;
use parking_lot::FairMutex;
use tokio::sync::OwnedRwLockReadGuard;

use crate::in_memory::DATA_INNER_LENGTH;

/// A link wrapper that implements `Ord` based on absolute index calculation.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Into)]
pub struct IndexOrdLink<const DATA_LENGTH: usize = DATA_INNER_LENGTH>(pub Link);

impl<const DATA_LENGTH: usize> IndexOrdLink<DATA_LENGTH> {
    /// Calculates the absolute index of the link.
    fn absolute_index(&self) -> u64 {
        let page_id: u32 = self.0.page_id.into();
        (page_id as u64 * DATA_LENGTH as u64) + self.0.offset as u64
    }

    fn unite_with_right_neighbor(&self, other: &Self) -> Option<Self> {
        let self_end = self.absolute_index() + self.0.length as u64;
        let other_start = other.absolute_index();

        if self.0.page_id != other.0.page_id {
            return None;
        }

        if self_end == other_start {
            let new_length = self.0.length + other.0.length;
            Some(IndexOrdLink(Link {
                page_id: self.0.page_id,
                offset: self.0.offset,
                length: new_length,
            }))
        } else {
            None
        }
    }

    fn unite_with_left_neighbor(&self, other: &Self) -> Option<Self> {
        let other_end = other.absolute_index() + other.0.length as u64;
        let self_start = self.absolute_index();

        if self.0.page_id != other.0.page_id {
            return None;
        }

        if other_end == self_start {
            let new_offset = other.0.offset;
            let new_length = self.0.length + other.0.length;
            Some(IndexOrdLink(Link {
                page_id: other.0.page_id,
                offset: new_offset,
                length: new_length,
            }))
        } else {
            None
        }
    }
}

impl<const DATA_LENGTH: usize> PartialOrd for IndexOrdLink<DATA_LENGTH> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<const DATA_LENGTH: usize> Ord for IndexOrdLink<DATA_LENGTH> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.absolute_index().cmp(&other.absolute_index())
    }
}

#[derive(Debug)]
pub struct EmptyLinkRegistry<const DATA_LENGTH: usize = DATA_INNER_LENGTH> {
    index_ord_links: BTreeSet<IndexOrdLink<DATA_LENGTH>>,
    length_ord_links: BTreeMultiMap<u32, Link>,

    pub(crate) page_links_map: BTreeMultiMap<PageId, Link>,

    /// Aggregate bytes across all registered empty links. u64: a u32 wraps
    /// once the aggregate passes 4 GiB of reclaimable space.
    sum_links_len: AtomicU64,

    pub(crate) op_lock: FairMutex<()>,

    /// Reader/writer exclusion between link consumers and vacuum. An insert
    /// that pops a link holds a read guard until its write through that link
    /// completes; vacuum takes the write side, so it cannot start reclaiming
    /// while any popped link is still being written through, and no new link
    /// can be popped while vacuum runs.
    vacuum_lock: Arc<tokio::sync::RwLock<()>>,
}

/// A [`Link`] popped from the registry, together with the read guard that
/// keeps vacuum out until the caller finished writing through the link.
/// Keep the guard alive for the full duration of that write.
pub type PoppedLink = (Link, OwnedRwLockReadGuard<()>);

impl<const DATA_LENGTH: usize> Default for EmptyLinkRegistry<DATA_LENGTH> {
    fn default() -> Self {
        Self {
            index_ord_links: BTreeSet::new(),
            length_ord_links: BTreeMultiMap::new(),
            page_links_map: BTreeMultiMap::new(),
            sum_links_len: Default::default(),
            op_lock: Default::default(),
            vacuum_lock: Default::default(),
        }
    }
}

impl<const DATA_LENGTH: usize> EmptyLinkRegistry<DATA_LENGTH> {
    pub fn remove_link<L: Into<Link>>(&self, link: L) {
        let link = link.into();
        self.index_ord_links.remove(&IndexOrdLink(link));
        self.length_ord_links.remove(&link.length, &link);
        self.page_links_map.remove(&link.page_id, &link);

        // Saturating: a remove for a link that is not accounted any more
        // (e.g. a double remove) must not underflow the aggregate.
        let _ = self
            .sum_links_len
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                Some(v.saturating_sub(u64::from(link.length)))
            });
    }

    fn insert_link<L: Into<Link>>(&self, link: L) {
        let link = link.into();
        self.index_ord_links.insert(IndexOrdLink(link));
        self.length_ord_links.insert(link.length, link);
        self.page_links_map.insert(link.page_id, link);

        self.sum_links_len.fetch_add(u64::from(link.length), Ordering::AcqRel);
    }

    pub fn remove_link_for_page(&self, page_id: PageId) {
        let _g = self.op_lock.lock();
        let links = self.page_links_map.get(&page_id).map(|(_, l)| l).collect::<Vec<_>>();
        for l in links {
            self.remove_link(l);
        }
    }

    pub fn push(&self, link: Link) {
        let mut index_ord_link = IndexOrdLink(link);
        let _g = self.op_lock.lock();

        {
            let mut iter = self.index_ord_links.range(..index_ord_link).rev();
            if let Some(possible_left_neighbor) = iter.next()
                && let Some(united_link) = index_ord_link.unite_with_left_neighbor(&possible_left_neighbor)
            {
                drop(iter);

                // Remove left neighbor
                self.remove_link(possible_left_neighbor);

                index_ord_link = united_link;
            }
        }

        {
            let mut iter = self.index_ord_links.range(index_ord_link..);
            if let Some(possible_right_neighbor) = iter.next()
                && let Some(united_link) = index_ord_link.unite_with_right_neighbor(&possible_right_neighbor)
            {
                drop(iter);

                // Remove right neighbor
                self.remove_link(possible_right_neighbor);

                index_ord_link = united_link;
            }
        }

        self.insert_link(index_ord_link);
    }

    /// Pops the largest empty link. Returns it with a vacuum read guard: the
    /// old `try_lock().is_err()` probe dropped its guard immediately, so
    /// vacuum could start (and reclaim the link's page) while the caller was
    /// still writing through the link. The caller must hold the returned
    /// guard until that write completes.
    pub fn pop_max(&self) -> Option<PoppedLink> {
        // Nothing registered means nothing to pop, and the whole body below
        // exists only to choose which link to hand back. Checking one relaxed
        // atomic first keeps an append-only table off both locks entirely.
        //
        // This is the hot path, not a corner: `DataPages::insert` calls
        // `pop_max` on **every** insert, so before this an append-only workload
        // took a global `FairMutex` per row to discover there was nothing to
        // reuse. Under eight concurrent writers that mutex shows up in a
        // profile as `RawMutex::lock_slow`.
        //
        // `Relaxed` is enough because the answer is a hint, not an invariant.
        // A push landing concurrently can leave this reading zero, and the
        // caller then appends a fresh row instead of reusing a link that became
        // available a moment ago. That is the same outcome as calling one
        // instruction earlier, and the link stays registered for the next
        // insert. The reverse cannot happen: the counter is only non-zero when
        // a link was registered, and the locked path below re-checks anyway.
        if self.sum_links_len.load(Ordering::Relaxed) == 0 {
            return None;
        }

        let guard = self.vacuum_lock.clone().try_read_owned().ok()?;

        let _g = self.op_lock.lock();

        let mut iter = self.length_ord_links.iter().rev();
        let (_, max_length_link) = iter.next()?;
        drop(iter);

        self.remove_link(max_length_link);

        Some((max_length_link, guard))
    }

    pub fn iter(&self) -> impl Iterator<Item = Link> + '_ {
        self.index_ord_links.iter().map(|l| l.0)
    }

    /// Number of registered empty links, without materializing them.
    pub fn len(&self) -> usize {
        self.index_ord_links.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index_ord_links.is_empty()
    }

    pub fn get_empty_links_size_bytes(&self) -> u64 {
        self.sum_links_len.load(Ordering::Acquire)
    }

    /// Takes the vacuum (write) side of the exclusion: waits until every
    /// popped link's read guard is dropped, and blocks new pops while held.
    pub async fn lock_vacuum(&self) -> tokio::sync::RwLockWriteGuard<'_, ()> {
        self.vacuum_lock.write().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unite_with_right_neighbor() {
        let left = IndexOrdLink::<DATA_INNER_LENGTH>(Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        });

        let right = IndexOrdLink::<DATA_INNER_LENGTH>(Link {
            page_id: 1.into(),
            offset: 100,
            length: 50,
        });

        let united = left.unite_with_right_neighbor(&right).unwrap();
        assert_eq!(united.0.page_id, 1.into());
        assert_eq!(united.0.offset, 0);
        assert_eq!(united.0.length, 150);
    }

    #[test]
    fn test_unite_with_left_neighbor() {
        let left = IndexOrdLink::<DATA_INNER_LENGTH>(Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        });

        let right = IndexOrdLink::<DATA_INNER_LENGTH>(Link {
            page_id: 1.into(),
            offset: 100,
            length: 50,
        });

        let united = right.unite_with_left_neighbor(&left).unwrap();
        assert_eq!(united.0.page_id, 1.into());
        assert_eq!(united.0.offset, 0);
        assert_eq!(united.0.length, 150);
    }

    #[test]
    fn test_unite_fails_on_gap() {
        let link1 = IndexOrdLink::<DATA_INNER_LENGTH>(Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        });

        let link2 = IndexOrdLink::<DATA_INNER_LENGTH>(Link {
            page_id: 1.into(),
            offset: 200,
            length: 50,
        });

        assert!(link1.unite_with_right_neighbor(&link2).is_none());
        assert!(link2.unite_with_left_neighbor(&link1).is_none());
    }

    #[test]
    fn test_unite_fails_on_different_pages() {
        let link1 = IndexOrdLink::<DATA_INNER_LENGTH>(Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        });

        let link2 = IndexOrdLink::<DATA_INNER_LENGTH>(Link {
            page_id: 2.into(),
            offset: 100,
            length: 50,
        });

        assert!(link1.unite_with_right_neighbor(&link2).is_none());
        assert!(link2.unite_with_left_neighbor(&link1).is_none());
    }

    #[test]
    fn test_index_ord_link_ordering() {
        const TEST_DATA_LENGTH: usize = 1000;

        let link1 = IndexOrdLink::<TEST_DATA_LENGTH>(Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        });

        let link2 = IndexOrdLink::<TEST_DATA_LENGTH>(Link {
            page_id: 1.into(),
            offset: 100,
            length: 50,
        });

        let link3 = IndexOrdLink::<TEST_DATA_LENGTH>(Link {
            page_id: 2.into(),
            offset: 0,
            length: 200,
        });

        assert!(link1 < link2);
        assert!(link2 < link3);
        assert!(link1 < link3);
    }

    #[test]
    fn test_push_merges_both_sides() {
        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();

        let left = Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        };

        let middle = Link {
            page_id: 1.into(),
            offset: 100,
            length: 50,
        };

        let right = Link {
            page_id: 1.into(),
            offset: 150,
            length: 75,
        };

        registry.push(left);
        registry.push(right);
        registry.push(middle);

        let (result, _guard) = registry.pop_max().unwrap();
        assert_eq!(result.page_id, 1.into());
        assert_eq!(result.offset, 0);
        assert_eq!(result.length, 225);
    }

    #[test]
    fn test_push_non_adjacent_no_merge() {
        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();

        let link1 = Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        };

        let link2 = Link {
            page_id: 1.into(),
            offset: 200,
            length: 50,
        };

        registry.push(link1);
        registry.push(link2);

        let (pop1, _guard1) = registry.pop_max().unwrap();
        let (pop2, _guard2) = registry.pop_max().unwrap();

        assert_eq!(pop1.length, 100);
        assert_eq!(pop2.length, 50);
    }

    #[test]
    fn test_pop_max_returns_largest() {
        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();

        let small = Link {
            page_id: 1.into(),
            offset: 0,
            length: 50,
        };

        let large = Link {
            page_id: 1.into(),
            offset: 100,
            length: 200,
        };

        let medium = Link {
            page_id: 1.into(),
            offset: 300,
            length: 100,
        };

        registry.push(small);
        registry.push(large);
        registry.push(medium);

        assert_eq!(registry.pop_max().unwrap().0.length, 300); // two links were united
        assert_eq!(registry.pop_max().unwrap().0.length, 50);
    }

    #[test]
    fn test_pop_max_preserves_link_across_repeated_removal() {
        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();

        for page_id in 1..=10_000_u32 {
            let link = Link {
                page_id: page_id.into(),
                offset: page_id,
                length: page_id % 1_024 + 1,
            };
            registry.push(link);
            assert_eq!(registry.pop_max().map(|(l, _)| l), Some(link));
        }

        assert!(registry.pop_max().is_none());
    }

    #[test]
    fn test_iter_returns_all_links() {
        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();

        let link1 = Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        };

        let link2 = Link {
            page_id: 2.into(),
            offset: 0,
            length: 150,
        };

        let link3 = Link {
            page_id: 3.into(),
            offset: 0,
            length: 200,
        };

        registry.push(link1);
        registry.push(link2);
        registry.push(link3);

        let links: Vec<Link> = registry.iter().collect();
        assert_eq!(links.len(), 3);
    }

    /// The fast path in `pop_max` must agree with the slow path.
    ///
    /// `pop_max` returns early when `sum_links_len` is zero, so a counter that
    /// drifts above zero while the registry is empty costs a pointless lock,
    /// and one that drifts to zero while links remain makes reuse stop
    /// silently: inserts would append forever and the free list would never
    /// drain. Neither shows up as a failing assertion anywhere else, because
    /// appending is a correct way to insert.
    ///
    /// This pins the two together across a push/pop cycle.
    #[test]
    fn the_pop_fast_path_agrees_with_the_registry() {
        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();

        // Empty: the counter says so, and popping takes the early return.
        assert_eq!(registry.get_empty_links_size_bytes(), 0);
        assert!(registry.is_empty());
        assert!(registry.pop_max().is_none(), "an empty registry has nothing to pop");

        let link = Link {
            page_id: 1.into(),
            offset: 0,
            length: 64,
        };
        registry.push(link);

        // Non-empty: the counter must be non-zero, or `pop_max` would return
        // early and this link would never be reused.
        assert_ne!(
            registry.get_empty_links_size_bytes(),
            0,
            "a registered link left the counter at zero, so pop_max would skip it"
        );
        let popped = registry.pop_max().expect("a registered link is poppable");
        assert_eq!(popped.0, link);
        drop(popped);

        // Drained: back to agreeing.
        assert_eq!(
            registry.get_empty_links_size_bytes(),
            0,
            "the counter did not return to zero after the only link was popped"
        );
        assert!(registry.pop_max().is_none(), "a drained registry has nothing to pop");
    }

    #[test]
    fn test_empty_registry() {
        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();

        assert!(registry.pop_max().is_none());
        assert_eq!(registry.iter().count(), 0);
    }

    #[test]
    fn test_sum_links_counter() {
        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();

        let link1 = Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        };

        let link2 = Link {
            page_id: 1.into(),
            offset: 100,
            length: 150,
        };

        registry.push(link1);
        assert_eq!(registry.sum_links_len.load(Ordering::Acquire), 100);

        registry.push(link2);
        assert_eq!(registry.sum_links_len.load(Ordering::Acquire), 250);

        registry.pop_max();
        assert_eq!(registry.sum_links_len.load(Ordering::Acquire), 0);
    }

    #[test]
    fn test_sum_links_counter_saturates_on_double_remove() {
        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();

        let link = Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        };

        registry.push(link);
        registry.remove_link(link);
        assert_eq!(registry.get_empty_links_size_bytes(), 0);

        // A second remove of the same link must not underflow the aggregate.
        registry.remove_link(link);
        assert_eq!(registry.get_empty_links_size_bytes(), 0);
    }

    #[tokio::test]
    async fn test_lock_vacuum_prevents_pop() {
        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();

        let link = Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        };

        registry.push(link);

        let popped = registry.pop_max();
        assert!(popped.is_some());
        assert_eq!(popped.unwrap().0.length, 100);

        registry.push(Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        });

        let _lock = registry.lock_vacuum().await;
        let popped_locked = registry.pop_max();
        assert!(
            popped_locked.is_none(),
            "pop_max should return None when vacuum lock is held"
        );

        drop(_lock);
        let popped_after_unlock = registry.pop_max();
        assert!(
            popped_after_unlock.is_some(),
            "pop_max should return link after vacuum lock is released"
        );
        assert_eq!(popped_after_unlock.unwrap().0.length, 100);
    }

    #[tokio::test]
    async fn test_popped_link_guard_blocks_vacuum() {
        use futures::FutureExt;

        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();
        registry.push(Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        });

        let (link, guard) = registry.pop_max().unwrap();
        assert_eq!(link.length, 100);

        // The popped link is still being written through while its guard is
        // alive: vacuum must not be able to take its lock in that window.
        assert!(
            registry.lock_vacuum().now_or_never().is_none(),
            "vacuum must wait for the popped link's write to complete"
        );

        drop(guard);
        assert!(
            registry.lock_vacuum().now_or_never().is_some(),
            "vacuum should proceed once the popped link's guard is dropped"
        );
    }

    #[tokio::test]
    async fn test_concurrent_pops_share_the_vacuum_read_side() {
        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();
        registry.push(Link {
            page_id: 1.into(),
            offset: 0,
            length: 100,
        });
        registry.push(Link {
            page_id: 2.into(),
            offset: 0,
            length: 50,
        });

        // Two inserts may write through popped links at the same time; the
        // exclusion is only against vacuum, not between inserts.
        let first = registry.pop_max().unwrap();
        let second = registry.pop_max().unwrap();
        assert_eq!(first.0.length, 100);
        assert_eq!(second.0.length, 50);
    }
}
