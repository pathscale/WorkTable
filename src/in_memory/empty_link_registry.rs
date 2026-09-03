use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use data_bucket::Link;
use data_bucket::page::PageId;
use derive_more::Into;
use indexset::concurrent::multimap::BTreeMultiMap;
use indexset::concurrent::set::BTreeSet;
use parking_lot::FairMutex;
use tokio::sync::{Notify, OwnedRwLockReadGuard};

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
    ///
    /// Accounting only. It is a sum of lengths, so it cannot answer "is
    /// anything registered" without assuming every mutation was real, and a
    /// mutation that changed nothing used to move it anyway.
    sum_links_len: AtomicU64,

    /// How many links are registered in the authoritative container.
    ///
    /// Separate from `sum_links_len` because the `pop_max` fast path needs
    /// membership, not bytes, and membership is the thing that can be kept
    /// exact: both counters now move only when `index_ord_links` actually
    /// reports a change. Before that, a double remove drove the byte total to
    /// zero while links were still registered, and the fast path then returned
    /// `None` before taking the lock, so those links could never be reused and
    /// inserts appended forever.
    item_count: AtomicUsize,

    pub(crate) op_lock: FairMutex<()>,

    /// Reader/writer exclusion between link consumers and vacuum. An insert
    /// that pops a link holds a read guard until its write through that link
    /// completes; vacuum takes the write side, so it cannot start reclaiming
    /// while any popped link is still being written through, and no new link
    /// can be popped while vacuum runs.
    vacuum_lock: Arc<tokio::sync::RwLock<()>>,

    /// How many times a caller has asked this registry for reclaimable space.
    ///
    /// This is what vacuum samples to answer "is now a good time". While
    /// vacuum holds the exclusion every one of these attempts fails and the
    /// caller allocates a fresh page instead, so the *rate* of attempts is
    /// precisely the rate at which vacuum is making foreground inserts more
    /// expensive. Counting demand rather than instrumenting the insert path
    /// costs nothing extra: `pop_max` already loads an atomic here.
    pop_attempts: AtomicU64,

    /// Reclaimable bytes at which a parked vacuum is woken, or `0` when
    /// nothing is waiting.
    ///
    /// A timer cannot know when a table became fragmented; the registry can,
    /// because it is the thing that grew.
    vacuum_wake_threshold: AtomicU64,

    /// Wakes a vacuum parked in [`Self::wait_for_fragmentation`].
    vacuum_wake: Arc<Notify>,

    /// Pages named by a *batched* delete, for the next sweep to look at first.
    ///
    /// A ranged or batched delete concentrates its freed space, so its pages
    /// are the ones most likely to empty out entirely — the cheapest possible
    /// reclamation, and the most valuable. A scattered single-row delete says
    /// nothing about where to look, so [`Self::push`] does not record
    /// anything and only [`Self::push_many`] does.
    targeted_pages: FairMutex<std::collections::BTreeSet<PageId>>,
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
            item_count: Default::default(),
            op_lock: Default::default(),
            vacuum_lock: Default::default(),
            pop_attempts: Default::default(),
            vacuum_wake_threshold: Default::default(),
            vacuum_wake: Default::default(),
            targeted_pages: Default::default(),
        }
    }
}

impl<const DATA_LENGTH: usize> EmptyLinkRegistry<DATA_LENGTH> {
    pub fn remove_link<L: Into<Link>>(&self, link: L) {
        let link = link.into();
        // `index_ord_links` is the authoritative membership set, and its
        // `remove` reports whether anything was there. The counters follow
        // that answer rather than the caller's intent: a remove for a link
        // that is not registered must leave both untouched, or the aggregate
        // drifts down past what is actually free.
        let was_present = self.index_ord_links.remove(&IndexOrdLink(link)).is_some();
        self.length_ord_links.remove(&link.length, &link);
        self.page_links_map.remove(&link.page_id, &link);

        if was_present {
            self.item_count.fetch_sub(1, Ordering::AcqRel);
            // Saturating still, as a belt: the count is what the fast path
            // trusts, and this is accounting.
            let _ = self
                .sum_links_len
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                    Some(v.saturating_sub(u64::from(link.length)))
                });
        }
    }

    fn insert_link<L: Into<Link>>(&self, link: L) {
        let link = link.into();
        // Symmetrically: `insert` reports false when an equal entry was
        // already registered, and counting that would drift the other way,
        // leaving the fast path convinced there is space that is not there.
        let is_new = self.index_ord_links.insert(IndexOrdLink(link));
        self.length_ord_links.insert(link.length, link);
        self.page_links_map.insert(link.page_id, link);

        if is_new {
            self.item_count.fetch_add(1, Ordering::AcqRel);
            let freed = self.sum_links_len.fetch_add(u64::from(link.length), Ordering::AcqRel) + u64::from(link.length);
            self.wake_vacuum_if_fragmented(freed);
        }
    }

    pub fn remove_link_for_page(&self, page_id: PageId) {
        let _g = self.op_lock.lock();
        let links = self.page_links_map.get(&page_id).map(|(_, l)| l).collect::<Vec<_>>();
        for l in links {
            self.remove_link(l);
        }
    }

    pub fn push(&self, link: Link) {
        let _g = self.op_lock.lock();
        self.push_locked(IndexOrdLink(link));
    }

    /// Restores several freed links in one pass.
    ///
    /// Reclamation frees links in retirement order, and in the common case
    /// those links are adjacent: a range delete, or any workload that deletes
    /// in primary key order, frees a contiguous run of them. Pushed one at a
    /// time each link pays its own coalesce, and a coalesce is up to two
    /// removals and one insertion across three ordered containers, so freeing
    /// `n` adjacent links costs `n` times that. Merging the batch against
    /// itself first turns a whole run into a single insertion, and the lock is
    /// taken once rather than `n` times.
    pub fn push_many(&self, links: &[Link]) {
        match links {
            [] => return,
            [link] => return self.push(*link),
            _ => {}
        }

        let runs = Self::merge_runs(links);

        // Recorded before the links are registered, so a sweep woken by the
        // registration below already sees where to look.
        self.note_coalesced_pages(links, &runs);

        let _g = self.op_lock.lock();
        for run in runs {
            self.push_locked(run);
        }
    }

    /// Merges a batch of freed links against itself, so a contiguous run
    /// becomes one link before the registry ever sees it.
    ///
    /// Separate from [`push_many`] and pure, because the saving it represents
    /// is invisible in the registry's final state: coalescing per link and
    /// coalescing per run leave exactly the same contents, and differ only in
    /// how much work they did to get there. A test can only hold onto that
    /// difference by looking at this directly.
    ///
    /// [`push_many`]: Self::push_many
    fn merge_runs(links: &[Link]) -> Vec<IndexOrdLink<DATA_LENGTH>> {
        let mut sorted: Vec<IndexOrdLink<DATA_LENGTH>> = links.iter().copied().map(IndexOrdLink).collect();
        sorted.sort_unstable();

        // `unite_with_right_neighbor` already encodes both the same-page
        // requirement and the adjacency test, so merging the sorted batch is
        // the same rule the registry applies against its own contents.
        let mut runs: Vec<IndexOrdLink<DATA_LENGTH>> = Vec::with_capacity(sorted.len());
        for link in sorted {
            match runs.last().and_then(|last| last.unite_with_right_neighbor(&link)) {
                Some(united) => {
                    if let Some(last) = runs.last_mut() {
                        *last = united;
                    }
                }
                None => runs.push(link),
            }
        }
        runs
    }

    /// The body of [`push`], for callers already holding `op_lock`.
    ///
    /// [`push`]: Self::push
    fn push_locked(&self, mut index_ord_link: IndexOrdLink<DATA_LENGTH>) {
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
        // Counted before the early return, because an attempt that finds the
        // registry empty is still a caller that wanted space.
        self.pop_attempts.fetch_add(1, Ordering::Relaxed);

        if self.item_count.load(Ordering::Relaxed) == 0 {
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

    /// How many times a caller has asked for reclaimable space since the
    /// registry was created. Monotonic; callers compare two samples.
    pub fn pop_attempts(&self) -> u64 {
        self.pop_attempts.load(Ordering::Relaxed)
    }

    /// Reclaimable bytes currently registered.
    pub fn reclaimable_bytes(&self) -> u64 {
        self.sum_links_len.load(Ordering::Relaxed)
    }

    /// Wake a vacuum parked in [`Self::wait_for_fragmentation`] once
    /// reclaimable space reaches `bytes`. Passing `0` disables the wake.
    pub fn set_vacuum_wake_threshold(&self, bytes: u64) {
        self.vacuum_wake_threshold.store(bytes, Ordering::Release);
    }

    /// Park until enough space has been freed to be worth reclaiming.
    ///
    /// Returns immediately when the threshold is already met, so a caller that
    /// checks and then waits cannot miss the crossing that happened in
    /// between.
    pub async fn wait_for_fragmentation(&self) {
        let threshold = self.vacuum_wake_threshold.load(Ordering::Acquire);
        if threshold == 0 || self.sum_links_len.load(Ordering::Acquire) < threshold {
            self.vacuum_wake.notified().await;
        }
    }

    /// Records the pages a batch freed *contiguously*.
    ///
    /// Every deferred reclamation arrives through [`Self::push_many`], so the
    /// batch alone does not say a delete was ranged — a scattered delete is
    /// batched too. Coalescing does say it: a run only forms when freed links
    /// were adjacent on the same page, which is what a ranged delete produces
    /// and a scattered one does not. So a page whose links merged is a page a
    /// ranged delete emptied part of, and it is where a sweep should look
    /// first.
    fn note_coalesced_pages(&self, links: &[Link], runs: &[IndexOrdLink<DATA_LENGTH>]) {
        let mut links_per_page: std::collections::BTreeMap<PageId, usize> = Default::default();
        for link in links {
            *links_per_page.entry(link.page_id).or_default() += 1;
        }
        let mut runs_per_page: std::collections::BTreeMap<PageId, usize> = Default::default();
        for run in runs {
            *runs_per_page.entry(run.0.page_id).or_default() += 1;
        }

        let coalesced: Vec<PageId> = links_per_page
            .into_iter()
            .filter(|(page, count)| runs_per_page.get(page).copied().unwrap_or(0) < *count)
            .map(|(page, _)| page)
            .collect();
        if coalesced.is_empty() {
            return;
        }
        self.targeted_pages.lock().extend(coalesced);
    }

    /// Takes the pages named by batched deletes since the last call.
    ///
    /// Draining rather than reading: a sweep that has taken them is
    /// responsible for them, and leaving them would make every later sweep
    /// re-prioritise pages that are already compact.
    pub fn take_targeted_pages(&self) -> std::collections::BTreeSet<PageId> {
        std::mem::take(&mut *self.targeted_pages.lock())
    }

    /// Wakes a parked vacuum when freeing crossed the configured threshold.
    ///
    /// Called on the delete path, so it must stay to a relaxed load in the
    /// common case where nothing is waiting.
    fn wake_vacuum_if_fragmented(&self, reclaimable: u64) {
        let threshold = self.vacuum_wake_threshold.load(Ordering::Relaxed);
        if threshold != 0 && reclaimable >= threshold {
            self.vacuum_wake.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn link(page_id: u32, offset: u32, length: u32) -> Link {
        Link {
            page_id: page_id.into(),
            offset,
            length,
        }
    }

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

    /// The batch path must land the registry in exactly the state the
    /// one-at-a-time path would, for every shape of input: adjacent, gapped,
    /// out of order, spread across pages, and duplicated. Comparing the two
    /// registries rather than asserting a hand-written expectation is what
    /// gives this teeth: `push_many` merging a run that `push` would not, or
    /// dropping a link at a page boundary, shows up as a difference here
    /// without anyone having to predict it.
    #[test]
    fn push_many_agrees_with_pushing_one_at_a_time() {
        let cases: Vec<Vec<Link>> = vec![
            // A contiguous run in order: the case reclamation actually hits.
            (0..8).map(|i| link(1, i * 32, 32)).collect(),
            // The same run shuffled, because retirement order is not
            // allocation order once inserts reuse links.
            vec![
                link(1, 96, 32),
                link(1, 0, 32),
                link(1, 224, 32),
                link(1, 32, 32),
                link(1, 160, 32),
            ],
            // Two runs with a live row between them.
            vec![link(1, 0, 32), link(1, 32, 32), link(1, 128, 32), link(1, 160, 32)],
            // Adjacent offsets on different pages must not merge.
            vec![link(1, 0, 32), link(2, 32, 32), link(2, 0, 32), link(3, 0, 32)],
            // Uneven lengths, so a wrong merge changes a length rather than
            // just a count.
            vec![link(4, 0, 16), link(4, 16, 48), link(4, 64, 8)],
            // A repeated link. Neither path deduplicates, so neither may
            // start.
            vec![link(5, 0, 32), link(5, 0, 32)],
        ];

        for (case, links) in cases.iter().enumerate() {
            let one_at_a_time = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();
            for l in links {
                one_at_a_time.push(*l);
            }

            let batched = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();
            batched.push_many(links);

            let expected: Vec<Link> = one_at_a_time.iter().collect();
            let actual: Vec<Link> = batched.iter().collect();
            assert_eq!(actual, expected, "case {case}: registries diverged");
            assert_eq!(
                batched.get_empty_links_size_bytes(),
                one_at_a_time.get_empty_links_size_bytes(),
                "case {case}: byte totals diverged"
            );
        }
    }

    /// The reason the batch path exists, asserted where it is observable.
    ///
    /// It cannot be asserted through the registry: pushing eight adjacent
    /// links one at a time and pushing them as one run both leave a registry
    /// holding a single 256-byte link. The saving is the work done to get
    /// there, so the merge is tested as the pure function it is. Deleting the
    /// self-merge leaves every registry-level test green and fails this one.
    #[test]
    fn merging_a_batch_collapses_runs_before_the_registry_sees_them() {
        // A contiguous run in retirement order.
        let run: Vec<Link> = (0..8).map(|i| link(7, i * 32, 32)).collect();
        let merged = EmptyLinkRegistry::<DATA_INNER_LENGTH>::merge_runs(&run);
        assert_eq!(merged.len(), 1, "eight adjacent links are one insertion, not eight");
        assert_eq!(merged[0].0.offset, 0);
        assert_eq!(merged[0].0.length, 8 * 32);

        // Out of order, because retirement order is not allocation order.
        let shuffled = vec![link(7, 96, 32), link(7, 0, 32), link(7, 64, 32), link(7, 32, 32)];
        let merged = EmptyLinkRegistry::<DATA_INNER_LENGTH>::merge_runs(&shuffled);
        assert_eq!(merged.len(), 1, "sorting is what makes an out-of-order run mergeable");
        assert_eq!(merged[0].0.length, 128);

        // A gap splits the batch, and nothing merges across pages however
        // adjacent the offsets look.
        let split = vec![link(7, 0, 32), link(7, 32, 32), link(7, 128, 32)];
        assert_eq!(EmptyLinkRegistry::<DATA_INNER_LENGTH>::merge_runs(&split).len(), 2);
        let across_pages = vec![link(7, 0, 32), link(8, 32, 32)];
        assert_eq!(
            EmptyLinkRegistry::<DATA_INNER_LENGTH>::merge_runs(&across_pages).len(),
            2,
            "a page boundary is not an adjacency"
        );

        // Nothing is invented or dropped: the merged bytes equal the input.
        let total: u64 = run.iter().map(|l| u64::from(l.length)).sum();
        let merged_total: u64 = EmptyLinkRegistry::<DATA_INNER_LENGTH>::merge_runs(&run)
            .iter()
            .map(|l| u64::from(l.0.length))
            .sum();
        assert_eq!(merged_total, total);
    }

    /// A batch merged against itself still has to coalesce with what the
    /// registry already holds on both sides.
    #[test]
    fn push_many_coalesces_with_links_already_registered() {
        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();
        registry.push(link(1, 0, 32));
        registry.push(link(1, 160, 32));

        registry.push_many(&[link(1, 64, 32), link(1, 32, 32), link(1, 96, 32), link(1, 128, 32)]);

        assert_eq!(registry.len(), 1, "the batch should bridge the two registered links");
        let (popped, _guard) = registry.pop_max().unwrap();
        assert_eq!(popped.offset, 0);
        assert_eq!(popped.length, 192);
    }

    /// A remove for a link that is not registered must not move the counters.
    ///
    /// It used to. `remove_link` subtracted the length whether or not anything
    /// was there, so removing the same link twice drove the aggregate to zero
    /// while another link was still registered. The `pop_max` fast path then
    /// returned `None` before taking the lock, and that surviving link could
    /// never be reused: every insert appended instead, forever.
    #[test]
    fn a_double_remove_does_not_hide_a_surviving_link() {
        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();
        let a = link(1, 0, 100);
        let b = link(1, 500, 100);
        registry.push(a);
        registry.push(b);

        let _g = registry.op_lock.lock();
        registry.remove_link(a);
        registry.remove_link(a);
        drop(_g);

        assert_eq!(registry.len(), 1, "b must still be registered");
        let (popped, _guard) = registry
            .pop_max()
            .expect("the surviving link must still be reachable through the fast path");
        assert_eq!(popped, b);
    }

    /// And pushing the same link twice must not invent space that is not
    /// there, which is the same defect with the sign flipped.
    #[test]
    fn a_duplicate_push_does_not_invent_space() {
        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();
        let a = link(2, 0, 100);
        registry.push(a);
        registry.push(a);

        assert_eq!(registry.len(), 1, "the same link is one entry");
        assert_eq!(
            registry.get_empty_links_size_bytes(),
            100,
            "byte total must match the one entry actually registered"
        );
        let (popped, _guard) = registry.pop_max().expect("the entry is there");
        assert_eq!(popped, a);
        assert!(
            registry.pop_max().is_none(),
            "one push and one duplicate is one link, not two"
        );
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

    /// A ranged delete frees adjacent links, which coalesce; a scattered one
    /// frees links that do not. Only the first says anything about where the
    /// next sweep should look, and every deferred reclamation arrives batched
    /// either way — so the batch alone cannot be the signal.
    #[test]
    fn only_a_contiguous_batch_marks_its_pages_for_the_next_sweep() {
        let registry = EmptyLinkRegistry::<DATA_INNER_LENGTH>::default();

        registry.push_many(&[link(1, 0, 10), link(1, 50, 10), link(1, 100, 10)]);
        assert!(
            registry.take_targeted_pages().is_empty(),
            "a scattered batch names no page: nothing about it says where space is concentrated"
        );

        registry.push_many(&[link(2, 0, 10), link(2, 10, 10), link(2, 20, 10)]);
        assert_eq!(
            registry.take_targeted_pages().into_iter().collect::<Vec<_>>(),
            vec![PageId::from(2)],
            "adjacent links coalesce, which is what a ranged delete leaves behind"
        );

        assert!(
            registry.take_targeted_pages().is_empty(),
            "taking must drain: a sweep that took a page owns it, and leaving it would \
             make every later sweep re-prioritise a page that is already compact"
        );
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
