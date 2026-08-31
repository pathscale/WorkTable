//! Transitional page identities shared by the sized and unsized batch paths.
//!
//! Both `SpaceIndex` and `SpaceIndexUnsized` replay the same WorkTablesIndex
//! CDC stream shapes, so both need the same batch-scoped alias resolution
//! when a split or a max-remove re-keys a page mid-batch while later events
//! still name a historical maximum.

use data_bucket::Link;
use data_bucket::page::PageId;
use eyre::eyre;

// Normal persistence batches begin at 16 source pages. Keep that common case
// inline while allowing analyzer retries to grow beyond it without turning a
// recovery batch into a terminal capacity error.
const INLINE_BATCH_ALIASED_PAGES: usize = 16;

/// Transitional event identities for pages whose canonical TOC key changed
/// earlier in the same CDC batch.
///
/// Each page owns at most the event identity and its actual pre-event identity.
/// The current canonical identity is retained alongside them so alias lookup
/// never has to predict page mutation semantics. Normal batches use the inline
/// slots; analyzer retries may spill into `overflow` without losing events.
pub(super) struct PageAliases<T> {
    inline: [Option<PageAliasEntry<T>>; INLINE_BATCH_ALIASED_PAGES],
    overflow: Vec<PageAliasEntry<T>>,
}

pub(super) struct PageAliasEntry<T> {
    page_id: PageId,
    current_key: (T, Link),
    aliases: [Option<(T, Link)>; 2],
}

impl<T> Default for PageAliases<T> {
    fn default() -> Self {
        Self {
            inline: std::array::from_fn(|_| None),
            overflow: Vec::new(),
        }
    }
}

impl<T: Eq> PageAliases<T> {
    fn entries(&self) -> impl Iterator<Item = &PageAliasEntry<T>> {
        self.inline.iter().flatten().chain(self.overflow.iter())
    }

    pub(super) fn resolve(&self, key: &(T, Link)) -> Option<(PageId, &(T, Link))> {
        self.entries().find_map(|entry| {
            entry
                .aliases
                .iter()
                .flatten()
                .any(|alias| alias == key)
                .then_some((entry.page_id, &entry.current_key))
        })
    }

    #[cfg(test)]
    fn get(&self, key: &(T, Link)) -> Option<PageId> {
        self.resolve(key).map(|(page_id, _)| page_id)
    }

    pub(super) fn len(&self) -> usize {
        self.entries().map(|entry| entry.aliases.iter().flatten().count()).sum()
    }

    #[cfg(test)]
    fn page_len(&self) -> usize {
        self.entries().count()
    }

    pub(super) fn remove_page(&mut self, page_id: PageId) {
        if let Some(slot) = self
            .inline
            .iter_mut()
            .find(|slot| slot.as_ref().is_some_and(|entry| entry.page_id == page_id))
        {
            *slot = None;
        } else if let Some(index) = self.overflow.iter().position(|entry| entry.page_id == page_id) {
            self.overflow.swap_remove(index);
        }
    }

    pub(super) fn replace(
        &mut self,
        page_id: PageId,
        current_key: (T, Link),
        event_key: (T, Link),
        pre_event_key: (T, Link),
    ) -> eyre::Result<()> {
        let first_alias = (event_key != current_key).then_some(event_key);
        let second_alias =
            (pre_event_key != current_key && first_alias.as_ref() != Some(&pre_event_key)).then_some(pre_event_key);

        if first_alias.is_none() && second_alias.is_none() {
            self.remove_page(page_id);
            return Ok(());
        }

        for alias in [first_alias.as_ref(), second_alias.as_ref()].into_iter().flatten() {
            if let Some(owner) = self
                .entries()
                .find(|entry| entry.page_id != page_id && entry.aliases.iter().flatten().any(|stored| stored == alias))
            {
                return Err(eyre!(
                    "page alias ownership collision between {:?} and {page_id:?}",
                    owner.page_id
                ));
            }
        }

        let entry = PageAliasEntry {
            page_id,
            current_key,
            aliases: [first_alias, second_alias],
        };
        if let Some(slot) = self
            .inline
            .iter_mut()
            .find(|slot| slot.as_ref().is_some_and(|stored| stored.page_id == page_id))
        {
            *slot = Some(entry);
        } else if let Some(slot) = self.overflow.iter_mut().find(|stored| stored.page_id == page_id) {
            *slot = entry;
        } else if let Some(slot) = self.inline.iter_mut().find(|slot| slot.is_none()) {
            *slot = Some(entry);
        } else {
            self.overflow.push(entry);
        }
        Ok(())
    }
}

#[cfg(test)]
mod alias_tests {
    use super::*;

    fn link(offset: u32) -> Link {
        Link {
            page_id: 1.into(),
            offset,
            length: 8,
        }
    }

    #[test]
    fn repeated_maximum_changes_keep_one_alias_per_page() {
        let page_id = PageId::from(7);
        let mut aliases = PageAliases::default();
        for revision in 0..1_000 {
            let current = (format!("current-{revision}"), link(revision + 1_000));
            aliases
                .replace(
                    page_id,
                    current,
                    (format!("key-{revision}"), link(revision)),
                    (format!("key-{revision}"), link(revision)),
                )
                .unwrap();
        }

        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases.get(&("key-999".into(), link(999))), Some(page_id));
        assert_eq!(aliases.page_len(), 1);
    }

    #[test]
    fn canonical_only_transition_stores_no_alias_entry() {
        let page_id = PageId::from(7);
        let canonical = ("current".to_string(), link(1));
        let mut aliases = PageAliases::default();

        aliases
            .replace(page_id, canonical.clone(), canonical.clone(), canonical)
            .unwrap();

        assert_eq!(aliases.page_len(), 0);
        assert_eq!(aliases.len(), 0);
    }

    #[test]
    fn split_keeps_only_the_two_live_transitional_identities() {
        let old_page = PageId::from(3);
        let right_page = PageId::from(4);
        let mut aliases = PageAliases::default();
        aliases
            .replace(
                old_page,
                ("old-current".to_string(), link(9)),
                ("older".to_string(), link(1)),
                ("older".to_string(), link(1)),
            )
            .unwrap();
        aliases.remove_page(old_page);
        aliases
            .replace(
                right_page,
                ("right-current".to_string(), link(4)),
                ("event".to_string(), link(2)),
                ("pre-split".to_string(), link(3)),
            )
            .unwrap();

        assert_eq!(aliases.len(), 2);
        assert_eq!(aliases.page_len(), 1);
    }

    #[test]
    fn split_remove_insert_preserves_the_event_identity() {
        let right_page = PageId::from(4);
        let pre_split = ("pre-split".to_string(), link(1));
        let post_split = ("post-split".to_string(), link(2));
        let after_remove = ("after-remove".to_string(), link(3));
        let mut aliases = PageAliases::default();

        aliases
            .replace(right_page, post_split.clone(), pre_split.clone(), pre_split.clone())
            .unwrap();
        aliases
            .replace(right_page, after_remove.clone(), pre_split.clone(), post_split.clone())
            .unwrap();

        assert_eq!(aliases.get(&pre_split), Some(right_page));
        assert_eq!(aliases.get(&post_split), Some(right_page));
        assert_eq!(
            aliases.resolve(&pre_split).map(|(_, current)| current),
            Some(&after_remove)
        );
    }

    #[test]
    fn batches_beyond_inline_capacity_preserve_every_alias() {
        let mut aliases = PageAliases::default();
        let page_count = INLINE_BATCH_ALIASED_PAGES as u32 + 8;
        for page in 1..=page_count {
            aliases
                .replace(
                    PageId::from(page),
                    (format!("current-{page}"), link(page + 1_000)),
                    (format!("old-{page}"), link(page)),
                    (format!("old-{page}"), link(page)),
                )
                .unwrap();
        }
        assert_eq!(aliases.page_len(), page_count as usize);
        assert_eq!(aliases.overflow.len(), 8);
        assert_eq!(aliases.get(&("old-1".into(), link(1))), Some(PageId::from(1)));
        assert_eq!(
            aliases.get(&(format!("old-{page_count}"), link(page_count))),
            Some(PageId::from(page_count))
        );
    }

    #[test]
    fn alias_invariants_fail_without_corrupting_existing_ownership() {
        let first_page = PageId::from(1);
        let second_page = PageId::from(2);
        let shared = ("shared".to_string(), link(1));
        let mut aliases = PageAliases::default();
        aliases
            .replace(
                first_page,
                ("first-current".to_string(), link(9)),
                shared.clone(),
                shared.clone(),
            )
            .unwrap();

        assert!(
            aliases
                .replace(
                    second_page,
                    ("second-current".to_string(), link(10)),
                    shared.clone(),
                    shared.clone(),
                )
                .is_err()
        );
        assert_eq!(aliases.get(&shared), Some(first_page));
        assert_eq!(aliases.page_len(), 1);
    }
}
