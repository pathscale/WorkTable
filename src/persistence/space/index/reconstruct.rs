//! Reconstruction of non-unique (multi) index nodes from persisted pages.
//!
//! Extracted from the `PersistIndex` derive so the algorithm is a plain
//! generic function that can be unit-tested with synthetic pages; the proc
//! macro only generates type plumbing and node attachment.

use std::fmt::Debug;

use indexset::core::multipair::MultiPair;
use indexset::core::pair::Pair;

/// A persisted page in `(node id, logical entries)` form, ready for
/// [`reconstruct_multi_index_nodes`].
pub type PersistedMultiNode<K, V> = (Pair<K, V>, Vec<Pair<K, V>>);

/// Rebuilds the in-memory nodes of a non-unique secondary index from its
/// persisted pages.
///
/// Input contract: `entries` for every page must be the page's LOGICAL entry
/// sequence as returned by `get_node()` — index pages store cells in
/// event-arrival order, but the slot table preserves the tree order the
/// in-memory node had when it was persisted, and `get_node()` resolves
/// through it. That order is authoritative and is preserved verbatim here:
///
/// - CDC events are positional (`InsertAt`/`RemoveAt` carry an index into
///   the node), and the on-disk page applies them through the same slot
///   order. A reconstruction that re-sorts entries within a node desyncs
///   every later positional event.
/// - When a node's maximum is removed, the page promotes its logical
///   predecessor to be the new node id. The reconstructed in-memory node
///   must agree on that predecessor, or the next event addresses the node
///   by a maximum the table of contents never heard of.
///
/// What reconstruction must still re-derive is the duplicate ordering
/// *between* nodes: `MultiPair` discriminators are not persisted, and the
/// outer node index (a map keyed by each node's maximum) requires every
/// entry to compare `<=` its node's registered maximum, with maxima that
/// are unique under `Ord` — attaching a node whose maximum compares `Equal`
/// to another's *replaces* that node wholesale. Hence:
///
/// - Nodes are processed in ascending `(node id key, first entry key,
///   node id link)` order. The first-entry-key component is load-bearing:
///   several nodes can share one maximum key (a key's duplicates crossing
///   leaf boundaries), and the persisted `(key, link)` id alone cannot
///   recover their relative order — links are row locations, uncorrelated
///   with the lost discriminators. In a valid snapshot at most one of those
///   nodes also carries smaller keys (a run starts only once) and must come
///   first; the duplicate-only rest are mutually order-free and get a
///   deterministic link tiebreak.
/// - Discriminators grow across node boundaries within one key, so
///   straddling segments stay disjoint and node maxima stay unique.
///   Numbering per node instead would hand two same-max-key nodes an Equal
///   maximum and one of them would silently vanish on attach.
/// - Discriminators 0 and `u64::MAX` are reserved (range infimum/supremum
///   used by lookups), so numbering starts at 1 and is capped at
///   `u64::MAX - 1`. The cap and the `saturating_add` only matter for a key
///   with more than `u64::MAX - 2` duplicates — unreachable in practice —
///   so they are purely defensive.
///
/// Structural violations in the persisted input (empty pages, a last entry
/// that is not the node id, keys out of order within a page, keys going
/// backwards across the node sequence) are reported with `tracing::error!`
/// and handled best-effort instead of aborting the load: files damaged by
/// earlier releases must stay loadable, and `select_all` (which reads data
/// pages, not this index) remains complete either way.
pub fn reconstruct_multi_index_nodes<K, V>(
    index_name: &str,
    pages: Vec<PersistedMultiNode<K, V>>,
) -> Vec<Vec<MultiPair<K, V>>>
where
    K: Ord + Clone + Debug,
    V: Ord + PartialEq + Clone + Debug,
{
    let mut prepared = Vec::with_capacity(pages.len());
    for (node_id, entries) in pages {
        let Some(last) = entries.last() else {
            tracing::error!(
                "index {index_name}: persisted page for node id {node_id:?} has no entries but the table of contents                  still references it; skipping the page"
            );
            continue;
        };
        if last.key != node_id.key || last.value != node_id.value {
            tracing::error!(
                "index {index_name}: page for node id {node_id:?} ends with {last:?} instead of its node id; the                  reconstructed maximum will not match the table of contents and later persistence events for this                  node may not resolve"
            );
        }
        if entries.windows(2).any(|w| w[0].key > w[1].key) {
            tracing::error!(
                "index {index_name}: page for node id {node_id:?} has keys out of logical order; the slot table is                  damaged and lookups for the affected keys may be incomplete"
            );
        }
        prepared.push((node_id, entries));
    }

    prepared.sort_by(|(a_id, a_entries), (b_id, b_entries)| {
        a_id.key
            .cmp(&b_id.key)
            .then_with(|| a_entries[0].key.cmp(&b_entries[0].key))
            .then_with(|| a_id.value.cmp(&b_id.value))
    });

    let mut nodes = Vec::with_capacity(prepared.len());
    let mut prev_key: Option<K> = None;
    let mut next_discriminator = 1u64;
    for (node_id, entries) in prepared {
        let mut node = Vec::with_capacity(entries.len());
        for p in entries {
            let same_key = prev_key.as_ref() == Some(&p.key);
            if !same_key {
                if let Some(prev) = &prev_key
                    && *prev > p.key
                {
                    // Impossible for a valid snapshot given the node ordering
                    // above; a damaged file can produce it. The counter reset
                    // below can then collide node maxima, so surface it.
                    tracing::error!(
                        "index {index_name}: entry keys go backwards across the reconstructed node sequence                          ({prev:?} -> {:?} in the node for id {node_id:?}); the persisted index is structurally                          damaged and lookups for the affected keys may be incomplete",
                        p.key,
                    );
                }
                prev_key = Some(p.key.clone());
                next_discriminator = 1;
            }
            node.push(MultiPair {
                key: p.key,
                value: p.value,
                discriminator: next_discriminator.min(u64::MAX - 1),
            });
            next_discriminator = next_discriminator.saturating_add(1);
        }
        nodes.push(node);
    }
    nodes
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair(key: u64, value: u64) -> Pair<u64, u64> {
        Pair { key, value }
    }

    fn flatten(nodes: &[Vec<MultiPair<u64, u64>>]) -> Vec<(u64, u64)> {
        nodes.iter().flatten().map(|p| (p.key, p.value)).collect()
    }

    fn assert_valid_tree(nodes: &[Vec<MultiPair<u64, u64>>]) {
        for node in nodes {
            assert!(!node.is_empty(), "reconstruction must not attach empty nodes");
            for w in node.windows(2) {
                assert!(w[0] < w[1], "node not internally sorted: {:?} !< {:?}", w[0], w[1]);
            }
        }
        for w in nodes.windows(2) {
            let prev_max = w[0].last().unwrap();
            let next_min = w[1].first().unwrap();
            assert!(
                prev_max < next_min,
                "node ranges overlap: prev max {prev_max:?} !< next min {next_min:?}"
            );
        }
        // Node maxima must be unique under Ord: the outer index is a map
        // keyed by the maximum, and an Equal maximum replaces the node.
        for (i, a) in nodes.iter().enumerate() {
            for b in nodes.iter().skip(i + 1) {
                assert_ne!(
                    a.last().unwrap().cmp(b.last().unwrap()),
                    std::cmp::Ordering::Equal,
                    "two node maxima compare Equal: {:?} vs {:?}",
                    a.last().unwrap(),
                    b.last().unwrap()
                );
            }
        }
    }

    /// The review counterexample: a mixed boundary page (tail of key 1, start
    /// of key 2's run) whose node-id link sorts AFTER a duplicate-only page
    /// of key 2, with equal duplicate counts. Ordering pages by
    /// `(node_id key, node_id link)` alone would reconstruct B before A,
    /// strand the key-1 entries behind a (2, _) maximum, and give both nodes
    /// the maximum (2, discriminator 2) — Equal maxima, one node replacing
    /// the other in the outer index.
    #[test]
    fn mixed_boundary_page_with_adversarial_link_order() {
        let page_a = (pair(2, 20), vec![pair(1, 11), pair(2, 21), pair(2, 20)]);
        let page_b = (pair(2, 10), vec![pair(2, 12), pair(2, 10)]);

        // Input order must not matter; test both.
        for pages in [
            vec![page_a.clone(), page_b.clone()],
            vec![page_b.clone(), page_a.clone()],
        ] {
            let nodes = reconstruct_multi_index_nodes("test", pages);

            assert_eq!(nodes.len(), 2);
            assert_valid_tree(&nodes);

            // Every persisted entry survives exactly once.
            let mut all = flatten(&nodes);
            all.sort_unstable();
            assert_eq!(all, vec![(1, 11), (2, 10), (2, 12), (2, 20), (2, 21)]);

            // The mixed boundary node comes first (it carries key 1)...
            assert_eq!(nodes[0].first().unwrap().key, 1);
            // ...each node keeps its logical entry order verbatim...
            assert_eq!(flatten(&nodes[..1]), vec![(1, 11), (2, 21), (2, 20)]);
            assert_eq!(flatten(&nodes[1..]), vec![(2, 12), (2, 10)]);
            // ...and each node still ends with its persisted node id entry.
            assert_eq!((nodes[0].last().unwrap().key, nodes[0].last().unwrap().value), (2, 20));
            assert_eq!((nodes[1].last().unwrap().key, nodes[1].last().unwrap().value), (2, 10));
        }
    }

    /// A straddle chain for one key plus neighbours on both sides:
    /// [1,1,2,2] -> [2,2] -> [2,2] -> [2,3,3], with node-id links running
    /// against the logical order wherever the format allows it.
    #[test]
    fn multi_node_straddle_chain() {
        let pages = vec![
            (pair(3, 5), vec![pair(2, 70), pair(3, 90), pair(3, 5)]),
            (pair(2, 40), vec![pair(2, 41), pair(2, 40)]),
            (pair(2, 30), vec![pair(2, 31), pair(2, 30)]),
            (pair(2, 60), vec![pair(1, 2), pair(1, 1), pair(2, 61), pair(2, 60)]),
        ];
        let expected_len: usize = pages.iter().map(|(_, e)| e.len()).sum();

        let nodes = reconstruct_multi_index_nodes("test", pages);

        assert_eq!(nodes.len(), 4);
        assert_valid_tree(&nodes);
        assert_eq!(flatten(&nodes).len(), expected_len);
        // The mixed [1,1,2..] node must be first and the [..2,3,3] node last.
        assert_eq!(nodes[0].first().unwrap().key, 1);
        assert_eq!(nodes[3].last().unwrap().key, 3);
        // Within every node the logical entry order is preserved verbatim.
        assert_eq!(flatten(&nodes[..1]), vec![(1, 2), (1, 1), (2, 61), (2, 60)]);
        // Key 2's discriminators grow across all four nodes.
        let discs: Vec<u64> = nodes
            .iter()
            .flatten()
            .filter(|p| p.key == 2)
            .map(|p| p.discriminator)
            .collect();
        for w in discs.windows(2) {
            assert!(w[0] < w[1], "key 2 discriminators not strictly increasing: {discs:?}");
        }
    }

    /// Damaged input must not panic and must keep every entry somewhere:
    /// empty pages, a node id that is not the last entry, keys out of
    /// logical order within a page.
    #[test]
    fn damaged_pages_are_survivable() {
        let pages = vec![
            (pair(5, 1), vec![]),
            (pair(7, 99), vec![pair(6, 2), pair(7, 1)]),
            (pair(9, 50), vec![pair(9, 3), pair(8, 4), pair(9, 50)]),
        ];

        let nodes = reconstruct_multi_index_nodes("test", pages);

        assert_eq!(nodes.len(), 2);
        assert_eq!(flatten(&nodes).len(), 5);
    }
}
