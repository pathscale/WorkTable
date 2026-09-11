# On-disk space layer: known issues

Open defects and design gaps in `src/persistence/space/**` that are documented
here rather than fixed. Durability semantics in general are covered by
[persistence-durability.md](persistence-durability.md); this file records the
concrete space-layer mechanisms behind them. The release graph now uses
DataBucket 0.7; the old 0.5.2 findings below are marked as historical.

## 1. Sized batch path panics on transitional TOC identities (fixed)

Fixed: `SpaceIndex::process_change_event_batch` now shares the `PageAliases`
transitional-identity machinery with the unsized path
(src/persistence/space/index/page_aliases.rs). Batch events that name a
historical page maximum (after a mid-batch split or max-remove re-key)
resolve through the aliases, and every former panic is a typed error.

## 2. Table of contents persisted before the index pages it references (fixed)

Fixed at the call-order level: `process_create_node`, `process_split_node`,
and both batch flush paths write index pages before persisting their TOC.
Reload uses the TOC as page authority, so an unreferenced page can be ignored.
These writes are not atomic or synchronously committed. A power failure may
still lose or reorder them; issuing the page write first does not by itself
prove that a durable TOC can never reference missing or older page bytes.
See the durability contract and v3 integrity checks before planning recovery.

## 3. No fsync discipline layer-wide

Ordinary space-layer writes do not provide a power-loss commit. The current
portable file adapter does not turn `flush()` into a durability barrier; only
the ART checkpoint path calls
`sync_data` (`ArtFile::write_new_file`). Data pages, index pages, info pages, and the
table of contents are therefore never synchronously committed: after a power
loss every "completed" batch may be partially or wholly absent, and there is
no ordering barrier between the TOC write and the index-page writes it
references (see issue 2). This is consistent with the documented best-effort
contract, but it is a property of this layer, not only of the queueing above
it.

## 4. Performance: full-TOC rewrite per event, on-disk free-slot scan per insert

- Every single-event path (`process_insert_at`, `process_remove_at`,
  `process_create_node`, `process_remove_node`, `process_split_node`) that
  touches the table of contents calls `IndexTableOfContents::persist`, which
  rewrites **every** TOC segment page, not just the dirty one. With N segments
  the per-event disk traffic for a create/remove/split grows linearly with the
  total index size.
- `data_bucket`'s `IndexPage::persist_value` (used by the sized single-insert
  path) finds the next free value slot by reading values from the file one by
  one until it hits a default-initialized slot. Each insert therefore pays an
  on-disk linear scan proportional to page occupancy on top of the write
  itself.

## 5. Historical DataBucket 0.5.2 findings

The old 0.5.2 pin no longer applies. The coordinated release uses DataBucket
0.7, checked page/link bounds and v3 checksums and row directories. The former
u32 relative-seek and link-addition findings must not be quoted as current
unfixed behavior of this release.

The WorkTable TOC wrapper still permits an entry larger than one segment to
occupy its own in-memory segment. DataBucket now rejects a persist that
exceeds the page budget rather than writing into the following page. Earlier
rejection or segment spilling would improve this path; the current behavior
is a typed persistence failure, not an oversized successful page write.

TOC removal size estimates may remain conservative, causing earlier segment
growth. This is capacity accounting, distinct from the bounds checks that
prevent writing outside a page slot.
