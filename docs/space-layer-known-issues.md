# On-disk space layer: known issues

Open defects and design gaps in `src/persistence/space/**` that are documented
here rather than fixed. Durability semantics in general are covered by
[persistence-durability.md](persistence-durability.md); this file records the
concrete space-layer mechanisms behind them plus issues pinned inside the
external `data_bucket = "=0.5.2"` dependency.

## 1. Sized batch path panics on transitional TOC identities (fixed)

Fixed: `SpaceIndex::process_change_event_batch` now shares the `PageAliases`
transitional-identity machinery with the unsized path
(src/persistence/space/index/page_aliases.rs). Batch events that name a
historical page maximum (after a mid-batch split or max-remove re-key)
resolve through the aliases, and every former panic is a typed error.

## 2. Table of contents persisted before the index pages it references

`process_create_node` and `process_split_node` (both sized and unsized paths)
persist the table of contents first and only then write the new index page;
the batch paths likewise call `table_of_contents.persist(..)` before
`persist_pages_batch(..)`. A crash between the two writes leaves a durable TOC
entry pointing at a page slot that holds zeroes or a previous generation's
bytes. On the next load the TOC is trusted, so the load either fails parsing
the phantom page or (for a stale prior page) attaches wrong node content.
Reversing the order alone does not fully close the window (the two writes are
still not atomic), but writing pages before the TOC that references them would
shrink the failure to "orphaned page bytes", which a reload ignores.

## 3. No fsync discipline layer-wide

Every write path in the space layer ends with `File::flush()`, which for
`tokio::fs::File` only pushes user-space buffers to the OS; nothing calls
`sync_data`/`sync_all` except the ART checkpoint writer
(`ArtFile::write_new_file`). Data pages, index pages, info pages, and the
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

## 5. Pinned `data_bucket = "=0.5.2"` defects (cannot be fixed here)

- `seek_to_page_start_relatively` (src/page/util.rs) computes
  `(index * PAGE_SIZE as u32) as i64`: the multiply wraps in u32 once a file
  passes 4 GiB, so batch parse/persist of high page ids seeks into live early
  pages. The same class of bug was fixed on the WorkTable side in
  `update_data_length`; the batch helpers still route through this function.
- `update_at` and `DataPage::{update_at, get_at}` compute
  `link.offset + link.length` in u32 without overflow checks; adversarial or
  corrupted links near `u32::MAX` wrap instead of failing the bounds check.
  WorkTable's `save_data` now checks the addition before calling in.
- `TableOfContentsPage::remove_without_record` adjusts `estimated_size` as if
  the removed page id were also pushed onto `empty_pages` (it adds one PageId
  size back). When called without the push, the estimate over-counts by one
  PageId per call. This is conservative (segments look fuller than they are,
  causing at worst premature segment growth), and WorkTable's key-update path
  accepts the over-count deliberately.
- `IndexTableOfContents::try_insert` keeps `data_bucket`'s historical fallback
  for an entry larger than one segment: it gets its own page unchecked, and
  persisting that segment overruns the page slot exactly like the update-path
  overflow that is now guarded. The guard cannot be added to the insert path
  without breaking the small-`DATA_LENGTH` test fixtures that rely on the
  fallback; a real fix needs segment-spilling support in `data_bucket`.
