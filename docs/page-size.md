# Page size: every place it is decided

A WorkTable page has two sizes and they are not interchangeable.

* **stride** is what one page occupies on disk, header included. It is what
  every file offset is computed from, and getting it wrong puts a read in the
  middle of a neighbouring page. In `data_bucket` it is the const generic
  spelled `STRIDE`, a `u32`.
* **inner size** is the stride less the 28-byte `GeneralHeader`: how many bytes
  of row or index content a page can hold. It is spelled `INNER_PAGE_SIZE`,
  `DATA_LENGTH`, or `DATA_INNER_LENGTH` depending on where you are.

A generated table emits both as constants named after itself, so `SomeTable`
gets `SOME_TABLE_PAGE_SIZE` and `SOME_TABLE_INNER_PAGE_SIZE`. Every location
below either defines one of those, threads one through, or consumes it.

## What it costs to get this wrong

A persisted table used to be refused any page size but 16384. The seeks computed
every offset from a hardcoded constant while the generated table threaded the
configured one, so the two disagreed and the file was silently corrupt. Both now
take the stride as a parameter and `page_size` works for a persisted table;
`tests/persistence/custom_page_size.rs` writes one at 8192, checks the file spans
several pages of that size, and reloads every row.

**Three separate places had to be found before that passed, and each was silent.**
They are worth knowing because the next such parameter will hide in the same
kind of place:

* `check_value_write_bounds` measured a slot write against the crate default
  rather than the page. At a smaller page it let the write run past the page end
  into its neighbour and returned success.
* one of two `WorkTable<...>` emissions in the in-memory generator hardcoded
  `INNER_PAGE_SIZE` where the other used the table constant, so an in-memory page
  and a persisted one disagreed whenever the two differed.
* `SpaceLogicalIndex` and its three siblings wrapped `SpaceIndex` without passing
  a stride, and a `= DEFAULT_PAGE_STRIDE` default on that parameter made it
  compile. The index file was then written at 16384 and read at 8192.

That last one is the lesson: **the defaults were removed.** Every instantiation
now names its stride, so a wrapper that forgets to thread it fails to compile
rather than quietly picking 16384.

## data_bucket

| Location | What it decides |
|---|---|
| `src/page/mod.rs` | `PAGE_SIZE` (the 16384 default), `INNER_PAGE_SIZE`, and `DEFAULT_PAGE_STRIDE`, which is `PAGE_SIZE` as a `u32` so callers wanting the default need no cast in generic position |
| `src/page/util.rs` `page_start_offset<STRIDE>` | The only multiplication of a page index by a stride. Everything else goes through it |
| `src/page/util.rs` `seek_to_page_start<STRIDE>`, `seek_to_page_start_relatively<STRIDE>`, `seek_by_link<STRIDE>` | The three seeks |
| `src/page/util.rs` `persist_page<T, STRIDE>`, `persist_page_in_place<T, STRIDE>`, `persist_pages_batch<T, STRIDE>` | The writes. `persist_page_in_place` also checks the payload against `STRIDE - GENERAL_HEADER_SIZE` rather than the crate default |
| `src/page/util.rs` `update_at<DATA_LENGTH, STRIDE>` | In-place row rewrite. Two parameters because the bound it checks and the offset it seeks to are different quantities |
| `src/page/util.rs` `parse_page`, `parse_pages_batch`, `parse_general_header_by_index`, `parse_data_page`, `parse_data_pages_batch` | The reads. `parse_data_page` and `parse_data_pages_batch` already took a `const PAGE_SIZE: u32` that nothing used; the stride is a separate parameter and that one is still the payload length |
| `src/page/index/mod.rs` `IndexPageUtility` | `parse_index_page_utility<STRIDE>` and `persist_index_page_utility<STRIDE>`. The default body's overflow check uses the page's own capacity |
| `src/page/index/page.rs` | `read_value_with_index<STRIDE>`, `persist_value<STRIDE>`, `remove_value<STRIDE>` |
| `src/page/index/page_for_unsized.rs` | `persist_value<STRIDE>`, `read_value_with_offset<STRIDE>` |
| `src/page/iterators.rs` | Reads at `DEFAULT_PAGE_STRIDE`. A standalone file reader with no table to ask |

## worktable

| Location | What it decides |
|---|---|
| `src/table/mod.rs` | `WorkTable<..., const DATA_LENGTH: usize = INNER_PAGE_SIZE, ...>`: the in-memory data page size |
| `src/in_memory/data.rs` | `DATA_INNER_LENGTH`, the row area of an in-memory page, and the one place still fixed to the crate default |
| `src/persistence/space/data.rs` | `SpaceData<PkGenState, const INNER_PAGE_SIZE: usize, const PAGE_SIZE: u32>`. Its `PAGE_SIZE` is the stride and is passed to every data-file call |
| `src/persistence/space/index/mod.rs` | `SpaceIndex<T, const INNER_PAGE_SIZE: u32, const STRIDE: u32>` |
| `src/persistence/space/index/unsized_.rs` | `SpaceIndexUnsized<T, const DATA_LENGTH: u32, const STRIDE: u32>` |
| `src/persistence/space/index/util.rs` | `map_index_pages_to_toc_and_general` and its unsized form build a table of contents, so they carry the stride it will be read at |
| `src/persistence/space/logical_index.rs` | `SpaceLogicalIndex`, `SpaceLogicalIndexUnsized`, `SpaceLogicalMultiIndex` and `SpaceLogicalMultiIndexUnsized` each wrap one of the above and pass the stride through |
| `src/persistence/space/index/table_of_contents.rs` | `IndexTableOfContents<T, const DATA_LENGTH: u32, const STRIDE: u32>`. It lives in the index file, so it takes that file's stride |
| `src/migration/mod.rs` | Reads at `DEFAULT_PAGE_STRIDE`. It opens files whose table it has not loaded |

There are no defaults on these parameters. A default is what let
`SpaceLogicalIndex` wrap `SpaceIndex` without a stride and take 16384 in
silence, so every instantiation names it and the compiler finds the ones that
do not.

## worktable_codegen

| Location | What it decides |
|---|---|
| `generators/in_memory/table/mod.rs` `gen_page_size_consts` | Emits `<TABLE>_PAGE_SIZE` and `<TABLE>_INNER_PAGE_SIZE` from `config.page_size`, or from the crate default when it is absent. **This is the hook.** The same function exists in `generators/persist/table/mod.rs` and `generators/read_only/table/mod.rs` |
| `generators/in_memory/table/mod.rs` | The emitted `WorkTable<...>` takes the inner constant as its `DATA_LENGTH`. There are two such emissions in this file and only one of them used to be parameterised |
| `persist_index/space/index.rs`, `persist_table/generator/space.rs` | Instantiate `SpaceIndex` and `SpaceIndexUnsized` with the inner constant and the page constant |
| `persist_index/generator.rs`, `persist_table/generator/space_file/mod.rs` | Every emitted `parse_page`, `persist_page` and `parse_data_page` call passes the table's page constant as the stride |

## worktable_dsl

| Location | What it decides |
|---|---|
| `dsl/src/parser/config.rs` | Parses `page_size` out of the `config` block |
| `dsl/src/model/config.rs` | `Config::page_size` and the span kept for diagnostics |
| `dsl/src/validate.rs` `validate_page_size` | Refuses a non-default size on a persisted table, and explains which half of the problem is still open |
| `dsl/src/validate.rs` `validate_arctic_page_size` | Refuses a size above 65535 for an Arctic-backed table: Arctic packs a link into one `u64` with 16-bit offset and length fields |

## If you add another location

1. Do not give the stride a default. Every one of the three bugs above was a
   place that compiled because something else supplied 16384.
2. Assert file lengths as well as a round trip. A round trip alone proves
   nothing: a table that silently fell back to the default still reads its own
   writes.
3. `DataPage<N>` is an inline `[u8; N]`, so a 32768-byte page overflows the
   stack of a debug-build test before it reaches any of this. Measure large
   pages in release.
