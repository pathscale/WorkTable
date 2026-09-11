# Page sizes in the v3 format

Three quantities must remain separate:

| Quantity | Meaning |
|---|---|
| Page stride | Physical bytes per page, including the 28-byte general header. Every page seek uses this value. |
| Payload capacity | Stride minus the header. Index and metadata pages retain this entire budget. |
| Row capacity | For persisted data pages, payload capacity less the worst-case live-row directory reservation and its eight-byte trailer. |

A persisted table named SomeTable emits SOME_TABLE_PAGE_SIZE and
SOME_TABLE_INNER_SIZE. The latter is calculated by data_page_row_capacity
using the minimum archived wrapped-row size. It is the in-memory row allocator
budget as well as the persisted data-row budget. It must not be reused as an
index node size or index-page payload capacity.

The directory contains eight bytes per live row. Its fixed trailer contains
CRC-32 at page offset P-8 and the count at P-4. Reserving the worst case before
allocation prevents an insertion from publishing an index and subsequently
discovering that its directory entry does not fit. Variable archives can be
larger than their minimum size and therefore need no more directory entries
than this reservation permits.

## Implementation boundaries

- DataBucket page_start_offset and seek helpers take STRIDE explicitly. Persist
  helpers check encoded payloads against STRIDE minus GENERAL_HEADER_SIZE.
- DataBucket data-page readers take both a row-buffer bound and physical stride.
  They read the entire physical payload to validate the directory and checksum.
- WorkTable SpaceData uses its row bound for DataPage and the full payload bound
  for SpaceInfo. A restored free range advances the append cursor so the append
  allocator cannot overlap memory owned by the restored free list.
- WorktableNameGenerator::get_disk_page_capacity supplies the full payload
  budget to primary and secondary index nodes, persisted index pages, and table
  of contents readers. OffsetEqLink still carries the row bound.
- Generated persistence readers and writers pass the same table stride through
  data, index and logical-index wrappers. There is no implicit stride default
  on WorkTable persistence wrappers.

## Validation and allowed sizes

The default stride is 16,384 bytes. Persisted tables accept configured sizes
of at least 512 bytes. Arctic-backed tables reject sizes above 65,535 because
its packed links have 16-bit offset and length fields. These are validation
rules for the existing page_size option; the v3 implementation adds no grammar.

Custom-page tests assert physical file lengths and reopen every row. String
and UUID index tests exercise the full index budget independently of row
capacity. The slotted-page tests enumerate rows without consulting an index,
including after deletion and variable-length relocation. Vacuum tests cover
reopen followed by reuse of a reclaimed page.

Page size is part of the store layout. Changing it requires an explicit data
cutover; reopening an existing file with a different size is not a migration.
See [the v3 cutover](on-disk-v3-cutover.md).
