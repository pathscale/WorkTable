# WorkTable and DataBucket v3 cutover

Release decision, 2026-09-11: ordinary persisted WorkTable stores will move
from page format v2 to v3. WorkTable and DataBucket must implement and release
that boundary together. This is independent of the table's `version:` schema
number and of either crate's package version.

## Motivation

A v2 data page records a high-water mark and row bytes. It does not record
where each row starts or ends. The primary index supplies those locations.
The schema in `SpaceInfoPage` explains how to decode a row, but cannot locate
rows if that index is unavailable or its representation changes. The free
range list is lossy and cannot substitute for a directory of live rows.

V3 needs a page-local row directory, with integrity validation covering both
the directory and row bytes. This makes data pages independently readable and
provides a basis for scans, index rebuilding, export and future migrations.
The directory changes the byte layout and usable page capacity. DataBucket's
codec and WorkTable's allocation, mutation, vacuum and persistence paths must
agree on it.

## Rollout policy

For almost all deployments, the planned migration is an explicit drop of the
old store followed by recreation or regeneration. There is no requirement for
a general v2 reader in the new runtime. An application that must retain data
needs an explicit source-to-target conversion tool; its old reader can remain
isolated from the production runtime.

Opening incompatible data must report a clear version error. An application
must not silently reinterpret, overwrite or automatically delete an old store.
A rollback to the old binary cannot use the new store.

## Binary layout and implementation

Ordinary data pages now use format 3. For a page stride P, the general
header remains bytes 0..28. Row offsets are relative to byte 28. The
directory ends at P-8 and contains little-endian pairs of u32 offset and
u32 length, one per live row. Entries are ordered by offset. The CRC-32
occupies P-8..P-4; the live-row count occupies P-4..P. Row bytes grow
forward; the directory occupies the tail. The checksum covers the entire
payload, including unused bytes, directory and count, excluding only its
own four-byte word. Header fields are validated separately.

WorkTable reserves room for the worst-case slot count using the minimum
archived row-wrapper size. This reduces the row allocator capacity. Index
and metadata pages keep their full payload budget, P-28. Updating, deleting,
relocating and reclaiming rows maintains the directory. Clearing a reclaimed
page persists an empty directory before advertising it as reusable. Reload
restores free-range ownership so append allocation cannot overlap it.

The separate Vec snapshot codec also uses version 3, but has a different
payload: an archived Vec<Row>, a count at P-8 and a checksum at P-4. It
starts with a data page and a row-type fingerprint; an ordinary WorkTable
space starts with a SpaceInfo page and schema metadata. These files are not
interchangeable. Page version alone does not identify the container.

DataBucket tools can create a sample v3 file and enumerate live row extents
without opening an index. The slotted-page tests independently decode rows
after inserts, deletes and relocation, and reject a preserved real v2
fixture without changing its bytes. Full release validation remains required
before readiness is declared.

## Required verification

Exercise multi-page inserts, variable-length updates, deletes, free-space
reuse, vacuum and reopen using the new layout. Read live rows from data pages
without consulting indexes, then verify rebuilt primary and secondary indexes.
Check directory capacity, row boundaries, page identity, checksums and explicit
refusal of v2 and unknown versions. Use actual old-format bytes for the refusal
test rather than having the new writer synthesize its own supposed old store.

Rerun persistence and mutation performance measurements after the integrated
change, including page sizes and batching. Measurements of the current v2
implementation cannot establish the cost of the v3 directory.
