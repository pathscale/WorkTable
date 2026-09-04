Change Log
==========

## [1.0.0-beta.18]

### Added

- Selectable `worktables_index`, `arctic`, and `congee` backends for generated
  primary and supported secondary indexes, including persisted topology load.
- Async and batched insert/delete paths, with bulk-mutation signaling used by
  reactive vacuum scheduling.
- `MemStat` for generated persisted/read-only tables and Arc-owned
  `unload_gracefully` for generation swaps.
- Strict persisted-state validation, schema metadata, recovery loading, and
  Arctic string/non-unique index support.

### Changed

- WorkTable row/page reclamation uses the local `ps-reclaim` domain for every
  index backend; the WorkTable Arctic adapter also selects Arctic's
  `ps-reclaim` SMR exclusively.
- Readers now synchronize on the exact physical cell. Unrelated rows cannot
  block because of a hashed lock collision.
- Vacuum discovers move candidates from a transient primary-index snapshot and
  keeps only one live-cell counter per page, removing the previous four-byte
  per-row directory.
- Vacuum waits for two quiet observations after mutation activity and yields
  throughout a bulk mutation instead of competing with foreground work.
- The archived wrapper retains the beta.17 inner-row position so legacy stores
  without bundled schema metadata remain readable.

### Fixed

- Torn reads and premature physical-link reuse during concurrent update,
  delete, and vacuum activity.
- In-place replacement now preserves the embedded cell lock byte while copying
  the rest of the archived row.
- Whole-map Arctic destruction uses an unordered physical drain instead of
  repeatedly searching for the next logical key.
- Persisted primary/secondary index reconstruction and validation failures that
  could otherwise expose missing, duplicate, or mismatched rows.

## [0.4.1]

### Added

- add ability to choose index type in `worktable!` declaration.
- added `index_set` and `tree_index` features to use index type as default in declaration.
-

### BC Breaks

- `.wt` files which are generated now have names as snake-case of table's name.
- `new` function now has only `DatabaseManager` as argument.

### Fixed

- `new` function generated if `persist: true` now is public.
- Bugs with insets and deletes after table load from file.

## [0.4.0]

### Added

- `SelectQueryBuilder` object that is used to customize `select_all` query. It has `limit` and `order_by` methods that
  can be used to limit returned row's count. `order_by` has not full functionality and is only available for indexed
  columns
  and only `Oreder::Asc`.
- `SelectResult` object with is partially same to `SelectQueryBuilder`. It allows to limit/order returned rows. Both
  `Oreder::Asc` and `Oreder::Desc` are available. No issues with not indexed columns.
- added `offset` for `SelectQueryBuilder` and `SelectResult`.
- added `optional` column attribute instead of explicit `Option` type declaration.
- support for enums in queries
- Added generation of `Space` object that represents file that stores table's data.
- Added `DatbaseManager` object that is used to control multiple tables.
- Added methods for `Worktables` to use data in files. `persist` is used to save data to file. `load_from_file` is
  used to load table from file.

### BC Breaks

- `select_all` now returns `SelectQueryBuilder` instead of `Vec<Row>`. To have same functionality old `select_all` users
  must call `execute` on returned builder.
- `select_by_{}` now returns `SelectResult` instead of `Vec<Row>`. To have same functionality old `select_all` users
  must call `execute` on returned builder.

## [0.3.10]

### BC Breaks

- Users don't need to define `<{ TestRow::ROW_SIZE }>` for `insert`, `update` and `upsert`.

### Added

- Support for `Option` types in columns.
- Support of `delete` queries.

### Fixed

- `Clippy` errors in macro declaration about unused `Result`'s.
