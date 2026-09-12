# Old page-format fixture

`v2.wt.data` is an actual v2 store retained from the local persistence tests
before the v3 implementation on 2026-09-11. Its source was
`tests/data/unsized_primary_and_other_sync/update_query_pk/test_sync/.wt.data`.
It contains synthetic test rows. Its first header identifies format 2, and
the file includes metadata and row data (16,596 bytes).

This fixture verifies refusal at the format boundary. It is not evidence of
v2-to-v3 conversion support, which is outside this release's runtime contract.
