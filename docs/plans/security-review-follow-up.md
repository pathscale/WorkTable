# Security review follow-up

This plan preserves review topics from a historical static audit. Its claims
were not runtime-verified and may no longer match the source. Recheck the
current implementation before treating any item as a defect or proposing a
fix. The audit's exploit narratives are intentionally not part of this public
plan.

## Re-review topics

- Concurrent row allocation during vacuum and normal inserts.
- Validation and reclamation rules for links accepted by public read APIs.
- Overlap checks for restored live rows and free ranges.
- Redaction and lifetime of presigned storage URLs in errors.
- Type restrictions and aliasing guarantees for in-place updates.
- Secret redaction in configuration debug output.
- Bounds on persisted index counts, restore allocations, and storage reads.
- Failure handling for corrupt persisted pages and malformed file names.
- Unsafe aliasing and seqlock read/write behavior.
- Full type identity checks in generated inline-type optimizations.
- Workflow action pinning and publish-token exposure.

## Review method and disposition

For each topic, record whether the current source still has the behavior,
whether it is reachable under supported APIs and inputs, and what evidence
supports that conclusion. Keep static source reasoning separate from model,
runtime, and build checks. Any confirmed issue gets a narrowly scoped source
change and a focused check before the topic is closed. Do not treat this list
as a vulnerability count or a current security verdict.

The historical audit left several source areas and downstream dependencies
unreviewed. Recheck its coverage statement before claiming the repository has
received a comprehensive security review.

## Queue dispositions (2026-11-05)

Current-source review integrated four bounded fixes on `master`: append
reservations use one bounded atomic update rather than rollback arithmetic;
`S3Config` debug output omits all credential-bearing configuration values;
ART pair decoding no longer preallocates from an untrusted count; and database
S3 index names reject colon and NUL in addition to path separators. Focused
regressions cover concurrent append bounds, debug redaction, a huge declared
ART pair count with a truncated body, and platform-sensitive index-name
rejection.

The following packet changes were not carried forward as written:

- Persisted live/free interval overlap detection remains required, but the
  candidate duplicates every free interval and sorts the combined set during
  load. Define a bounded/incremental audit that first validates positive
  lengths, page existence, and in-page bounds, then proves free ranges cannot
  alias live rows without unbounded extra load-time allocation.
- Presigned S3 request errors need redaction that retains safe status/source
  context and covers the full response-body and parse error chain. Do not adopt
  a fixed 60-second expiry without evidence that supported transfers and retry
  behavior fit within it.
- Blanket rejection of all `update_in_place` queries is a broad compatibility
  break without a demonstrated archived-layout contract. Specify which
  callback types are sound and verify both in-memory and persisted generation
  before narrowing the public API.
- Removing all token-spelling gates avoids false type identity but also removes
  safe scalar optimizations and can alter generated APIs. Replace name-based
  classification with compiler/type metadata or a narrowly proven opt-in;
  preserve the existing supported paths.

The `save_raw_row` API also still accepts an empty byte slice even though
zero-length links fail `validate_link`; define whether empty rows are forbidden
and make allocation and read behavior agree before treating this boundary as
closed. These dispositions are source review, not a full security verdict.
