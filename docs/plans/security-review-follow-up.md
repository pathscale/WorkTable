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
