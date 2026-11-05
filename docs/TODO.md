# Open work index

- Strict legacy snapshot adoption is implemented through DataBucket's restore token and `S3SyncDiskPersistenceEngine::from_restored_snapshot`. Callers must name the known source primary-key layout; fallible metadata preflight checks the application version before opening the disk engine. This does not migrate data or certify a deployment. Finish downstream whole-database preflight, physical/application migration verification, dependency publication, and externally enforced writer fencing before enabling the new startup path.

- [Detailed follow-ups](plans/detailed-followups.md): unresolved legacy release-yank decision, safety context, and disposition of the removed TODO items.
- [Deferred queue follow-ups](plans/deferred-queue-follow-ups.md): source requirements and release decision that were tracked only by the retired patch-queue README.
- [Consolidated open work](plans/open-work.md): persistence durability and space-layer follow-through, ART persistence, current paper evidence, and columnar side-index follow-up.
- [On-disk space-layer issues](space-layer-known-issues.md): concrete persistence behavior and efficiency concerns.
- [ART persistence plan](art-index-persistence-plan.md): design gates and remaining production work.
- [Columnar side-index plan](columnar-index-plan.md): implementation boundary and scoped follow-ups.
- [Paper two plan](paper-2-plan.md): thesis, evidence requirements, and technical gaps; refresh time-sensitive status before relying on it.
- [Security review follow-up](plans/security-review-follow-up.md): recheck historical static-review topics against current source; no finding is assumed confirmed.
- [Queue dispositions](plans/security-review-follow-up.md#queue-dispositions-2026-11-05): four narrow source fixes landed; unresolved overlap, S3 error-redaction, mutation-safety, and codegen type-identity work remains explicitly tracked.
- [Historical root release requirements](plans/historical-root-requirements.md): reconcile reclamation/unload/accounting, vacuum, backend migration, malformed-input diagnostics and application acceptance against current source; historical notes do not establish defects or completion.

## Pending schema-name release verification

- [x] Re-run the concrete S3 adoption regression and consumer whole-database preservation regression after schema-name initialization and adapter forwarding changes. Both passed on 2026-11-06 against WorkTable source `74ffd6c` and local codegen 2.0.0. The offline no-default-features check also passed. This verifies the previously failing new-store name preservation, not migration of existing stores or deployed S3 acceptance.
- [ ] Validate default/std/no-std and S3 builds plus package publication. The generated schema-name hook requires codegen 2.0.0 to avoid breaking older WorkTable consumers that accept codegen 1.x. Neither codegen 2 nor WorkTable 1.13.1 is published yet.
- [ ] Confirm legacy-store migration for preexisting metadata names; do not relax exact schema checks or infer missing tables.

## Verification phase before writable adoption

- Required primary and generated secondary index files are checked before opening restored snapshots for writes. Implementations without an explicit file inventory refuse adoption. The focused adoption regression passed locally: genuine manifests missing either index were rejected without changing local names/bytes or remote request counts; valid adoption and the next persisted mutation passed.
- The existing detect_version API keeps its previous generic bounds; strict migration/adoption continues through detect_source_version. Release-wide compatibility, actual downstream migration, and deployment validation remain separate gates.
