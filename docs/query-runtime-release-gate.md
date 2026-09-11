# Owned query runtime execution

The release review found that select profiles only recorded tuning and mutation section profiles were ignored. The implementation now uses an explicit ownership boundary without changing grammar.

- Generated hosted paged rows carry their declared backend and flavor. A named profile must match the backend family. Nagoya profiles may select another flavor without changing the table declaration.
- Synchronous execute remains available. An explicitly selected runtime requires execute_async().await; execute returns RuntimeRequiresAsync rather than ignoring the selection.
- execute_async materializes borrowed iteration and predicates on the caller before constructing the future. Owned range filtering, sorting, offset and limit execute on the selected pool. It defaults to the table's executor, materializes all input rows and does not fan out a query across workers.
- Runtime-annotated update/delete/in-place methods require an Arc table receiver and owned Send/static arguments. Unannotated methods retain their borrowed signatures. Portable table locks and private persistence I/O workers are unchanged.
- Pending owned tasks are cancelled when their waiting future is dropped. Synchronous work already running can complete; cancellation is not rollback. Panics propagate to the caller.
- Vec tables reject query profiles. Without default features, owned select execution remains inline and hosted profile markers are unavailable.

The generated-table tests in tests/runtime_execution.rs verify worker identity, query results with borrowed non-Send predicates, same-pool nesting on one worker, cancellation, panic propagation, persistence/reopen and optional Tokio execution. The wt-owned-runtime benchmark measures full materialization, synchronous versus scheduled sorting, empty dispatch roundtrips and scheduled mutations; it checks equal results. The separate full CI run covers existing callsites, no-default consumers and Clippy.

The old scoped-fork experiment is not used. See Nagoya's deferred-experiments note for its independent panic/progress defects. Canonical user-facing documentation is docs/wt-user-guide.typ.
