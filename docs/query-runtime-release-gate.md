# Per-query runtime scheduling release gate

The table-level runtime registry and its Nagoya/Tokio primitives execute real work. Per-query selection has a separate implementation gap: SelectQueryBuilder::runtime stores QueryParams::tuning, while all three generated select executors ignore that field. The update, delete and in-place section profile identifiers are parsed into the DSL model but are not used by the operation generators. Generated rows also lack the TableRuntime/RuntimeUnpinned implementations required by the builder method; the profile tests supply those implementations on a hand-written Trade row. Those tests check metadata and compile-time bounds, not generated-table execution or worker identity.

This blocks any release claim that per-query profiles schedule work. The canonical guide now states this limitation. It does not change the settled grammar.

A safe implementation needs an ownership boundary. Synchronous execute accepts iterators and predicates borrowing caller state; moving them into a detached pool with a forged lifetime is not acceptable. Owned asynchronous execution can retain the current synchronous API and add an explicit async callsite. Annotated mutations would need an owned table handle and Send/static captures, or a separately proven scoped execution facility. Nagoya does not currently provide a supported borrowed scope. Its old scoped-fork experiment has independent panic and progress defects documented in that repository.

The alternative alpha scope is to reject unsupported per-query execution requests explicitly, retain their schema representation, and ship the working table-level runtime selection. The owner is deciding between these callsite/scope options. Neither option introduces grammar.

Completion evidence must include execution on the selected worker pool, same-pool nested progress with a saturated small pool, cancellation and panic behavior, default/no-default compilation, and a benchmark separating data materialization from scheduling and execution. Metadata-only tests are insufficient.
