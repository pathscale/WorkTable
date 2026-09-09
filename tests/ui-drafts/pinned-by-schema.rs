// Must fail with E0277 and the `#[diagnostic::on_unimplemented]` text:
//
//   error[E0277]: `Ledger` already has a runtime pinned by the schema
//      |
//      |     let _ = builder.runtime(wide);
//      |                     ^^^^^^^ remove this `.runtime()`, or remove `runtime` from the section
//
// It must NOT be "no method named `runtime` found for struct
// `SelectQueryBuilder`", which is what omitting the method would give and which
// points at the builder rather than at the two declarations that disagree.
//
// `Ledger` stands in for a table whose `select` section is annotated
// `runtime wide:`: generated code emits its `TableRuntime` impl and withholds
// the `RuntimeUnpinned` one.

use worktable::prelude::*;
use worktable::runtimes;

runtimes! {
    wide: nagoya(spread),
}

struct Ledger {
    id: u64,
}

impl TableRuntime for Ledger {
    type Backend = NagoyaRt<Spread>;
}

fn main() {
    let rows = vec![Ledger { id: 1 }];
    let builder = SelectQueryBuilder::<Ledger, _, (), ()>::new(rows.into_iter());
    let _ = builder.runtime(wide);
}
