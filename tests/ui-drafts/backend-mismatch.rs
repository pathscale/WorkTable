// Must fail with E0271, naming both backends:
//
//   type mismatch resolving `<tokio_max as Profile>::Backend == NagoyaRt<Spread>`
//   expected struct `worktable::runtime::NagoyaRt<worktable::runtime::Spread>`
//              found struct `TokioRt`
//
// This one is unconditional and can never be waived: the table's `runtime:`
// selects the RwLock, Notify and JoinHandle it is built from, so nothing at a
// call site can change it.

use worktable::prelude::*;
use worktable::runtimes;

runtimes! {
    tokio_max: tokio,
}

struct Trade {
    id: u64,
}

impl TableRuntime for Trade {
    type Backend = NagoyaRt<Spread>;
}

impl RuntimeUnpinned for Trade {}

fn main() {
    let rows = vec![Trade { id: 1 }];
    let _ = SelectQueryBuilder::<Trade, _, (), ()>::new(rows.into_iter()).runtime(tokio_max);
}
