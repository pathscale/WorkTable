// Must fail with an arity error on `.runtime()`:
//
//   this method takes 1 argument but 2 arguments were supplied
//
// `.runtime()` takes a profile and nothing else. Every distinct
// parameterisation is a distinct thread pool, so free-form numbers here would
// mean a pool set nobody can enumerate; with names only, every pool the process
// will ever create is visible by reading one `runtimes!` block. A knob added
// later arrives as a further builder link, `.runtime(wide).workers(12)`, never
// as a second argument, because an arity change breaks every existing call.

use worktable::prelude::*;
use worktable::runtimes;

runtimes! {
    wide: nagoya(spread),
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
    let _ = SelectQueryBuilder::<Trade, _, (), ()>::new(rows.into_iter()).runtime(wide, 12);
}
