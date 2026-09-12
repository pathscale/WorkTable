// Must fail with "no method named `runtime` found for enum `Option`", on the
// `select(pk)` line only. The `select_all()` line above it must compile.
//
// `.runtime()` is on the builder-returning selects, `select_all` and
// `select_by_pk_range`. `select(pk)` returns `Option<TradeRow>` rather than a
// builder, so it has no `.runtime()`, and this is the one place where a missing
// method is the right message: there is no builder to pin.
//
// The reason is measured. A spawn is 21 ns and the wake that follows it about
// 2,250 ns at the median, against roughly 400 ns for a point read, so the hop
// costs several times the operation. `.runtime()` is for work already measured
// in microseconds.
//
// Needs a real generated table and a `TableRuntime` impl for its row type, so
// this case waits on the codegen lane; the other drafts hand-build the builder.

use worktable::prelude::*;
use worktable::{runtimes, worktable};

runtimes! {
    wide: nagoya(spread),
}

worktable! (
    name: Trade,
    columns: {
        id: u64 primary_key,
        qty: u64,
    }
);

fn main() {
    let table = TradeWorkTable::default();

    // Fine: a builder.
    let _ = table.select_all().limit(10).runtime(wide);

    // Not fine: a row.
    let _ = table.select(1u64).runtime(wide);
}
