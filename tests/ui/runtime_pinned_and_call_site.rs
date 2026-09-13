// DRAFT. Not wired into tests/ui.rs. The lane that lands the `Profile` marker
// and the call-site builder adds it and generates the .stderr.
//
// Rule (contract section 7, last row): a query whose section is annotated has
// its runtime pinned by the schema, so a call-site `.runtime()` on it is an
// error.
//
// This is the case the whole harness is for. The rule is implemented as an
// unsatisfiable bound carrying `#[diagnostic::on_unimplemented]`, never by
// omitting the method: omitting it yields "no method named `runtime`", which
// points at the call rather than at the schema line that pinned it. Both
// spellings fail to compile, so a test asserting only on failure cannot tell
// them apart. The .stderr has to contain "already has a runtime pinned by the
// schema".
use worktable::prelude::*;
use worktable::worktable;

runtimes! {
    wide: nagoya(spread),
}

worktable! {
    name: PinnedAndCallSite,
    persist: false,
    runtime: nagoya(locality),
    columns: {
        id: u64 primary_key autoincrement,
        qty: u64,
    },
    queries: {
        update runtime wide: {
            Fill(qty) by id,
        }
    },
}

fn main() {
    let table = PinnedAndCallSiteWorkTable::default();
    let _ = table.fill_query().runtime(wide).execute();
}
