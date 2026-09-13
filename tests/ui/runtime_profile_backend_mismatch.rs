// DRAFT. Not wired into tests/ui.rs. The lane that lands the `Profile` marker
// adds it and generates the .stderr.
//
// Rule (contract section 6): `.runtime()` and the section annotation take
// `P: Profile<Backend = Self::Backend>`, so naming a `tokio` profile on a
// table declared `runtime: nagoya` fails as a bound that names both backends.
// The message has to carry both names; a bare "trait bound not satisfied" is
// the regression this case exists to catch.
use worktable::prelude::*;
use worktable::worktable;

runtimes! {
    tokio_max:  tokio,
    fast_local: nagoya(locality),
}

worktable! {
    name: BackendMismatch,
    persist: false,
    runtime: nagoya(locality),
    columns: {
        id: u64 primary_key autoincrement,
        qty: u64,
    },
    queries: {
        update runtime tokio_max: {
            Fill(qty) by id,
        }
    },
}

fn main() {}
