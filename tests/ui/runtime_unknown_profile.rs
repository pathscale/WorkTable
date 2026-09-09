// DRAFT. Not wired into tests/ui.rs. The lane that lands section annotations
// adds it and generates the .stderr.
//
// Rule (contract section 2): the token after `runtime` at a section is a
// profile name declared by `runtimes!`, never a backend literal. `nope` is not
// one, and the message names it rather than reporting a parse failure at the
// colon.
use worktable::prelude::*;
use worktable::worktable;

runtimes! {
    tokio_max:  tokio,
    fast_local: nagoya(locality),
}

worktable! {
    name: UnknownProfile,
    persist: false,
    runtime: nagoya(locality),
    columns: {
        id: u64 primary_key autoincrement,
        qty: u64,
    },
    queries: {
        update runtime nope: {
            Fill(qty) by id,
        }
    },
}

fn main() {}
