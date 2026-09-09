// DRAFT. Not wired into tests/ui.rs. The parser lane that lands `runtime:`
// adds it and generates the .stderr.
//
// Rule (contract section 7): `runtime` is an arm of the free-order section
// loop, so nothing about its position stops it appearing twice. Two of them
// is a duplicate section, and the message says so rather than silently
// keeping the last.
use worktable::worktable;

worktable! {
    name: RuntimeTwice,
    persist: false,
    runtime: nagoya(locality),
    runtime: tokio,
    columns: {
        id: u64 primary_key autoincrement,
        value: u64,
    },
}

fn main() {}
