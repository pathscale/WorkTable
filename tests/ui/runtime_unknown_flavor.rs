// DRAFT. Not wired into tests/ui.rs. The parser lane that lands `runtime:`
// adds it and generates the .stderr.
//
// Rule (contract section 7): nagoya takes `locality`, `spread` or
// `throughput`. An unknown flavor is refused with the three listed, the same
// way `using` lists the four index backends.
use worktable::worktable;

worktable! {
    name: UnknownFlavor,
    persist: false,
    runtime: nagoya(banana),
    columns: {
        id: u64 primary_key autoincrement,
        value: u64,
    },
}

fn main() {}
