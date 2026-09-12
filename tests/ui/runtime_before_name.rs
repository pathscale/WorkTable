// DRAFT. Not wired into tests/ui.rs. The parser lane that lands `runtime:`
// adds it there and blesses the .stderr.
//
// Rule (contract section 2): `name`, `version`, `persist` and `partition_by`
// are a fixed ordered prefix, and the free-order section loop starts after
// them. So `runtime` cannot precede `name`.
//
// NOTE for whoever wires this up. This is the one draft whose current message
// is already the right verdict: the ordered prefix reads the first identifier,
// finds it is not `name`, and says
//
//     Expected `name` field. `WorkTable` name must be specified
//
// which is correct and confusing at once. It names the field that is missing
// rather than the one that is in the wrong place, so a reader who put
// `runtime` first has to work out that `runtime` is legal but not here.
// Whether to special-case it is a judgement call for the parser lane, not
// something this test decides. Bless whichever message that lane settles on.
use worktable::worktable;

worktable! {
    runtime: nagoya,
    name: RuntimeBeforeName,
    persist: false,
    columns: {
        id: u64 primary_key autoincrement,
        value: u64,
    },
}

fn main() {}
