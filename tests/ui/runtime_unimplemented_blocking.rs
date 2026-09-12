// DRAFT. Not wired into tests/ui.rs. The parser lane that lands `runtime:`
// adds it there and blesses the .stderr. Today this fails with "Unexpected
// token `runtime`; expected one of `columns`, `indexes`, `queries`,
// `config`", which is the free-order section loop refusing an arm it does not
// have yet: the right verdict for the wrong reason.
//
// Rule (contract section 7): `blocking` is a name the parser recognises only so it
// can refuse it well. Per PR #58, an inert declaration is an error, so this
// must not be accepted and quietly ignored. The message has to name `blocking`,
// say it is not implemented, and list the backends that are.
use worktable::worktable;

worktable! {
    name: UnimplementedBlocking,
    persist: false,
    runtime: blocking,
    columns: {
        id: u64 primary_key autoincrement,
        value: u64,
    },
}

fn main() {}
