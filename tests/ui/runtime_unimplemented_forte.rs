// DRAFT. Not wired into tests/ui.rs. The parser lane that lands `runtime:`
// adds it there and blesses the .stderr. Today this fails with "Unexpected
// token `runtime`; expected one of `columns`, `indexes`, `queries`,
// `config`", which is the free-order section loop refusing an arm it does not
// have yet: the right verdict for the wrong reason.
//
// Rule (contract section 7): `forte` is a name the parser recognises only so it
// can refuse it well. Per PR #58, an inert declaration is an error, so this
// must not be accepted and quietly ignored. The message has to name `forte`,
// say it is not implemented, and list the backends that are.
use worktable::worktable;

worktable! {
    name: UnimplementedForte,
    persist: false,
    runtime: forte,
    columns: {
        id: u64 primary_key autoincrement,
        value: u64,
    },
}

fn main() {}
