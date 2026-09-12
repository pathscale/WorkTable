// DRAFT. Not wired into tests/ui.rs. The parser lane that lands `runtime:`
// adds it and generates the .stderr.
//
// Rule (contract section 7): `RuntimeBackend::Tokio` carries no flavor, so
// `tokio(spread)` is refused rather than having its parenthesised part
// dropped. Dropping it would accept a declaration that means something the
// table cannot do.
use worktable::worktable;

worktable! {
    name: TokioWithFlavor,
    persist: false,
    runtime: tokio(spread),
    columns: {
        id: u64 primary_key autoincrement,
        value: u64,
    },
}

fn main() {}
