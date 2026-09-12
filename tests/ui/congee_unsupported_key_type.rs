// Rule: congee keys are the unsigned integers its public API accepts. A
// `String` primary key is refused here rather than at the point where the
// generated codec would fail to build.
use worktable::worktable;

worktable! {
    name: CongeeStringKey,
    persist: false,
    columns: {
        id: String primary_key using congee,
        value: u64,
    },
}

fn main() {}
