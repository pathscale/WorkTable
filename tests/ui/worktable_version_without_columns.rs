// Rule: `worktable_version!` requires a `columns` block.
use worktable::worktable_version;

worktable_version! {
    name: MissingColumnsVersion,
    version: 1,
}

fn main() {}
