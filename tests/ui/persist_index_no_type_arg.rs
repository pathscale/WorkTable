// Rule: `#[derive(PersistIndex)]` requires the first generic argument of each
// field type to be the indexed key type, not a lifetime.
use worktable_codegen::PersistIndex;

#[derive(PersistIndex)]
struct NoTypeArgIndex {
    by_id: Holder<'static>,
}

fn main() {}
