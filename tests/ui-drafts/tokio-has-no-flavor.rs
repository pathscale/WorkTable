// Must fail at macro expansion with:
//
//   tokio has no flavors; write `tokio`. Flavors belong to nagoya, whose pool they tune
//
// The span must be on `spread`, not on `tokio`: the backend is fine and the
// flavor is the part to delete.

use worktable::runtimes;

runtimes! {
    p: tokio(spread),
}

fn main() {}
