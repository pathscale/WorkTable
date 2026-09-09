// Must fail at macro expansion with:
//
//   unknown runtime backend `smol`; expected one of: nagoya, tokio

use worktable::runtimes;

runtimes! {
    p: smol,
}

fn main() {}
