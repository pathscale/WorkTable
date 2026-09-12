// Must fail at macro expansion with two spans, the second declaration first:
//
//   duplicate runtime profile `wide`
//   `wide` was already declared here
//
// A profile name is the whole of the call-site surface, so two of them is an
// ambiguity rather than a last-one-wins.

use worktable::runtimes;

runtimes! {
    wide: nagoya(spread),
    wide: tokio,
}

fn main() {}
