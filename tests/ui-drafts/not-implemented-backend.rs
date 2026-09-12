// Must fail at macro expansion with a message that names the
// backend, says it is not implemented and lists what is. The parser stops at
// the first bad entry, so only `forte` is reported; `blocking` and `bwos` need
// their own cases if each message is to be asserted.
//
//   runtime backend `forte` is not implemented; the backends that are: nagoya, tokio
//
// Rejected rather than accepted inert: a declaration that reads as if it
// selected something either did or failed to build. `forte`, `blocking` and
// `bwos` are a string list in the parser, not enum variants, and exist only so
// this message can be written.

use worktable::runtimes;

runtimes! {
    a: forte,
    b: blocking,
    c: bwos,
}

fn main() {}
