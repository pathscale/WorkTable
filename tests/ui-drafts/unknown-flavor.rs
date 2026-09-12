// Must fail at macro expansion, listing the three flavors:
//
//   unknown nagoya flavor `banana`; expected one of: locality, spread, throughput

use worktable::runtimes;

runtimes! {
    p: nagoya(banana),
}

fn main() {}
