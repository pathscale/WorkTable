#!/usr/bin/env bash
# Compile-time and binary-size cost of specialization (paper §5).
# Builds tiny crates with 1 and 8 worktable! tables, times cold release builds,
# reports binary sizes. Run from paper-bench/ inside the WorkTable clone.
set -euo pipefail

WT_PATH="$(cd "$(dirname "$0")/../.." && pwd)"   # the WorkTable clone root
WORK=$(mktemp -d)
echo "bench,tables,cold_build_secs,binary_bytes"

gen_crate() {
  local n=$1 dir=$2
  mkdir -p "$dir/src"
  cat > "$dir/Cargo.toml" <<EOF
[package]
name = "wt-cost-$n"
version = "0.1.0"
edition = "2024"
[workspace]
[dependencies]
worktable = { path = "$WT_PATH" }
tokio = { version = "1", features = ["full"] }
[profile.release]
lto = "fat"
codegen-units = 1
EOF
  {
    echo 'use worktable::prelude::*; use worktable::worktable;'
    for i in $(seq 1 "$n"); do
      cat <<EOF
worktable!(
    name: Cost$i,
    columns: {
        id: u64 primary_key autoincrement,
        a: u64,
        b: u64,
        c: f64,
        d: String,
    },
    indexes: { a_idx_$i: a, },
    queries: {
        update: { UpdA$i(a) by id, },
        in_place: { IncB$i(b) by id, }
    }
);
EOF
    done
    echo 'fn main() {'
    for i in $(seq 1 "$n"); do
      echo "  let t$i = Cost${i}WorkTable::default(); t$i.insert(Cost${i}Row{ id: t$i.get_next_pk().into(), a:1, b:1, c:1.0, d:\"x\".into() }).unwrap();"
    done
    echo '  println!("ok");'
    echo '}'
  } > "$dir/src/main.rs"
}

for n in 1 8; do
  dir="$WORK/cost-$n"
  gen_crate "$n" "$dir"
  ( cd "$dir" && cargo fetch >/dev/null 2>&1 )
  start=$(date +%s.%N)
  ( cd "$dir" && cargo build --release >/dev/null 2>&1 )
  end=$(date +%s.%N)
  bin=$(find "$dir/target/release" -maxdepth 1 -type f -perm -111 -name "wt-cost-*" | head -1)
  size=$(stat -f%z "$bin" 2>/dev/null || stat -c%s "$bin")
  echo "compile_cost,$n,$(echo "$end $start" | awk '{printf "%.1f", $1-$2}'),$size"
done

rm -rf "$WORK"
