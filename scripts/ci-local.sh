#!/bin/sh
# Run what CI runs, with the same arguments, in the same order.
#
# This exists because "clippy is clean" locally once meant `cargo clippy
# --all-targets`, which does not include `worktable_codegen` and does not deny
# warnings. Master went red on two lints that had never been run. If you change
# .github/workflows/rust.yml, change this too.
#
# POSIX sh: no arrays, no [[ ]], no pipefail. The matrix is written out rather
# than looped so that each job's arguments are the literal ones CI passes.
set -u

fail_count=0
failed=""

run() {
    echo "--- $* ---"
    if "$@"; then
        return 0
    fi
    fail_count=$((fail_count + 1))
    failed="${failed}  $*
"
}

echo "=== fmt ==="
run cargo fmt --all --check

echo "=== build and test (default) ==="
run cargo build --workspace --all-targets
run cargo test --workspace --all-targets

echo "=== build and test (versioned-row-publication) ==="
run cargo build --workspace --all-targets --features versioned-row-publication
run cargo test --workspace --all-targets --features versioned-row-publication

echo "=== build and test (all-features) ==="
run cargo build --workspace --all-targets --all-features
run cargo test --workspace --all-targets --all-features

echo "=== clippy (default) ==="
run cargo clippy --workspace --all-targets -- -D warnings

echo "=== clippy (all-features) ==="
run cargo clippy --workspace --all-targets --all-features -- -D warnings

echo
if [ "$fail_count" -eq 0 ]; then
    echo "all CI jobs passed"
    exit 0
fi
echo "FAILED ($fail_count):"
printf '%s' "$failed"
exit 1
