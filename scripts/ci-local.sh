#!/usr/bin/env bash
# Run what CI runs, with the same arguments, in the same order.
#
# This exists because "clippy is clean" locally once meant `cargo clippy
# --all-targets`, which does not include `worktable_codegen` and does not deny
# warnings. Master went red on two lints that had never been run. If you change
# .github/workflows/rust.yml, change this too.
set -uo pipefail

FEATURES=("" "--features versioned-row-publication" "--all-features")
CLIPPY_FEATURES=("" "--all-features")
failed=()

run() {
    echo "--- $* ---"
    if ! "$@"; then
        failed+=("$*")
    fi
}

echo "=== fmt ==="
run cargo fmt --all --check

for args in "${FEATURES[@]}"; do
    echo "=== build and test ${args:-(default)} ==="
    # shellcheck disable=SC2086
    run cargo build --workspace --all-targets $args
    # shellcheck disable=SC2086
    run cargo test --workspace --all-targets $args
done

for args in "${CLIPPY_FEATURES[@]}"; do
    echo "=== clippy ${args:-(default)} ==="
    # shellcheck disable=SC2086
    run cargo clippy --workspace --all-targets $args -- -D warnings
done

echo
if [ ${#failed[@]} -eq 0 ]; then
    echo "all CI jobs passed"
    exit 0
fi
echo "FAILED (${#failed[@]}):"
printf '  %s\n' "${failed[@]}"
exit 1
