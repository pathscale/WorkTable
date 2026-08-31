#!/bin/sh
# Publish worktable and worktable_codegen to crates.io if master's version is
# not on the index yet, then tag it.
#
# Driven by whether the version already exists on crates.io rather than by
# diffing Cargo.toml against the previous commit. That makes it idempotent: a
# push that changes nothing publishes nothing, a re-run after a half-finished
# release completes it, and it does not care whether the version bump arrived
# in one commit or ten.
#
# Publishing is irreversible. A version can never be reused and yanking does
# not delete, so every guard below fails closed.
#
# Usage:
#   scripts/release.sh --dry-run   report what would happen, publish nothing
#   scripts/release.sh             publish (needs CARGO_REGISTRY_TOKEN)
set -u

DRY_RUN=0
[ "${1:-}" = "--dry-run" ] && DRY_RUN=1

UA="worktable-release-script"
INDEX_TIMEOUT=300

die() {
    echo "release: $*" >&2
    exit 1
}

# The version field of a Cargo.toml's [package] section: first `version = "..."`
# after the `[package]` line, so a dependency's version cannot be picked up.
package_version() {
    sed -n '/^\[package\]/,/^\[/p' "$1" | sed -n 's/^version = "\([^"]*\)".*/\1/p' | head -1
}

published_versions() {
    curl -sS -H "User-Agent: $UA" "https://crates.io/api/v1/crates/$1/versions" \
        | tr ',' '\n' | sed -n 's/.*"num":"\([^"]*\)".*/\1/p'
}

is_published() {
    published_versions "$1" | grep -qx "$2"
}

WT_VERSION=$(package_version Cargo.toml)
CG_VERSION=$(package_version codegen/Cargo.toml)
PIN=$(sed -n 's/^worktable_codegen = .*version = "=\([^"]*\)".*/\1/p' Cargo.toml)

[ -n "$WT_VERSION" ] || die "could not read the worktable version"
[ -n "$CG_VERSION" ] || die "could not read the worktable_codegen version"
[ -n "$PIN" ] || die "could not read the worktable_codegen pin from Cargo.toml"

echo "worktable          $WT_VERSION"
echo "worktable_codegen  $CG_VERSION"
echo "pin                =$PIN"

# The two crates are released together and the pin is exact, so any
# disagreement means a bump was applied to one and not the others. Publishing
# that would put a permanently broken pairing on the index.
[ "$WT_VERSION" = "$CG_VERSION" ] \
    || die "version mismatch: worktable $WT_VERSION, worktable_codegen $CG_VERSION"
[ "$PIN" = "$CG_VERSION" ] \
    || die "pin mismatch: pin =$PIN, worktable_codegen $CG_VERSION"

CG_NEEDED=0
WT_NEEDED=0
is_published worktable_codegen "$CG_VERSION" || CG_NEEDED=1
is_published worktable "$WT_VERSION" || WT_NEEDED=1

if [ "$CG_NEEDED" -eq 0 ] && [ "$WT_NEEDED" -eq 0 ]; then
    echo "release: $WT_VERSION is already on crates.io, nothing to do"
    exit 0
fi

echo "release: to publish:${CG_NEEDED:+ }$([ $CG_NEEDED -eq 1 ] && echo worktable_codegen)$([ $WT_NEEDED -eq 1 ] && echo ' worktable')"

if [ "$DRY_RUN" -eq 1 ]; then
    echo "release: dry run, verifying the package instead"
    # Only codegen can be verified before it is on the index: worktable pins it
    # exactly, so its own dry run cannot resolve until codegen is published.
    [ "$CG_NEEDED" -eq 1 ] && { cargo publish --dry-run -p worktable_codegen || die "codegen dry run failed"; }
    exit 0
fi

[ -n "${CARGO_REGISTRY_TOKEN:-}" ] || die "CARGO_REGISTRY_TOKEN is not set"

# Order is forced: worktable pins worktable_codegen exactly, so the main crate
# cannot resolve until codegen is on the index.
if [ "$CG_NEEDED" -eq 1 ]; then
    echo "release: publishing worktable_codegen $CG_VERSION"
    cargo publish -p worktable_codegen || die "publishing worktable_codegen failed"

    echo "release: waiting for the index"
    waited=0
    while ! is_published worktable_codegen "$CG_VERSION"; do
        [ "$waited" -ge "$INDEX_TIMEOUT" ] \
            && die "worktable_codegen $CG_VERSION did not appear on the index within ${INDEX_TIMEOUT}s"
        sleep 10
        waited=$((waited + 10))
    done
    echo "release: worktable_codegen $CG_VERSION is on the index"
fi

if [ "$WT_NEEDED" -eq 1 ]; then
    echo "release: publishing worktable $WT_VERSION"
    cargo publish -p worktable || die "publishing worktable failed"
fi

echo "release: published $WT_VERSION"
