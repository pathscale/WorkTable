#!/bin/sh
# Canonical Typst sources; PDFs are local build artifacts.
set -eu
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
typst compile docs/wt-user-guide.typ docs/wt-user-guide.pdf
typst compile docs/why-worktables.typ docs/why-worktables.pdf
