# Validation without default features

WorkTable must preserve compilation of the library and generated in-memory
callsites with `default-features = false`. CI and `scripts/ci-local.sh` check
both the library and the isolated `tests/nostd-consumer` crate, and deny
warnings for the library. The consumer is outside the workspace so other
workspace packages cannot silently enable WorkTable's `std` feature.

This is not yet proof that WorkTable's entire linked dependency closure is
free of the standard library. Comparing PR head
`6f0f6c5cdcf0c27689c050f67e92d68e1e36c2f3` with the v3 implementation found
the same inherited `std` feature paths: fastrand, the psc-nanoid random-number
and archive dependencies, uuid, eyre's once_cell, and the DSL's indexmap.
DataBucket itself also still uses the standard library. These are existing
limitations, not evidence of an entirely freestanding build. Proc macros
execute on the build host and must be distinguished from runtime dependencies.

The FairMutex restoration in parking_lot_lite_hack 0.12.8 has stronger
coverage: its normal dependency graph enables no `std` feature with defaults
disabled, its eleven FairMutex tests and five backend tests pass, and the
library compiles with `arc_lock,send_guard` on macOS ARM64, Windows GNU x64
and Linux musl ARM64. The musl build retains two libc deprecation warnings
in the existing Linux thread parker.

The v3 CRC dependency has default features disabled. Row-directory allocation
uses `Vec`; allocation is already part of the portable in-memory API. Hosted
persistence and runtime thread creation remain behind WorkTable's `std`
feature. The new persistence-only mutation helper is also gated so it does
not create a dead-code warning in a build without that feature.
