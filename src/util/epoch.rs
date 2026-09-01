//! Per-table grace-period domains.
//!
//! A thin adapter over [`ps_reclaim`], which owns the implementation, its
//! contract tests and its benchmarks. Nothing about reclamation is decided
//! here; this exists so the rest of the crate keeps one import path and so the
//! reasoning for the choice has somewhere to live.
//!
//! # Why not `crossbeam-epoch`
//!
//! It was, behind a hand-written per-thread handle cache: a `thread_local!`, a
//! `RefCell::borrow_mut`, a linear scan for the domain and a move-to-front
//! write, all in front of the pin. Two separate costs.
//!
//! The wrapper was avoidable. The degradation was not: `crossbeam` runs a
//! global collect every 128 pins, and that walk grows with the reader count,
//! so a pin went from 1.92 ns at one reader to 9.00 ns at eight. `select`
//! takes a read guard, so every point read on a hot table paid it, and
//! `partition_ref` went from 0.71 ns unpinned to 9.79 ns at eight readers.
//!
//! # Why not `seize`
//!
//! Cheaper than `crossbeam` and flat, but it reclaims only when no reader at
//! all is live. `select` holds a read guard, so under continuous read traffic
//! that instant never arrives and retired links, pages and publications queue
//! forever. That is the property `reclamation_progresses_under_continuous_reader_overlap`
//! in `in_memory::pages` asserts, and it is the reason the global reader
//! counter was replaced in the first place.
//!
//! `arctic` reclaims through `seize` and is right to: a trie with short reads
//! reaches quiescence constantly. This does not.
//!
//! # crossbeam is still in the tree
//!
//! This removes it from WorkTable's own reclamation, not from the build. It
//! still arrives three ways: `congee-wt` depends on `crossbeam-epoch` directly
//! and re-exports its `Guard`, which `src/index/congee.rs` names; and
//! `crossbeam-skiplist` comes in under both `WorkTablesIndex` and `indexset`.
//! Say "no crossbeam in the reclamation path", not "no crossbeam", until
//! `congee-wt` is ported. See `docs/TODO.md`.

pub(crate) use ps_reclaim::{Domain as EpochDomain, Guard};
