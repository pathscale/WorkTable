//! Pairs index change event id *assignment* with event *queueing*, so that a
//! persistence stall on an event gap names its own cause instead of only its
//! symptom.
//!
//! # The defect this exists for
//!
//! `BatchOperation::validate` refuses to apply an event stream with a hole in
//! it (see the guard there and commit `c0c06ba`). A hole is normally
//! transient: the operation carrying the missing id has been produced but not
//! yet batched. A hole that survives the whole deferral budget means something
//! else, and the old message could not tell the two apart. It reported the
//! range and nothing more:
//!
//! ```text
//! persistence stalled on primary index event gap: last applied Id(1439),
//! next available Id(1455) (attempt 9)
//! ```
//!
//! Two very different bugs produce that line:
//!
//! 1. **A leak.** An id was assigned by the index and its event was then
//!    dropped instead of being pushed onto the persistence queue. Nothing will
//!    ever deliver it and the stream is permanently gapped.
//! 2. **A collection failure.** The operation carrying the id *was* queued and
//!    is still sitting in the analyzer, but batch collection keeps assembling
//!    batches that exclude it.
//!
//! Distinguishing those needs a record of what was queued, which is what this
//! ledger keeps. It is deliberately not a fix for either: it only observes.
//!
//! # Where the two sides are observed
//!
//! Assignment happens inside the index (`indexset` bumps an `AtomicU64` and
//! stamps the event in the same commit), which this crate cannot hook. What it
//! *can* hook is the other end: every event that reaches persistence passes
//! through [`crate::persistence::task::Queue`], so an id present in the stream
//! but absent from this ledger was assigned and never queued. That is the leak
//! signature, and it is what [`EventLedger::gap_report`] reports.
//!
//! The producer is named by [`std::panic::Location`], captured with
//! `#[track_caller]` at the queue push, so a leak points at the call site that
//! produced its neighbours: the generated query, `insert_many`, an
//! acknowledge path, or vacuum's `apply_move`. A full backtrace is captured
//! too, but only when `RUST_BACKTRACE` is set; `Backtrace::capture` is
//! essentially free otherwise.
//!
//! # Gating: `debug_assertions`, not a cargo feature
//!
//! Recording is compiled in unconditionally and gated at run time by
//! [`enabled`], which is true when `debug_assertions` is on or when
//! `WT_EVENT_LEDGER` is set in the environment.
//!
//! `debug_assertions` was chosen over a new cargo feature for one reason: the
//! stall is only ever observed under a full `cargo test --workspace
//! --all-targets --all-features` run, and that run is a debug build, so the
//! instrumentation is on exactly where the bug appears. A feature would also
//! have been enabled by `--all-features`, but it would have to be declared in
//! `Cargo.toml`, and a diagnostic that lives behind a flag nobody sets in the
//! failing configuration is worthless. Release builds fold `enabled()` down
//! to the environment check and every recording call returns immediately, so
//! they pay a predictable-branch and nothing else.
//!
//! `WT_EVENT_LEDGER=1` exists so a release build can be told to record without
//! being rebuilt, for the day the stall shows up outside a test run.
//!
//! Memory is bounded by [`WINDOW`] ids per stream. The gap sits at the head of
//! the stream by construction, so a recent window always covers it; the report
//! states the window it holds so a reader can see that for themselves.

// The ledger itself is arithmetic and bookkeeping, so it takes `core` and
// `alloc`. Two of its capabilities genuinely need an operating system, and only
// those are gated: `Backtrace`, and reading `WT_EVENT_LEDGER` from the
// environment. Without `std` the ledger is simply never enabled, which is the
// right answer for a diagnostic that an environment variable turns on.
use alloc::borrow::ToOwned as _;
// Only the gated backtrace field boxes anything.
#[cfg(feature = "std")]
use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::string::{String, ToString as _};
use alloc::vec::Vec;
use core::fmt::Write as _;
use core::panic::Location;
use hashbrown::HashMap;
#[cfg(feature = "std")]
use std::backtrace::Backtrace;
#[cfg(feature = "std")]
use std::sync::LazyLock;

use data_bucket::Link;
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;
use parking_lot::Mutex;

use crate::persistence::{OperationId, OperationType};

/// Ids retained per stream. The gap the guard reports is at the head of the
/// stream, so a window this size covers it many times over: the one observed
/// stall was 16 ids wide.
const WINDOW: usize = 8192;

/// Gap ids listed individually in a report before it summarises the rest.
const MAX_LISTED_GAP_IDS: usize = 64;

#[cfg(feature = "std")]
static ENABLED: LazyLock<bool> = LazyLock::new(|| {
    if cfg!(debug_assertions) {
        return true;
    }
    match std::env::var("WT_EVENT_LEDGER") {
        Ok(value) => !value.is_empty() && value != "0" && !value.eq_ignore_ascii_case("false"),
        Err(_) => false,
    }
});

/// Without `std` there is no environment to read the switch from, so the
/// ledger stays off. Everything below still compiles and can be driven
/// directly by a caller that wants it; what is missing is the ambient way to
/// turn it on.
#[cfg(not(feature = "std"))]
static ENABLED: bool = false;

/// Whether event bookkeeping is recording in this process.
///
/// See the module comment for why this is `debug_assertions` plus an
/// environment override rather than a cargo feature.
#[inline]
pub fn enabled() -> bool {
    #[cfg(feature = "std")]
    {
        *ENABLED
    }
    #[cfg(not(feature = "std"))]
    {
        ENABLED
    }
}

/// Which index's event id sequence a record belongs to.
///
/// Every index keeps its own counter, so ids only mean anything relative to a
/// stream. `Primary` allocates nothing, which keeps the hot path free of
/// allocation; secondary streams are labelled by the `Debug` rendering of the
/// table's `AvailableIndexes` value, which is the only name available to
/// non-generic code here.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum EventStream {
    Primary,
    Secondary(String),
}

impl core::fmt::Display for EventStream {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            EventStream::Primary => f.write_str("primary"),
            EventStream::Secondary(index) => write!(f, "secondary {index}"),
        }
    }
}

/// What has been observed happening to one event id.
///
/// Flags, not a state machine: an id is queued, then collected into a batch,
/// then possibly trimmed back out and requeued, possibly several times over.
/// The report reads the whole set.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Stages(u8);

impl Stages {
    /// Pushed onto the persistence queue by a producer.
    pub const QUEUED: Stages = Stages(1 << 0);
    /// Pulled into a `BatchOperation` by the analyzer.
    pub const COLLECTED: Stages = Stages(1 << 1);
    /// Handed back to the analyzer's queue after a deferral or a trim.
    pub const REQUEUED: Stages = Stages(1 << 2);
    /// Removed from a batch by `remove_operations_from_events`.
    pub const TRIMMED: Stages = Stages(1 << 3);
    /// Covered by the applied watermark reported after a batch.
    pub const APPLIED: Stages = Stages(1 << 4);

    fn insert(&mut self, other: Stages) {
        self.0 |= other.0;
    }

    /// Both flag sets at once.
    pub const fn union(self, other: Stages) -> Stages {
        Stages(self.0 | other.0)
    }

    fn contains(self, other: Stages) -> bool {
        self.0 & other.0 == other.0
    }
}

impl core::fmt::Display for Stages {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let mut first = true;
        for (flag, name) in [
            (Stages::QUEUED, "queued"),
            (Stages::COLLECTED, "collected"),
            (Stages::REQUEUED, "requeued"),
            (Stages::TRIMMED, "trimmed"),
            (Stages::APPLIED, "applied"),
        ] {
            if self.contains(flag) {
                if !first {
                    f.write_str("+")?;
                }
                f.write_str(name)?;
                first = false;
            }
        }
        if first {
            f.write_str("none")?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct IdRecord {
    stages: Stages,
    /// The operation that carried this id when it was first queued.
    op_id: Option<OperationId>,
    op_type: Option<OperationType>,
    /// Producer call site, from `#[track_caller]` at the queue push.
    site: Option<&'static Location<'static>>,
    /// Captured only when `RUST_BACKTRACE` is set; otherwise `Disabled` and
    /// free. Boxed so that the common empty case costs a pointer instead of an
    /// inline `Backtrace` in each of the thousands of records held per stream.
    // Gated with the capture below: without `std` there is no `Backtrace`,
    // and the ledger keeps the call site from `#[track_caller]` regardless,
    // which is the half that names the producer.
    #[cfg(feature = "std")]
    backtrace: Option<Box<Backtrace>>,
    collected: u32,
    requeued: u32,
}

impl IdRecord {
    fn new() -> Self {
        Self {
            stages: Stages::default(),
            op_id: None,
            op_type: None,
            site: None,
            #[cfg(feature = "std")]
            backtrace: None,
            collected: 0,
            requeued: 0,
        }
    }
}

#[derive(Debug, Default)]
struct StreamLedger {
    ids: BTreeMap<u64, IdRecord>,
    /// Ids below this were evicted by the window and cannot be answered for.
    evicted_below: u64,
    /// Highest id ever seen at any stage on this stream.
    highest_seen: u64,
    /// Highest id the analyzer reported as applied.
    applied_upto: u64,
    queued_total: u64,
}

impl StreamLedger {
    fn entry(&mut self, id: u64) -> &mut IdRecord {
        if id > self.highest_seen {
            self.highest_seen = id;
        }
        self.ids.entry(id).or_insert_with(IdRecord::new)
    }

    fn trim(&mut self) {
        while self.ids.len() > WINDOW {
            // Ids are close to monotonic, so the lowest key is the oldest.
            if let Some((id, _)) = self.ids.pop_first() {
                self.evicted_below = self.evicted_below.max(id + 1);
            } else {
                break;
            }
        }
    }

    /// Lowest id this ledger can still answer for.
    fn window_start(&self) -> u64 {
        self.ids.keys().next().copied().unwrap_or(self.evicted_below)
    }
}

/// Per-table record of which index change event ids reached the persistence
/// queue, and what happened to them afterwards.
///
/// Shared by `Arc` between the queue (the producer side), the analyzer, and
/// the `BatchOperation` whose guard reads it.
#[derive(Debug)]
pub struct EventLedger {
    label: String,
    /// True when no producer writes to this ledger, so "never queued" here
    /// means "never recorded", not "leaked". Reports say so rather than
    /// accusing a producer that was never watched.
    detached: bool,
    streams: Mutex<HashMap<EventStream, StreamLedger>>,
}

impl EventLedger {
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            detached: false,
            streams: Mutex::new(HashMap::new()),
        }
    }

    /// A ledger attached to nothing, for analyzers and `BatchOperation`s built
    /// outside `run_engine` (unit tests, defensive callers). It records
    /// normally; it is simply never shared with a producer, so its reports say
    /// so instead of reading a missing record as a leak.
    pub fn detached() -> Self {
        Self {
            label: "<detached>".to_owned(),
            detached: true,
            streams: Mutex::new(HashMap::new()),
        }
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    /// Collects the event ids of `evs`, for a later [`EventLedger::record_queued`].
    ///
    /// Split from the recording itself because the queue only knows a push was
    /// accepted after the operation has been moved into it, and a refused push
    /// must not be recorded as queued.
    pub fn event_ids<T>(evs: &[ChangeEvent<Pair<T, Link>>]) -> Vec<u64> {
        if !enabled() {
            return Vec::new();
        }
        evs.iter().map(|ev| ev.id().inner()).collect()
    }

    /// Records every id in `ids` as queued by `site`.
    ///
    /// Called from the persistence queue push, which is the single point every
    /// operation passes through on its way to the engine. An id in the applied
    /// stream that never appears here was assigned by the index and dropped
    /// before it reached persistence.
    pub fn record_queued(
        &self,
        stream: EventStream,
        ids: &[u64],
        op_id: OperationId,
        op_type: OperationType,
        site: &'static Location<'static>,
    ) {
        if !enabled() || ids.is_empty() {
            return;
        }
        // Costs nothing unless RUST_BACKTRACE is set: `capture` returns
        // `Disabled` without walking any frames. One capture per push, moved
        // onto the first newly recorded id, because every id in this vector
        // came from the same producer.
        #[cfg(feature = "std")]
        let backtrace = Backtrace::capture();
        #[cfg(feature = "std")]
        let mut backtrace =
            matches!(backtrace.status(), std::backtrace::BacktraceStatus::Captured).then_some(backtrace);
        let mut streams = self.streams.lock();
        let ledger = streams.entry(stream).or_default();
        for id in ids.iter().copied() {
            let record = ledger.entry(id);
            let first_time = !record.stages.contains(Stages::QUEUED);
            record.stages.insert(Stages::QUEUED);
            if first_time {
                record.op_id = Some(op_id);
                record.op_type = Some(op_type);
                record.site = Some(site);
                // One backtrace per push, not per id: every id in this vector
                // has the same producer, and keeping one each would multiply
                // the cost of a `RUST_BACKTRACE` run for no extra signal.
                #[cfg(feature = "std")]
                if record.backtrace.is_none() {
                    record.backtrace = backtrace.take().map(Box::new);
                }
            } else {
                record.stages.insert(Stages::REQUEUED);
                record.requeued = record.requeued.saturating_add(1);
            }
        }
        ledger.queued_total = ledger.queued_total.saturating_add(ids.len() as u64);
        ledger.trim();
    }

    /// Records a stage transition for a single id already known to a stream.
    pub fn record_stage(&self, stream: EventStream, id: u64, stage: Stages) {
        if !enabled() {
            return;
        }
        let mut streams = self.streams.lock();
        let ledger = streams.entry(stream).or_default();
        let record = ledger.entry(id);
        record.stages.insert(stage);
        if stage.contains(Stages::COLLECTED) {
            record.collected = record.collected.saturating_add(1);
        }
        if stage.contains(Stages::REQUEUED) {
            record.requeued = record.requeued.saturating_add(1);
        }
        ledger.trim();
    }

    /// Records a stage transition for every id in `evs`.
    pub fn record_stage_for_events<T>(&self, stream: EventStream, evs: &[ChangeEvent<Pair<T, Link>>], stage: Stages) {
        if !enabled() || evs.is_empty() {
            return;
        }
        let mut streams = self.streams.lock();
        let ledger = streams.entry(stream).or_default();
        for ev in evs {
            let record = ledger.entry(ev.id().inner());
            record.stages.insert(stage);
            if stage.contains(Stages::COLLECTED) {
                record.collected = record.collected.saturating_add(1);
            }
            if stage.contains(Stages::REQUEUED) {
                record.requeued = record.requeued.saturating_add(1);
            }
        }
        ledger.trim();
    }

    /// Records the applied watermark reported after a batch was accepted.
    pub fn record_applied_upto(&self, stream: EventStream, id: u64) {
        if !enabled() || id == 0 {
            return;
        }
        let mut streams = self.streams.lock();
        let ledger = streams.entry(stream).or_default();
        if id > ledger.applied_upto {
            ledger.applied_upto = id;
        }
        if id > ledger.highest_seen {
            ledger.highest_seen = id;
        }
        for (_, record) in ledger.ids.range_mut(..=id) {
            record.stages.insert(Stages::APPLIED);
        }
    }

    /// Explains the gap between `last_applied` and `next_available`.
    ///
    /// This is the whole point of the ledger. For every id in the hole it says
    /// whether the id ever reached the persistence queue, and if it did, what
    /// happened to it afterwards, so the reader can tell a leaked event from a
    /// batch collection that keeps missing one.
    pub fn gap_report(&self, stream: &EventStream, last_applied: u64, next_available: u64) -> String {
        let mut out = String::new();
        if !enabled() {
            let _ = write!(
                out,
                " Event bookkeeping is off in this build, so the gap cannot be attributed. \
                 Re-run with WT_EVENT_LEDGER=1 (and RUST_BACKTRACE=1 for producer backtraces) \
                 to have the next occurrence name its own cause."
            );
            return out;
        }

        if self.detached {
            let _ = write!(
                out,
                " This analyzer holds detached event bookkeeping: no producer ever wrote to it, \
                 so the gap cannot be attributed. Only analyzers built outside `run_engine` are detached."
            );
            return out;
        }

        let streams = self.streams.lock();
        let Some(ledger) = streams.get(stream) else {
            let _ = write!(
                out,
                " Event bookkeeping for table {label} holds no records at all for the {stream} stream, \
                 which should be impossible while it is applying that stream's events.",
                label = self.label,
            );
            return out;
        };

        let window_start = ledger.window_start();
        let first_missing = last_applied.saturating_add(1);
        if next_available <= first_missing {
            return out;
        }

        // Bounded on purpose: this runs while building a panic message, and a
        // corrupt watermark could otherwise make it walk billions of ids.
        let scan_end = next_available.min(first_missing.saturating_add(WINDOW as u64));
        let mut never_queued = Vec::new();
        let mut queued_not_applied = Vec::new();
        for id in first_missing..scan_end {
            match ledger.ids.get(&id) {
                Some(record) if record.stages.contains(Stages::QUEUED) => queued_not_applied.push((id, record)),
                _ => never_queued.push(id),
            }
        }

        let _ = write!(
            out,
            " Bookkeeping for table {label}, {stream} stream: window covers ids {window_start}..={highest}, \
             applied watermark {applied}, {total} id(s) queued in all, \
             {gap_len} id(s) in the gap ({scanned} scanned), {never} never queued, {queued} queued.",
            label = self.label,
            window_start = window_start,
            highest = ledger.highest_seen,
            applied = ledger.applied_upto,
            total = ledger.queued_total,
            gap_len = next_available - first_missing,
            scanned = scan_end - first_missing,
            never = never_queued.len(),
            queued = queued_not_applied.len(),
        );

        if first_missing < window_start {
            let _ = write!(
                out,
                " CAUTION: part of the gap ({first_missing}..{window_start}) fell out of the retained window, \
                 so those ids are unattributable rather than proven missing."
            );
        }

        if !never_queued.is_empty() {
            let _ = write!(out, " ASSIGNED BUT NEVER QUEUED: {}.", format_ids(&never_queued));
            let _ = write!(
                out,
                " Those ids were consumed by the index and their events never reached the persistence queue, \
                 so nothing will ever deliver them: this is an event leak upstream of the analyzer, \
                 not a batch collection problem."
            );
            let _ = write!(out, "{}", bracketing_producers(ledger, last_applied, next_available));
        }

        if !queued_not_applied.is_empty() {
            let _ = write!(out, " QUEUED BUT NOT APPLIED:");
            for (id, record) in queued_not_applied.iter().take(MAX_LISTED_GAP_IDS) {
                let _ = write!(
                    out,
                    " [{id}: {stages}, collected {collected}x, requeued {requeued}x, {op_type} op {op_id} from {site}]",
                    stages = record.stages,
                    collected = record.collected,
                    requeued = record.requeued,
                    op_type = OptionDisplay(record.op_type.as_ref().map(|t| format!("{t:?}"))),
                    op_id = OptionDisplay(record.op_id.as_ref().map(|id| format!("{id:?}"))),
                    site = OptionDisplay(record.site.map(|site| site.to_string())),
                );
            }
            if queued_not_applied.len() > MAX_LISTED_GAP_IDS {
                let _ = write!(out, " and {} more", queued_not_applied.len() - MAX_LISTED_GAP_IDS);
            }
            let _ = write!(
                out,
                ". Those events did reach the queue, so their operations are still somewhere in the analyzer \
                 and batch collection is failing to assemble them: the bug is in collection, not in event production."
            );
        }

        out
    }
}

/// Names the producers on either side of the hole.
///
/// A leaked id has no record of its own, so the closest evidence about who
/// should have produced it is who produced its neighbours. One index allocates
/// its ids from one counter, so the neighbours are almost always the same call
/// path.
fn bracketing_producers(ledger: &StreamLedger, last_applied: u64, next_available: u64) -> String {
    let mut out = String::new();
    let before = ledger.ids.range(..=last_applied).next_back();
    let after = ledger.ids.range(next_available..).next();
    for (side, entry) in [("before the gap", before), ("after the gap", after)] {
        let Some((id, record)) = entry else {
            let _ = write!(out, " No record {side}.");
            continue;
        };
        let _ = write!(
            out,
            " Producer {side} (id {id}): {op_type}, op {op_id}, pushed from {site}.",
            op_type = OptionDisplay(record.op_type.as_ref().map(|t| format!("{t:?}"))),
            op_id = OptionDisplay(record.op_id.as_ref().map(|id| format!("{id:?}"))),
            site = OptionDisplay(record.site.map(|site| site.to_string())),
        );
        #[cfg(feature = "std")]
        if let Some(backtrace) = &record.backtrace {
            let _ = write!(out, " Backtrace:\n{backtrace}\n");
        }
    }
    #[cfg(feature = "std")]
    if !ledger.ids.values().any(|record| record.backtrace.is_some()) {
        let _ = write!(
            out,
            " Re-run with RUST_BACKTRACE=1 for the full producer backtraces, which this run did not capture."
        );
    }
    out
}

impl Default for EventLedger {
    fn default() -> Self {
        Self::detached()
    }
}

struct OptionDisplay(Option<String>);

impl core::fmt::Display for OptionDisplay {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match &self.0 {
            Some(value) => f.write_str(value),
            None => f.write_str("<unrecorded>"),
        }
    }
}

/// Renders a sorted id list compactly, collapsing runs.
fn format_ids(ids: &[u64]) -> String {
    let mut out = String::new();
    let mut listed = 0usize;
    let mut i = 0usize;
    while i < ids.len() && listed < MAX_LISTED_GAP_IDS {
        let start = ids[i];
        let mut end = start;
        while i + 1 < ids.len() && ids[i + 1] == end + 1 {
            i += 1;
            end = ids[i];
        }
        if !out.is_empty() {
            out.push_str(", ");
        }
        if start == end {
            let _ = write!(out, "{start}");
        } else {
            let _ = write!(out, "{start}..={end}");
        }
        listed += 1;
        i += 1;
    }
    if i < ids.len() {
        let _ = write!(out, ", and {} more", ids.len() - i);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn insert_at(id: u64) -> ChangeEvent<Pair<u64, Link>> {
        ChangeEvent::InsertAt {
            event_id: id.into(),
            max_value: Pair {
                key: id,
                value: Link::default(),
            },
            value: Pair {
                key: id,
                value: Link::default(),
            },
            index: 0,
        }
    }

    #[track_caller]
    fn queue(ledger: &EventLedger, ids: &[u64], op_type: OperationType) {
        let evs = ids.iter().copied().map(insert_at).collect::<Vec<_>>();
        let ids = EventLedger::event_ids(&evs);
        ledger.record_queued(
            EventStream::Primary,
            &ids,
            OperationId::Single(uuid::Uuid::from_u128(1)),
            op_type,
            Location::caller(),
        );
    }

    /// These tests assert on what the ledger recorded, so they are only
    /// meaningful where it records. That is every normal test run
    /// (`debug_assertions`); a `--release` test run skips them rather than
    /// failing on a report that correctly says bookkeeping was off.
    fn recording() -> bool {
        enabled()
    }

    #[test]
    fn names_ids_that_were_never_queued() {
        if !recording() {
            return;
        }
        let ledger = EventLedger::new("test/table");
        queue(&ledger, &[1, 2, 3], OperationType::Insert);
        // 4 and 5 are assigned by the index and leaked: nothing queues them.
        queue(&ledger, &[6, 7], OperationType::Update);

        let report = ledger.gap_report(&EventStream::Primary, 3, 6);

        assert!(report.contains("ASSIGNED BUT NEVER QUEUED"), "{report}");
        assert!(report.contains("4..=5"), "{report}");
        assert!(report.contains("event leak upstream of the analyzer"), "{report}");
    }

    #[test]
    fn distinguishes_a_queued_id_from_a_leaked_one() {
        if !recording() {
            return;
        }
        let ledger = EventLedger::new("test/table");
        queue(&ledger, &[1, 2, 3], OperationType::Insert);
        queue(&ledger, &[4], OperationType::Update);
        queue(&ledger, &[6], OperationType::Insert);

        let report = ledger.gap_report(&EventStream::Primary, 3, 6);

        assert!(report.contains("QUEUED BUT NOT APPLIED"), "{report}");
        assert!(report.contains("the bug is in collection"), "{report}");
        // Only id 5 is missing; 4 was queued.
        assert!(report.contains("ASSIGNED BUT NEVER QUEUED: 5."), "{report}");
    }

    #[test]
    fn reports_a_gapless_stream_as_nothing() {
        if !recording() {
            return;
        }
        let ledger = EventLedger::new("test/table");
        queue(&ledger, &[1, 2, 3], OperationType::Insert);
        assert!(ledger.gap_report(&EventStream::Primary, 3, 4).is_empty());
    }

    #[test]
    fn window_eviction_is_reported_rather_than_guessed() {
        if !recording() {
            return;
        }
        let ledger = EventLedger::new("test/table");
        let ids = (1..=(WINDOW as u64 + 64)).collect::<Vec<_>>();
        queue(&ledger, &ids, OperationType::Insert);

        // Ask about a gap far below the retained window.
        let report = ledger.gap_report(&EventStream::Primary, 1, 20);
        assert!(report.contains("fell out of the retained window"), "{report}");
    }

    #[test]
    fn applied_watermark_marks_everything_behind_it() {
        if !recording() {
            return;
        }
        let ledger = EventLedger::new("test/table");
        queue(&ledger, &[1, 2, 3, 5], OperationType::Insert);
        ledger.record_applied_upto(EventStream::Primary, 3);

        let report = ledger.gap_report(&EventStream::Primary, 3, 5);
        assert!(report.contains("ASSIGNED BUT NEVER QUEUED: 4."), "{report}");
    }

    #[test]
    fn stages_render_every_flag_set() {
        let mut stages = Stages::default();
        assert_eq!(stages.to_string(), "none");
        stages.insert(Stages::QUEUED);
        stages.insert(Stages::TRIMMED);
        assert_eq!(stages.to_string(), "queued+trimmed");
    }
}
