//! Load and unload a `worktable_vec!` table as pages.
//!
//! # What this is
//!
//! `worktable_vec!` drops paging because a `Vec` does not need it while the
//! table is in use. It still needs a way to put rows on a disk and get them
//! back, and that is a codec rather than a storage engine: rows live in a
//! `Vec` and are pages only at rest. Everything between a load and an unload
//! runs at `Vec` speed because it *is* a `Vec`.
//!
//! This is ported from `worktable-vec`'s `hydrate` module. Corruption and append
//! tests live here alongside generated-table integration tests. It is reproduced rather
//! than depended on because a dependency would invert the direction this is
//! meant to travel: WorkTable is meant to absorb that crate, not require it.
//!
//! # Page based, and each page stands alone
//!
//! A page is 16 KiB: a 28 byte header, then an rkyv archive of **the rows that
//! fit in that page**, then a 12 byte directory at the tail. Nothing spans a
//! boundary.
//!
//! That is the whole design. An archive split across pages means one damaged
//! page destroys every row in the file, and it means appending a row rewrites
//! everything. Self-contained pages make damage local and appends O(new rows),
//! and cost only the few bytes of archive overhead repeated per page.
//!
//! # What is checked
//!
//! Every page carries a CRC-32 covering all bytes except the checksum, and every header field is
//! validated rather than merely written. rkyv's own validation checks that an
//! archive is structurally sound, which is not the same as checking that these
//! are the bytes that were written: a flipped bit inside a `u64` passes
//! structural validation and reads back as a different number. The checksum is
//! what catches that.
//!
//! # These files are not interchangeable with `worktable-vec`'s
//!
//! That crate stores `Vec<(K, V)>`, because its value type has no key in it. A
//! `worktable_vec!` row is a named struct that already carries its primary key
//! as a column, so this stores `Vec<Row>` and does not write the key twice.
//! Different types, different rkyv archives, different fingerprints.
//!
//! The fingerprint is what makes that safe rather than merely true: a foreign
//! file is refused by [`LoadError::ForeignRows`] instead of being read as
//! debris. Do not expect a file written by one to open in the other.
//!
//! # These are not WorkTable space files either
//!
//! A WorkTable space opens with a page carrying a name, a schema and a primary
//! key list. These pages use a distinct page type (4, archived rows), a zero
//! space id and a row-type fingerprint in the trailer. Neither reader accepts
//! the other container as its own.

use alloc::vec::Vec;

use rkyv::api::high::{HighDeserializer, HighValidator};
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::{Error as RkyvError, Strategy};
use rkyv::ser::Serializer;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};

/// One page, header included.
pub const PAGE_SIZE: usize = 4096 * 4;

/// DataBucket's `GENERAL_HEADER_SIZE`, which this page opens with.
pub const HEADER_SIZE: usize = 28;

/// The page trailer: row count, row-type fingerprint and CRC-32, all little endian.
pub const DIRECTORY_SIZE: usize = 12;

/// How much of a page is body, between the header and the directory.
pub const BODY_SIZE: usize = PAGE_SIZE - HEADER_SIZE - DIRECTORY_SIZE;

/// `DATA_VERSION` 3: DataBucket's page framing, plus a row directory.
///
/// This identifies the Vec snapshot framing. Ordinary WorkTable spaces also
/// use version 3, but start with SpaceInfo metadata and use a different row
/// directory layout. The two containers are not interchangeable.
pub const PAGE_VERSION: u32 = 3;

/// Archived row batches, distinct from DataBucket's ordinary Data pages (2).
const PAGE_TYPE_ARCHIVED_ROWS: u32 = 4;

/// What a load can refuse on.
///
/// Every variant is a statement about the bytes rather than about the caller,
/// and every one names the page, because a file that will not load is a
/// question about which page went wrong.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LoadError {
    /// The byte length is not a whole number of pages.
    NotWholePages {
        /// How many bytes arrived.
        found: usize,
    },
    /// A page carries a version this build does not write.
    ///
    /// Also what a page of zeroes looks like, which is the shape a torn write
    /// leaves behind.
    ForeignPages {
        /// Which page, counting from zero.
        page: usize,
        /// The version that page claims.
        version: u32,
    },
    /// A page belongs to a different container.
    ForeignPageType {
        /// Position in the supplied byte slice.
        page: usize,
        /// Type carried by the header.
        page_type: u32,
    },
    /// The space id, page number or chain links are invalid.
    PageIdentity {
        /// Position in the supplied byte slice.
        page: usize,
    },
    /// A header claimed a body longer than a page holds.
    Overlong {
        /// Which page, counting from zero.
        page: usize,
        /// What its header claimed.
        claimed: usize,
    },
    /// The body does not match the checksum written with it.
    ///
    /// This is the one rkyv cannot find. A flipped bit inside an integer is a
    /// structurally perfect archive of the wrong number.
    Corrupt {
        /// Which page, counting from zero.
        page: usize,
        /// The checksum written with the body.
        expected: u32,
        /// The checksum of the bytes actually there.
        found: u32,
    },
    /// The pages disagree with each other about the row type.
    Inconsistent {
        /// Which page disagreed.
        page: usize,
    },
    /// These are a different row type's bytes.
    ///
    /// Caught by a fingerprint rather than by deserialization, because
    /// deserialization does not catch it: rkyv validates a `(u64, String)`
    /// archive as a perfectly good `(u64, u64)` and hands back a `String`'s
    /// relative pointer as an integer. Keys look right, values are debris, and
    /// nothing errors.
    ForeignRows {
        /// The fingerprint these bytes were written with.
        found: u32,
        /// The fingerprint this row type expects.
        expected: u32,
    },
    /// A page's rows did not deserialize.
    Rows {
        /// Which page, counting from zero.
        page: usize,
    },
    /// A page's directory promised a row count its body did not contain.
    RowCount {
        /// Which page, counting from zero.
        page: usize,
        /// What the directory promised.
        expected: usize,
        /// What the body held.
        found: usize,
    },
}

impl core::fmt::Display for LoadError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::NotWholePages { found } => {
                write!(
                    formatter,
                    "{found} bytes is not a whole number of {PAGE_SIZE} byte pages"
                )
            }
            Self::ForeignPages { page, version } => {
                write!(
                    formatter,
                    "page {page} claims format version {version}, not {PAGE_VERSION}"
                )
            }
            Self::Overlong { page, claimed } => {
                write!(
                    formatter,
                    "page {page} claims a {claimed} byte body, over the {BODY_SIZE} byte limit"
                )
            }
            Self::ForeignPageType { page, page_type } => write!(
                formatter,
                "page {page} has type {page_type}, not archived rows ({PAGE_TYPE_ARCHIVED_ROWS})"
            ),
            Self::PageIdentity { page } => write!(formatter, "page {page} has invalid identity or chain links"),
            Self::Corrupt { page, expected, found } => {
                write!(
                    formatter,
                    "page {page} checksums to {found:#010x}, not the {expected:#010x} written with it"
                )
            }
            Self::Inconsistent { page } => {
                write!(
                    formatter,
                    "page {page} names a different row type than the pages before it"
                )
            }
            Self::ForeignRows { found, expected } => {
                write!(
                    formatter,
                    "these pages hold row type {found:#010x}, not {expected:#010x}"
                )
            }
            Self::Rows { page } => write!(formatter, "page {page} did not deserialize into rows"),
            Self::RowCount { page, expected, found } => {
                write!(formatter, "page {page} promised {expected} rows and held {found}")
            }
        }
    }
}

impl core::error::Error for LoadError {}

/// One row does not fit in a page, so the file was not written.
///
/// Refused rather than written, because the writer used to produce a file
/// `load` then refused: `unload` reported success and the rows were gone.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RowTooLarge {
    /// Which row, counting from zero.
    pub row: usize,
    /// How many bytes its archive needed.
    pub bytes: usize,
    /// How many a page body holds.
    pub limit: usize,
}

impl core::fmt::Display for RowTooLarge {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            formatter,
            "row {} needs {} bytes and a page body holds {}",
            self.row, self.bytes, self.limit
        )
    }
}

impl core::error::Error for RowTooLarge {}

/// Failure to encode a snapshot segment with an explicit starting page number.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnloadError {
    /// An individual row cannot fit.
    RowTooLarge(RowTooLarge),
    /// A page number would exceed the format's u32 range.
    PageIndexOverflow,
}

impl core::fmt::Display for UnloadError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::RowTooLarge(error) => error.fmt(formatter),
            Self::PageIndexOverflow => formatter.write_str("snapshot page number exceeds u32"),
        }
    }
}

impl core::error::Error for UnloadError {}

/// The one thing [`Codec::decode`] can say.
///
/// Which page it happened on is the caller's to add, because a codec does not
/// know it is reading a page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NotAnArchive;

/// Rows to bytes and back.
///
/// Blanket-implemented, so anything deriving rkyv's traits satisfies it and
/// the generated row needs no separate impl.
pub trait Codec: Sized {
    /// Rows to bytes.
    fn encode(&self) -> AlignedVec<16>;
    /// Bytes back to rows.
    ///
    /// # Errors
    ///
    /// Fails when the bytes are not this type's archive.
    fn decode(bytes: &[u8]) -> Result<Self, NotAnArchive>;
}

impl<T> Codec for T
where
    T: Archive + for<'a> Serialize<Strategy<Serializer<AlignedVec<16>, ArenaHandle<'a>, Share>, RkyvError>>,
    <T as Archive>::Archived:
        Deserialize<T, HighDeserializer<RkyvError>> + for<'a> CheckBytes<HighValidator<'a, RkyvError>>,
{
    fn encode(&self) -> AlignedVec<16> {
        // Infallible in practice: the only failure rkyv reports here is an
        // allocator refusing, which on this path means the process is already
        // out of memory.
        rkyv::to_bytes::<RkyvError>(self).expect("rows serialize")
    }

    fn decode(bytes: &[u8]) -> Result<Self, NotAnArchive> {
        rkyv::from_bytes::<T, RkyvError>(bytes).map_err(|_| NotAnArchive)
    }
}

/// What row type wrote these bytes.
///
/// FNV-1a over `core::any::type_name`, which is neither stable across compiler
/// versions nor guaranteed unique. That is fine for what it is for: refusing
/// an obvious mismatch, not authenticating a schema. A false match is possible
/// and a false mismatch is a rebuild, so it fails toward refusing to load
/// rather than toward reinterpreting.
pub fn fingerprint<T: ?Sized>() -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for byte in core::any::type_name::<T>().as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

/// CRC-32 using the existing no-default-features checksum dependency.
fn crc32(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}

/// DataBucket's `GeneralHeader`, byte for byte.
///
/// Seven little-endian `u32`s in declaration order, which is what
/// `rkyv::to_bytes` of that struct produces: no relative pointers, and
/// `page_type` padded from `u16` to four bytes. For a `Data` page of space 3,
/// id 7, previous 6, next 8, length `0x11223344`:
///
/// ```text
/// 02000000 03000000 07000000 06000000 08000000 02000000 44332211
/// version  space    page     previous next     type     length
/// ```
///
/// Written out here rather than imported from `data_bucket`, which is `std`.
/// The bytes above document the shared framing. The archived-rows page type
/// and trailer distinguish this container from ordinary DataBucket spaces.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Header {
    /// `DATA_VERSION`. See [`PAGE_VERSION`].
    version: u32,
    /// Zero: standalone snapshots do not belong to a WorkTable space.
    space: u32,
    page: u32,
    previous: u32,
    next: u32,
    /// Archived row batches, which is 4.
    page_type: u32,
    /// Bytes of row archive in this page, before the directory.
    body: u32,
}

impl Header {
    fn write(self, out: &mut Vec<u8>) {
        for field in [
            self.version,
            self.space,
            self.page,
            self.previous,
            self.next,
            self.page_type,
            self.body,
        ] {
            out.extend_from_slice(&field.to_le_bytes());
        }
    }

    fn read(raw: &[u8]) -> Self {
        let at = |n: usize| {
            let mut word = [0u8; 4];
            word.copy_from_slice(&raw[n * 4..n * 4 + 4]);
            u32::from_le_bytes(word)
        };
        Self {
            version: at(0),
            space: at(1),
            page: at(2),
            previous: at(3),
            next: at(4),
            page_type: at(5),
            body: at(6),
        }
    }
}

/// The row directory, at the tail of every page.
///
/// This count describes the archived row batch. Ordinary WorkTable v3 data
/// pages instead use an offset/length directory per live row. Both containers
/// describe their rows locally, but their directory layouts are different.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Directory {
    rows: u32,
    schema: u32,
    crc: u32,
}

impl Directory {
    fn write(self, page: &mut [u8]) {
        let at = page.len() - DIRECTORY_SIZE;
        page[at..at + 4].copy_from_slice(&self.rows.to_le_bytes());
        page[at + 4..at + 8].copy_from_slice(&self.schema.to_le_bytes());
        page[at + 8..].copy_from_slice(&self.crc.to_le_bytes());
    }

    fn read(page: &[u8]) -> Self {
        let at = page.len() - DIRECTORY_SIZE;
        let word = |n: usize| {
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(&page[n..n + 4]);
            u32::from_le_bytes(bytes)
        };
        Self {
            rows: word(at),
            schema: word(at + 4),
            crc: word(at + 8),
        }
    }
}

/// The most rows of `rows` whose archive fits one page body.
///
/// **Bounded probes.** The obvious version binary searches over the whole
/// remaining slice, which re-serializes every row still to be written on every
/// probe, for every page. In `worktable-vec` that measured 2.3 seconds to
/// write what rkyv alone encodes in 6.7 ms, because the work is quadratic in
/// the row count.
///
/// So the search is bounded to roughly two pages of rows: one sample encode
/// gives bytes per row, the estimate from that sets the ceiling, and the
/// binary search runs under it. Every probe serializes about a page, never a
/// file. Uniform rows land in a probe or two and wildly variable rows still
/// terminate, because the ceiling is only a ceiling.
///
/// Always returns at least one for a non-empty slice, so the caller always
/// makes progress. A single row too large for a page is caught by the writer
/// rather than looping here forever.
fn rows_per_page<R>(rows: &[R], hint: usize) -> (usize, AlignedVec<16>)
where
    Vec<R>: Codec,
    R: Clone,
{
    if rows.is_empty() {
        return (0, rows.to_vec().encode());
    }

    let mut best = None;
    let mut fits = |take: usize| {
        let archive = rows[..take].to_vec().encode();
        if archive.len() <= BODY_SIZE {
            best = Some((take, archive));
            true
        } else {
            false
        }
    };

    // A page holds about what the last one held, so start there and walk.
    // Uniform rows settle in a probe or two; only the first page, or a run
    // whose rows change size, pays for a search.
    if hint > 0 && hint <= rows.len() && fits(hint) {
        let mut take = hint;
        while take < rows.len() && fits(take + 1) {
            take += 1;
        }
        return best.expect("the successful hint supplied an archive");
    }

    // No usable hint, or the rows grew. One sample gives bytes per row, and
    // the estimate from it bounds the search to about two pages of rows.
    let sample = rows.len().min(64);
    let sampled = rows[..sample].to_vec().encode().len();
    let estimate = (BODY_SIZE * sample)
        .checked_div(sampled)
        .map_or(rows.len(), |estimate| estimate.max(1));
    let mut low = 1usize;
    let mut high = rows.len().min(estimate.saturating_mul(2)).max(1);
    while low < high {
        let mid = low + (high - low).div_ceil(2);
        if fits(mid) {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    best.filter(|(take, _)| *take == low)
        .unwrap_or_else(|| (low, rows[..low].to_vec().encode()))
}

/// Rows to pages, each page standing alone.
///
/// # Errors
///
/// [`RowTooLarge`] when one row's archive does not fit a page body. Nothing is
/// written in that case.
pub fn to_pages<R>(rows: &[R]) -> Result<Vec<u8>, RowTooLarge>
where
    Vec<R>: Codec,
    R: Clone,
{
    match to_pages_at(rows, 0) {
        Ok(bytes) => Ok(bytes),
        Err(UnloadError::RowTooLarge(error)) => Err(error),
        // A Vec holding over u32::MAX 16 KiB pages would require over 64 TiB.
        Err(UnloadError::PageIndexOverflow) => panic!("snapshot exceeds u32 page count"),
    }
}

/// Encode an append segment, numbering its pages from `first_page`.
///
/// Pass the existing file length divided by [`PAGE_SIZE`]. The preceding
/// segment remains terminal; appending does not rewrite its last page.
/// Only new rows belong in this segment. Updates and deletes require a full
/// snapshot. A standalone segment can be inspected with [`from_pages`].
///
/// # Errors
///
/// Refuses oversized rows and page-number overflow without producing bytes.
pub fn to_pages_at<R>(rows: &[R], first_page: u32) -> Result<Vec<u8>, UnloadError>
where
    Vec<R>: Codec,
    R: Clone,
{
    let schema = fingerprint::<Vec<R>>();
    let mut out = Vec::new();
    let mut rest = rows;
    let mut hint = 0usize;

    // An empty table still writes one page. A zero byte file is
    // indistinguishable from a missing one, and a load has to tell "no rows"
    // from "nothing landed".
    loop {
        let (take, archive) = rows_per_page(rest, hint);
        hint = take;
        let body = archive.as_ref();
        // `rows_per_page` returns at least one so the loop always advances, so
        // a body over the limit means that one row does not fit a page.
        if body.len() > BODY_SIZE {
            return Err(UnloadError::RowTooLarge(RowTooLarge {
                row: rows.len() - rest.len(),
                bytes: body.len(),
                limit: BODY_SIZE,
            }));
        }
        let page = u32::try_from(out.len() / PAGE_SIZE)
            .ok()
            .and_then(|offset| first_page.checked_add(offset))
            .ok_or(UnloadError::PageIndexOverflow)?;
        let last = rest.len() == take;
        let next = if last {
            page
        } else {
            page.checked_add(1).ok_or(UnloadError::PageIndexOverflow)?
        };
        Header {
            version: PAGE_VERSION,
            space: 0,
            page,
            previous: page.saturating_sub(1),
            // A last page points at itself, so a chain walker stops rather
            // than running off the end.
            next,
            page_type: PAGE_TYPE_ARCHIVED_ROWS,
            body: u32::try_from(body.len()).expect("a body inside u32"),
        }
        .write(&mut out);
        out.extend_from_slice(body);
        out.resize(out.len().next_multiple_of(PAGE_SIZE), 0);

        // The directory goes in last, into the tail of the page just written.
        let start = out.len() - PAGE_SIZE;
        Directory {
            rows: u32::try_from(take).expect("a row count inside u32"),
            schema,
            crc: 0,
        }
        .write(&mut out[start..]);
        let crc = crc32(&out[start..out.len() - 4]);
        let end = out.len();
        out[end - 4..].copy_from_slice(&crc.to_le_bytes());

        rest = &rest[take..];
        if rest.is_empty() {
            break;
        }
    }
    Ok(out)
}

/// One page back into rows, with every header field checked.
fn page_rows<R>(
    raw: &[u8],
    index: usize,
    schema: &mut Option<u32>,
    previous: Option<Header>,
    last: bool,
) -> Result<(Vec<R>, Header), LoadError>
where
    Vec<R>: Codec,
{
    let header = Header::read(&raw[..HEADER_SIZE]);
    if header.version != PAGE_VERSION {
        return Err(LoadError::ForeignPages {
            page: index,
            version: header.version,
        });
    }
    if header.page_type != PAGE_TYPE_ARCHIVED_ROWS {
        return Err(LoadError::ForeignPageType {
            page: index,
            page_type: header.page_type,
        });
    }
    let directory = Directory::read(raw);
    let found = crc32(&raw[..PAGE_SIZE - 4]);
    if found != directory.crc {
        return Err(LoadError::Corrupt {
            page: index,
            expected: directory.crc,
            found,
        });
    }
    let valid_predecessor = previous.is_none_or(|before| {
        if before.next == before.page {
            // Independent unloads restart at zero. Explicit append segments
            // continue numbering without rewriting the previous terminal page.
            header.page == 0 || before.page.checked_add(1) == Some(header.page)
        } else {
            header.page == before.next
        }
    });
    if header.space != 0
        || header.previous != header.page.saturating_sub(1)
        || !(header.next == header.page || header.page.checked_add(1) == Some(header.next))
        || !valid_predecessor
        || (last && header.next != header.page)
    {
        return Err(LoadError::PageIdentity { page: index });
    }
    match schema {
        None => *schema = Some(directory.schema),
        // Every page names the row type, so a file spliced onto another is
        // caught where they stop agreeing rather than concatenated.
        Some(first) if *first != directory.schema => {
            return Err(LoadError::Inconsistent { page: index });
        }
        Some(_) => {}
    }
    let expected = fingerprint::<Vec<R>>();
    if directory.schema != expected {
        return Err(LoadError::ForeignRows {
            found: directory.schema,
            expected,
        });
    }

    let take = header.body as usize;
    if take > BODY_SIZE {
        return Err(LoadError::Overlong {
            page: index,
            claimed: take,
        });
    }
    let body = &raw[HEADER_SIZE..HEADER_SIZE + take];

    // Copied into an AlignedVec because rkyv reads an archive in place and
    // needs it aligned. A page body sits at a header's offset into a Vec<u8>,
    // which is aligned to nothing in particular.
    let mut aligned = AlignedVec::<16>::with_capacity(take);
    aligned.extend_from_slice(body);
    let rows = Vec::<R>::decode(&aligned).map_err(|NotAnArchive| LoadError::Rows { page: index })?;
    if rows.len() != directory.rows as usize {
        return Err(LoadError::RowCount {
            page: index,
            expected: directory.rows as usize,
            found: rows.len(),
        });
    }
    Ok((rows, header))
}

/// Every page back into one row vector.
///
/// # Errors
///
/// [`LoadError`], naming the page that went wrong.
pub fn from_pages<R>(bytes: &[u8]) -> Result<Vec<R>, LoadError>
where
    Vec<R>: Codec,
{
    if bytes.is_empty() || !bytes.len().is_multiple_of(PAGE_SIZE) {
        return Err(LoadError::NotWholePages { found: bytes.len() });
    }

    let mut schema = None;
    let mut rows = Vec::new();
    let mut previous = None;
    for (index, raw) in bytes.as_chunks::<PAGE_SIZE>().0.iter().enumerate() {
        let last = index + 1 == bytes.len() / PAGE_SIZE;
        let (mut page, header) = page_rows(raw, index, &mut schema, previous, last)?;
        rows.append(&mut page);
        previous = Some(header);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set_word(page: &mut [u8], offset: usize, value: u32) {
        page[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    fn checksum(page: &mut [u8]) {
        let crc = crc32(&page[..PAGE_SIZE - 4]);
        set_word(page, PAGE_SIZE - 4, crc);
    }

    #[test]
    fn corruption_in_header_body_padding_and_directory_is_refused() {
        let bytes = to_pages(&[42u64, 97]).unwrap();
        for offset in (0..HEADER_SIZE).chain([
            HEADER_SIZE,
            HEADER_SIZE + 8,
            PAGE_SIZE / 2,
            PAGE_SIZE - 12,
            PAGE_SIZE - 8,
            PAGE_SIZE - 4,
            PAGE_SIZE - 1,
        ]) {
            let mut damaged = bytes.clone();
            damaged[offset] ^= 1;
            assert!(from_pages::<u64>(&damaged).is_err(), "accepted damage at {offset}");
        }
        assert_eq!(from_pages::<u64>(&bytes).unwrap(), [42, 97]);
    }

    #[test]
    fn ordinary_data_pages_and_invalid_links_are_refused_even_with_valid_crc() {
        let bytes = to_pages(&[42u64]).unwrap();
        let mut foreign = bytes.clone();
        set_word(&mut foreign, 20, 2);
        checksum(&mut foreign);
        assert!(matches!(
            from_pages::<u64>(&foreign),
            Err(LoadError::ForeignPageType { page_type: 2, .. })
        ));
        for (offset, value) in [(4, 1), (8, 1), (12, 1), (16, 1)] {
            let mut damaged = bytes.clone();
            set_word(&mut damaged, offset, value);
            checksum(&mut damaged);
            assert!(matches!(
                from_pages::<u64>(&damaged),
                Err(LoadError::PageIdentity { .. })
            ));
        }
    }

    #[test]
    fn page_omission_reordering_and_truncation_are_refused() {
        let rows: Vec<u64> = (0..8_000).collect();
        let bytes = to_pages(&rows).unwrap();
        assert!(bytes.len() >= PAGE_SIZE * 3);
        assert_eq!(from_pages::<u64>(&bytes).unwrap(), rows);
        assert!(from_pages::<u64>(&bytes[..bytes.len() - PAGE_SIZE]).is_err());
        let mut omitted = bytes[..PAGE_SIZE].to_vec();
        omitted.extend_from_slice(&bytes[PAGE_SIZE * 2..]);
        assert!(from_pages::<u64>(&omitted).is_err());
        let mut swapped = bytes.clone();
        swapped[..PAGE_SIZE].copy_from_slice(&bytes[PAGE_SIZE..2 * PAGE_SIZE]);
        swapped[PAGE_SIZE..2 * PAGE_SIZE].copy_from_slice(&bytes[..PAGE_SIZE]);
        assert!(from_pages::<u64>(&swapped).is_err());
    }

    #[test]
    fn append_segments_keep_existing_pages_and_allow_independent_snapshots() {
        let first: Vec<u64> = (0..4_000).collect();
        let next: Vec<u64> = (4_000..8_000).collect();
        let mut bytes = to_pages(&first).unwrap();
        let original = bytes.clone();
        let segment = to_pages_at(&next, (bytes.len() / PAGE_SIZE) as u32).unwrap();
        assert_eq!(from_pages::<u64>(&segment).unwrap(), next);
        bytes.extend_from_slice(&segment);
        assert_eq!(&bytes[..original.len()], &original);
        assert_eq!(from_pages::<u64>(&bytes).unwrap(), (0..8_000).collect::<Vec<_>>());
        bytes.extend_from_slice(&to_pages(&[8_000u64]).unwrap());
        assert_eq!(from_pages::<u64>(&bytes).unwrap(), (0..8_001).collect::<Vec<_>>());
    }

    #[test]
    fn append_page_overflow_is_a_reported_error() {
        let rows: Vec<u64> = (0..4_000).collect();
        assert_eq!(to_pages_at(&rows, u32::MAX), Err(UnloadError::PageIndexOverflow));
        let last = to_pages_at(&[1u64], u32::MAX).unwrap();
        assert_eq!(from_pages::<u64>(&last).unwrap(), [1]);
    }

    #[test]
    fn trailer_schema_and_count_are_validated_before_accepting_rows() {
        let bytes = to_pages(&[42u64]).unwrap();
        assert!(matches!(from_pages::<u32>(&bytes), Err(LoadError::ForeignRows { .. })));
        let mut wrong_count = bytes.clone();
        set_word(&mut wrong_count, PAGE_SIZE - DIRECTORY_SIZE, 2);
        checksum(&mut wrong_count);
        assert!(matches!(
            from_pages::<u64>(&wrong_count),
            Err(LoadError::RowCount {
                expected: 2,
                found: 1,
                ..
            })
        ));
        let mut overlong = bytes;
        set_word(&mut overlong, 24, PAGE_SIZE as u32);
        checksum(&mut overlong);
        assert!(matches!(from_pages::<u64>(&overlong), Err(LoadError::Overlong { .. })));
    }
}
