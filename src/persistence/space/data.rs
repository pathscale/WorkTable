use alloc::{string::String, string::ToString, vec::Vec};
use hashbrown::HashSet;
use std::path::Path;

use crate::fsx::File;
use crate::persistence::SpaceDataOps;
use crate::persistence::space::{BatchData, open_or_create_file};
use crate::prelude::WT_DATA_EXTENSION;
use convert_case::{Case, Casing};
use data_bucket::{
    DataPage, GeneralHeader, GeneralPage, Link, PageType, Persistable, SizeMeasurable, SpaceInfoPage,
    parse_data_pages_batch, parse_general_header_by_index, persist_page, persist_pages_batch,
};
use nagoya::io::{Read as _, Write as _};
use rkyv::api::high::HighDeserializer;
use rkyv::rancor::Strategy;
use rkyv::ser::Serializer;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};

fn link_sort_key(link: &Link) -> (u32, u32) {
    (link.page_id.into(), link.offset)
}

fn link_end(link: &Link) -> u64 {
    u64::from(link.offset) + u64::from(link.length)
}

/// Sorts and coalesces ranges within each page.
fn normalize_ranges(mut ranges: Vec<Link>) -> (Vec<Link>, bool) {
    let was_sorted = ranges
        .windows(2)
        .all(|pair| link_sort_key(&pair[0]) <= link_sort_key(&pair[1]));
    ranges.sort_unstable_by_key(link_sort_key);

    let mut changed = !was_sorted;
    let mut normalized: Vec<Link> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if range.length == 0 {
            changed = true;
            continue;
        }

        if let Some(last) = normalized.last_mut()
            && last.page_id == range.page_id
            && u64::from(range.offset) <= link_end(last)
        {
            let end = link_end(last).max(link_end(&range));
            last.length = (end - u64::from(last.offset)) as u32;
            changed = true;
            continue;
        }

        normalized.push(range);
    }

    (normalized, changed)
}

/// Subtracts sorted, coalesced used ranges from sorted, coalesced free ranges.
///
/// Both cursors only move forward, so the subtraction is O(f log f + u log u)
/// for sorting and O(f + u) for the scan instead of rebuilding the full free
/// list once per used link.
fn subtract_used_ranges(free_ranges: Vec<Link>, used_ranges: impl IntoIterator<Item = Link>) -> (Vec<Link>, bool) {
    let (free_ranges, mut changed) = normalize_ranges(free_ranges);
    let (used_ranges, _) = normalize_ranges(used_ranges.into_iter().collect());
    if used_ranges.is_empty() {
        return (free_ranges, changed);
    }

    let mut remaining = Vec::with_capacity(free_ranges.len() + used_ranges.len());
    let mut used_index = 0;

    for free in free_ranges {
        let free_page: u32 = free.page_id.into();
        let free_start = u64::from(free.offset);
        let free_end = link_end(&free);

        while let Some(used) = used_ranges.get(used_index) {
            let used_page: u32 = used.page_id.into();
            if used_page < free_page || (used_page == free_page && link_end(used) <= free_start) {
                used_index += 1;
            } else {
                break;
            }
        }

        let mut cursor = free_start;
        let mut scan = used_index;
        while let Some(used) = used_ranges.get(scan) {
            let used_page: u32 = used.page_id.into();
            let used_start = u64::from(used.offset);
            let used_end = link_end(used);
            if used_page != free_page || used_start >= free_end {
                break;
            }

            if used_end > cursor {
                if cursor < used_start {
                    let segment_end = used_start.min(free_end);
                    remaining.push(Link {
                        page_id: free.page_id,
                        offset: cursor as u32,
                        length: (segment_end - cursor) as u32,
                    });
                }

                let overlap_end = used_end.min(free_end);
                if cursor.max(used_start) < overlap_end {
                    changed = true;
                    cursor = overlap_end;
                }
            }

            if used_end <= free_end {
                scan += 1;
            } else {
                break;
            }
        }
        used_index = scan;

        if cursor < free_end {
            remaining.push(Link {
                page_id: free.page_id,
                offset: cursor as u32,
                length: (free_end - cursor) as u32,
            });
        }
    }

    (remaining, changed)
}

#[derive(Debug)]
pub struct SpaceData<PkGenState, const INNER_PAGE_SIZE: usize, const PAGE_SIZE: u32> {
    pub info: GeneralPage<SpaceInfoPage<PkGenState>>,
    pub last_page_id: u32,
    pub current_data_length: u32,
    pub data_file: File,
}

impl<PkGenState, const INNER_PAGE_SIZE: usize, const PAGE_SIZE: u32> SpaceData<PkGenState, INNER_PAGE_SIZE, PAGE_SIZE> {
    /// Creates every page from the current high-water mark through `target`.
    ///
    /// A link can name a page more than one past `last_page_id`: two writers
    /// allocating pages at once hand the queue the higher page first. Creating
    /// only the named page left the skipped ids as holes of zeros that the
    /// file nonetheless spans, and a hole is not a page. The batch path then
    /// classifies a skipped id as already existing, parses the zeroed header
    /// back as page 0 and looks up a key the batch never held; reload reads
    /// the same junk. So close the gap at the moment it opens.
    ///
    /// `already_written` names ids the caller persists itself, so the batch
    /// path does not pay a second write for each page it is about to write
    /// with its rows in it.
    async fn create_pages_up_to(&mut self, target: u32, already_written: &HashSet<u32>) -> eyre::Result<()> {
        while self.last_page_id < target {
            let id = self.last_page_id + 1;
            if !already_written.contains(&id) {
                let mut page = GeneralPage {
                    header: GeneralHeader::new(id.into(), PageType::Data, 0.into()),
                    inner: DataPage::<INNER_PAGE_SIZE>::new(),
                };
                persist_page::<_, PAGE_SIZE>(&mut page, &mut self.data_file).await?;
            }
            self.last_page_id = id;
            self.current_data_length = 0;
        }
        Ok(())
    }

    /// Keeps the serialized info page inside page 0's fixed slot.
    ///
    /// `empty_links_list` is the only unbounded part of [`SpaceInfoPage`], and
    /// `persist_page` writes the serialized info unchecked from the start of
    /// page 0: once it outgrows the slot (around 1350 reclaimed ranges) the
    /// overflow bytes land on data page 1 and corrupt live rows. Dropping the
    /// excess tail ranges leaks reusable space until those pages are
    /// reclaimed again, but never corrupts.
    fn bound_empty_links_list(&mut self)
    where
        SpaceInfoPage<PkGenState>: Persistable,
    {
        // Seeks inside data_bucket use its own PAGE_SIZE constant, so the slot
        // is bounded by whichever of the two page sizes is smaller.
        let budget = (PAGE_SIZE as usize).min(data_bucket::PAGE_SIZE) - data_bucket::GENERAL_HEADER_SIZE;
        let mut serialized = self.info.inner.as_bytes().as_ref().len();
        if serialized <= budget {
            return;
        }

        static TRUNCATION_WARNING: std::sync::Once = std::sync::Once::new();
        TRUNCATION_WARNING.call_once(|| {
            tracing::warn!(
                budget,
                "empty_links_list no longer fits the info page; dropping excess free ranges (space leak, not corruption)"
            );
        });

        let per_link = Link::default().aligned_size().max(1);
        while serialized > budget && !self.info.inner.empty_links_list.is_empty() {
            let keep = self
                .info
                .inner
                .empty_links_list
                .len()
                .saturating_sub((serialized - budget) / per_link + 1);
            self.info.inner.empty_links_list.truncate(keep);
            serialized = self.info.inner.as_bytes().as_ref().len();
        }
    }

    /// Removes written byte ranges from the durable free-range list.
    ///
    /// This is persisted before the corresponding row bytes. A crash between
    /// those writes can leak reusable space, but can never leave a live row
    /// described as free and eligible to be overwritten after reload.
    fn consume_reusable_ranges(&mut self, used_links: impl IntoIterator<Item = Link>) -> bool {
        let free_ranges = core::mem::take(&mut self.info.inner.empty_links_list);
        let (remaining, changed) = subtract_used_ranges(free_ranges, used_links);
        self.info.inner.empty_links_list = remaining;
        changed
    }
}

impl<PkGenState, const INNER_PAGE_SIZE: usize, const PAGE_SIZE: u32> SpaceDataOps<PkGenState>
    for SpaceData<PkGenState, INNER_PAGE_SIZE, PAGE_SIZE>
where
    PkGenState: Default
        + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>
        + Archive
        + Send
        + Sync,
    <PkGenState as Archive>::Archived: Deserialize<PkGenState, HighDeserializer<rkyv::rancor::Error>>,
    SpaceInfoPage<PkGenState>: Persistable,
{
    async fn from_table_files_path<S: AsRef<str> + Send>(table_path: S, version: u32) -> eyre::Result<Self> {
        let path = format!("{}/{}", table_path.as_ref(), WT_DATA_EXTENSION);
        let mut data_file = if !Path::new(&path).exists() {
            let name = table_path
                .as_ref()
                .split("/")
                .last()
                .expect("is not in root...")
                .to_string()
                .from_case(Case::Snake)
                .to_case(Case::Pascal);
            let mut data_file = open_or_create_file(path).await?;
            Self::bootstrap(&mut data_file, name, version).await?;
            data_file
        } else {
            open_or_create_file(path).await?
        };
        // The metadata occupies the payload, not the page stride. Read its
        // declared length after validating against that payload. DataBucket's
        // generic metadata reader takes a u32 capacity while ours is usize;
        // stable Rust cannot cast a generic const in another const argument.
        let header = parse_general_header_by_index::<PAGE_SIZE>(&mut data_file, 0).await?;
        eyre::ensure!(
            header.page_type == PageType::SpaceInfo,
            "expected a WorkTable space-info page"
        );
        let capacity = (PAGE_SIZE as usize)
            .checked_sub(data_bucket::GENERAL_HEADER_SIZE)
            .ok_or_else(|| eyre::eyre!("page stride is smaller than its header"))?;
        eyre::ensure!(INNER_PAGE_SIZE <= capacity, "inner page exceeds page payload");
        let length = if header.data_length == 0 {
            capacity
        } else {
            header.data_length as usize
        };
        eyre::ensure!(length <= capacity, "metadata exceeds page payload capacity");
        let mut bytes = vec![0; length];
        data_file.read_exact(&mut bytes).await?;
        let info = GeneralPage {
            inner: SpaceInfoPage::from_bytes(&bytes, header.data_version),
            header,
        };
        let file_length = crate::fsx::file_metadata(&mut data_file).await?;
        // Mirror the index file's ceil logic: a file whose length is an exact
        // page multiple ends with a full last page, so the plain floor
        // division names a page id one past EOF and reopening the table fails
        // on the header read. `ceil(len / PAGE_SIZE) - 1` is the last page
        // that actually exists in both the partial and the full-page case.
        let page_id = if file_length % PAGE_SIZE as u64 == 0 {
            (file_length / PAGE_SIZE as u64).saturating_sub(1)
        } else {
            file_length / PAGE_SIZE as u64
        };
        let last_page_header = parse_general_header_by_index::<PAGE_SIZE>(&mut data_file, page_id as u32).await?;

        Ok(Self {
            data_file,
            info,
            last_page_id: page_id as u32,
            current_data_length: last_page_header.data_length,
        })
    }

    async fn bootstrap(file: &mut File, table_name: String, version: u32) -> eyre::Result<()> {
        let info = SpaceInfoPage {
            id: 0.into(),
            page_count: 0,
            name: table_name,
            version,
            row_schema: vec![],
            primary_key_fields: vec![],
            secondary_index_types: vec![],
            pk_gen_state: Default::default(),
            empty_links_list: vec![],
        };
        let mut page = GeneralPage {
            header: GeneralHeader::new(0.into(), PageType::SpaceInfo, 0.into()),
            inner: info,
        };
        Ok(persist_page::<_, PAGE_SIZE>(&mut page, file).await?)
    }

    async fn save_data(&mut self, link: Link, bytes: &[u8]) -> eyre::Result<()> {
        let mut batch = BatchData::new();
        batch.insert(link.page_id, vec![(link, bytes.to_vec())]);
        self.save_batch_data(batch).await
    }

    async fn save_batch_data(&mut self, batch_data: BatchData) -> eyre::Result<()> {
        let used_links = batch_data
            .values()
            .flat_map(|ops| ops.iter().filter(|(_, bytes)| !bytes.is_empty()).map(|(link, _)| *link));
        if self.consume_reusable_ranges(used_links) {
            self.save_info().await?;
        }

        let page_ids = batch_data.keys().map(|id| (*id).into()).collect::<Vec<_>>();
        let ids_to_create = page_ids
            .iter()
            .filter(|id| **id > self.last_page_id)
            .cloned()
            .collect::<Vec<_>>();
        let ids_to_parse = page_ids
            .iter()
            .filter(|id| **id <= self.last_page_id)
            .cloned()
            .collect::<Vec<_>>();

        // `page_ids` iterates a HashMap, so `ids_to_create` is unordered:
        // taking `.last()` here picked an arbitrary created page, and a batch
        // creating several pages could leave `last_page_id` below a page that
        // now exists. The next batch touching that page would see it as "new"
        // and re-create it zero-filled, wiping the rows persisted before.
        //
        // Moving the mark to the maximum is necessary but not sufficient: the
        // ids between it and the old mark that this batch does not touch have
        // to become real pages too, or they stay holes. `create_pages_up_to`
        // skips the ids this batch writes for itself below.
        if let Some(max) = ids_to_create.iter().max().copied() {
            let written_by_this_batch = ids_to_create.iter().copied().collect::<HashSet<_>>();
            self.create_pages_up_to(max, &written_by_this_batch).await?;
        }
        let created_pages = ids_to_create
            .into_iter()
            .map(|id| GeneralPage {
                header: GeneralHeader::new(id.into(), PageType::Data, 0.into()),
                inner: DataPage {
                    rows: Vec::new(),
                    length: 0,
                    data: [0; INNER_PAGE_SIZE],
                },
            })
            .collect::<Vec<_>>();
        let parsed_pages =
            parse_data_pages_batch::<PAGE_SIZE, INNER_PAGE_SIZE, PAGE_SIZE>(&mut self.data_file, ids_to_parse).await?;

        let updated_pages = vec![parsed_pages, created_pages]
            .into_iter()
            .flatten()
            .map(|mut page| {
                let id = page.header.page_id;
                let ops = batch_data
                    .get(&id)
                    .expect("should be available as pages parsed from these ids");
                for (link, bytes) in ops {
                    if bytes.is_empty() {
                        page.inner.remove_at(*link);
                    } else {
                        page.inner.update_at(*link, bytes)?;
                    }
                }
                Ok::<_, eyre::Report>(page)
            })
            .collect::<Result<Vec<_>, _>>()?;

        // The batch writes each touched page's occupied extent into its header
        // (`persist_page_in_place` persists `inner.length` as data_length), so
        // the in-memory mirror for the last page must follow it. Leaving it
        // stale would make the next single-row save on the last page publish
        // an outdated length over the freshly persisted one.
        if let Some(page) = updated_pages
            .iter()
            .find(|page| u32::from(page.header.page_id) == self.last_page_id)
        {
            self.current_data_length = page.inner.length;
        }

        persist_pages_batch::<_, PAGE_SIZE>(updated_pages, &mut self.data_file).await?;
        // The batch's last page write is a buffered `write_all`; flush so the
        // batch is visible to other handles once it reports done.
        self.data_file.flush().await?;

        Ok(())
    }

    async fn reclaim_data_pages(&mut self, page_ids: Vec<data_bucket::page::PageId>) -> eyre::Result<()> {
        let page_ids = page_ids
            .into_iter()
            .filter(|page_id| {
                let id: u32 = (*page_id).into();
                id != 0 && id <= self.last_page_id
            })
            .collect::<HashSet<_>>();

        if page_ids.is_empty() {
            return Ok(());
        }

        // A reclaimed page must contain no live directory entries. Persist
        // that state before advertising the whole page as reusable.
        let cleared = page_ids
            .iter()
            .map(|page_id| GeneralPage {
                header: GeneralHeader::new(*page_id, PageType::Data, 0.into()),
                inner: DataPage::<INNER_PAGE_SIZE>::new(),
            })
            .collect();
        persist_pages_batch::<_, PAGE_SIZE>(cleared, &mut self.data_file).await?;
        self.data_file.flush().await?;

        self.info
            .inner
            .empty_links_list
            .retain(|link| !page_ids.contains(&link.page_id));
        let mut page_ids = page_ids.into_iter().collect::<Vec<_>>();
        page_ids.sort_unstable();
        self.info
            .inner
            .empty_links_list
            .extend(page_ids.into_iter().map(|page_id| Link {
                page_id,
                offset: 0,
                length: INNER_PAGE_SIZE as u32,
            }));
        self.info.inner.empty_links_list.sort_by_key(|link| {
            let page_id: u32 = link.page_id.into();
            (page_id, link.offset)
        });
        self.save_info().await
    }

    fn get_mut_info(&mut self) -> &mut GeneralPage<SpaceInfoPage<PkGenState>> {
        &mut self.info
    }

    async fn save_info(&mut self) -> eyre::Result<()> {
        // Single choke point for the info page reaching disk: enforce the
        // page-0 slot budget however the free-range list was mutated.
        self.bound_empty_links_list();
        persist_page::<_, PAGE_SIZE>(&mut self.info, &mut self.data_file).await?;
        // A generated table may immediately reopen this file through a
        // separate handle. Make the updated metadata visible before reporting
        // success, just as `save_data` does for row bytes.
        self.data_file.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use data_bucket::page::PageId;

    use super::{SpaceData, subtract_used_ranges};
    use crate::persistence::SpaceDataOps;
    use crate::persistence::space::BatchData;
    use crate::prelude::Link;

    fn link(page_id: u32, offset: u32, length: u32) -> Link {
        Link {
            page_id: PageId::from(page_id),
            offset,
            length,
        }
    }

    #[test]
    fn reusable_ranges_are_subtracted_in_one_sorted_scan() {
        let free = vec![link(2, 0, 50), link(1, 0, 100)];
        let used = vec![link(1, 40, 20), link(2, 0, 10), link(1, 10, 20), link(1, 25, 30)];

        let (remaining, changed) = subtract_used_ranges(free, used);

        assert!(changed);
        assert_eq!(remaining, vec![link(1, 0, 10), link(1, 60, 40), link(2, 10, 40)]);
    }

    #[test]
    fn non_overlapping_used_ranges_leave_free_ranges_unchanged() {
        let free = vec![link(1, 0, 10), link(1, 20, 10), link(2, 0, 10)];
        let used = vec![link(1, 10, 10), link(3, 0, 10)];

        let (remaining, changed) = subtract_used_ranges(free.clone(), used);

        assert!(!changed);
        assert_eq!(remaining, free);
    }

    #[test]
    fn randomized_subtraction_matches_byte_level_coverage() {
        const PAGES: usize = 4;
        const BYTES: usize = 64;
        let mut rng = fastrand::Rng::with_seed(0x51ce_5eed);

        for case in 0..1_000 {
            let mut free = Vec::new();
            let mut used = Vec::new();
            let mut expected = [[false; BYTES]; PAGES];

            for _ in 0..rng.usize(0..20) {
                let page = rng.usize(0..PAGES);
                let start = rng.usize(0..BYTES);
                let end = rng.usize(start + 1..=BYTES);
                free.push(link((page + 1) as u32, start as u32, (end - start) as u32));
                expected[page][start..end].fill(true);
            }
            for _ in 0..rng.usize(0..20) {
                let page = rng.usize(0..PAGES);
                let start = rng.usize(0..BYTES);
                let end = rng.usize(start + 1..=BYTES);
                used.push(link((page + 1) as u32, start as u32, (end - start) as u32));
                expected[page][start..end].fill(false);
            }

            let (remaining, _) = subtract_used_ranges(free, used);
            let mut actual = [[false; BYTES]; PAGES];
            for range in remaining {
                let page: u32 = range.page_id.into();
                let page = page as usize - 1;
                let start = range.offset as usize;
                let end = start + range.length as usize;
                assert!(actual[page][start..end].iter().all(|occupied| !occupied), "case {case}");
                actual[page][start..end].fill(true);
            }

            assert_eq!(actual, expected, "case {case}");
        }
    }

    /// A gap in the page sequence must not make the batch path parse a hole.
    ///
    /// Two writers allocating pages at once can hand the queue a link on the
    /// higher page first. `save_data` then creates only that page and moves
    /// `last_page_id` up to it, so the skipped page is a hole of zeros that
    /// the file nonetheless spans. The next batch touching the skipped page
    /// classifies it as already existing (`id <= last_page_id`), parses the
    /// hole back, reads a `page_id` of 0 out of the zeroed header and looks up
    /// a key the batch never contained.
    ///
    /// This is the mechanism behind
    /// `tests/persistence/concurrent_upsert_batch.rs`, reduced to the two
    /// calls that produce it so it takes milliseconds instead of forty
    /// minutes.
    #[tokio::test]
    async fn a_batch_touching_a_skipped_page_does_not_parse_a_hole() {
        const PAGE: u32 = data_bucket::PAGE_SIZE as u32;
        const INNER: usize = data_bucket::PAGE_SIZE - data_bucket::GENERAL_HEADER_SIZE;

        let dir = std::env::temp_dir().join(format!("wt-page-gap-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("a scratch dir");
        let path = dir.to_str().expect("utf-8 path").to_owned();

        let mut space = SpaceData::<u64, INNER, PAGE>::from_table_files_path(path, 1)
            .await
            .expect("a fresh space");

        // Page 3, with 1 and 2 never written: the out-of-order case.
        space
            .save_data(link(3, 0, 8), &[1u8; 8])
            .await
            .expect("the high page saves");
        assert_eq!(space.last_page_id, 3, "the high-water mark follows the link");

        // Now hand the batch path the page that was skipped.
        let mut batch = BatchData::new();
        batch.insert(PageId::from(1u32), vec![(link(1, 0, 8), vec![2u8; 8])]);
        space.save_batch_data(batch).await.expect("the skipped page saves");

        // The point of the fix is on disk, not in the call returning: every id
        // through the high-water mark has to carry its own header. Reading
        // them back is what distinguishes a filled gap from a hole that this
        // particular call happened to survive.
        for id in 1..=3u32 {
            let header = super::parse_general_header_by_index::<PAGE>(&mut space.data_file, id)
                .await
                .expect("a header at every page through the mark");
            assert_eq!(u32::from(header.page_id), id, "page {id} is a page, not a hole");
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
