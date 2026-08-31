use std::collections::HashSet;
use std::io::SeekFrom;
use std::path::Path;

use crate::persistence::SpaceDataOps;
use crate::persistence::space::{BatchData, open_or_create_file};
use crate::prelude::WT_DATA_EXTENSION;
use convert_case::{Case, Casing};
use data_bucket::{
    DataPage, GeneralHeader, GeneralPage, Link, PageType, Persistable, SizeMeasurable, SpaceInfoPage,
    parse_data_pages_batch, parse_general_header_by_index, parse_page, persist_page, persist_pages_batch, update_at,
};
use rkyv::api::high::HighDeserializer;
use rkyv::rancor::Strategy;
use rkyv::ser::Serializer;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

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
    async fn update_data_length(&mut self) -> eyre::Result<()> {
        let offset = (u32::default().aligned_size() * 6) as u64;
        // The multiplication must happen in u64: `last_page_id * PAGE_SIZE`
        // in u32 wraps once the file passes 4 GiB, and the wrapped position
        // lands inside a live early page, overwriting its header in place.
        self.data_file
            .seek(SeekFrom::Start(
                u64::from(self.last_page_id) * u64::from(PAGE_SIZE) + offset,
            ))
            .await?;
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&self.current_data_length)?;
        self.data_file.write_all(bytes.as_ref()).await?;
        Ok(())
    }

    /// Removes written byte ranges from the durable free-range list.
    ///
    /// This is persisted before the corresponding row bytes. A crash between
    /// those writes can leak reusable space, but can never leave a live row
    /// described as free and eligible to be overwritten after reload.
    fn consume_reusable_ranges(&mut self, used_links: impl IntoIterator<Item = Link>) -> bool {
        let free_ranges = std::mem::take(&mut self.info.inner.empty_links_list);
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
        let info = parse_page::<_, PAGE_SIZE>(&mut data_file, 0).await?;
        let file_length = data_file.metadata().await?.len();
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
        let last_page_header = parse_general_header_by_index(&mut data_file, page_id as u32).await?;

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
        persist_page(&mut page, file).await
    }

    async fn save_data(&mut self, link: Link, bytes: &[u8]) -> eyre::Result<()> {
        if self.consume_reusable_ranges([link]) {
            self.save_info().await?;
        }
        if link.page_id > self.last_page_id.into() {
            let mut page = GeneralPage {
                header: GeneralHeader::new(link.page_id, PageType::Data, 0.into()),
                inner: DataPage {
                    length: 0,
                    data: [0; 1],
                },
            };
            persist_page(&mut page, &mut self.data_file).await?;
            self.current_data_length = 0;
            self.last_page_id += 1;
        }
        // `current_data_length` mirrors the last page's persisted data_length:
        // the number of bytes occupied from the page start. Only a write that
        // lands on the last page AND ends past the currently occupied extent
        // grows it. Rewrites of an existing link and writes into reused free
        // ranges (which always sit inside previously occupied extents) must
        // not touch it: unconditionally adding `link.length` inflated the
        // persisted length on every hot-row update until it exceeded the page
        // capacity and a later batch persist sliced out of range.
        if u32::from(link.page_id) == self.last_page_id {
            let link_end = link
                .offset
                .checked_add(link.length)
                .ok_or_else(|| eyre::eyre!("link range {link:?} overflows u32"))?;
            if link_end > self.current_data_length {
                self.current_data_length = link_end;
                self.update_data_length().await?;
            }
        }
        update_at::<{ PAGE_SIZE }>(&mut self.data_file, link, bytes).await?;
        // `update_at` ends with a buffered `write_all` that `tokio::fs::File`
        // completes on a background blocking task. Flush before reporting the
        // save done so the bytes are visible to any other handle.
        self.data_file.flush().await?;
        Ok(())
    }

    async fn save_batch_data(&mut self, batch_data: BatchData) -> eyre::Result<()> {
        let used_links = batch_data.values().flat_map(|ops| ops.iter().map(|(link, _)| *link));
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
        if let Some(max) = ids_to_create.iter().max() {
            // High-water mark: every id in `ids_to_create` is > last_page_id by
            // construction, but state the monotonic invariant directly so a
            // future refactor of the filter above cannot regress it.
            self.last_page_id = self.last_page_id.max(*max);
        }
        let created_pages = ids_to_create
            .into_iter()
            .map(|id| GeneralPage {
                header: GeneralHeader::new(id.into(), PageType::Data, 0.into()),
                inner: DataPage {
                    length: 0,
                    data: [0; INNER_PAGE_SIZE],
                },
            })
            .collect::<Vec<_>>();
        let parsed_pages =
            parse_data_pages_batch::<PAGE_SIZE, INNER_PAGE_SIZE>(&mut self.data_file, ids_to_parse).await?;

        let updated_pages = vec![parsed_pages, created_pages]
            .into_iter()
            .flatten()
            .map(|mut page| {
                let id = page.header.page_id;
                let ops = batch_data
                    .get(&id)
                    .expect("should be available as pages parsed from these ids");
                for (link, bytes) in ops {
                    page.inner.update_at(*link, bytes)?;
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

        persist_pages_batch(updated_pages, &mut self.data_file).await?;
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
        persist_page(&mut self.info, &mut self.data_file).await?;
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

    use super::subtract_used_ranges;
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
}
