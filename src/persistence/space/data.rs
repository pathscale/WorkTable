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

#[derive(Debug)]
pub struct SpaceData<PkGenState, const INNER_PAGE_SIZE: usize, const PAGE_SIZE: u32> {
    pub info: GeneralPage<SpaceInfoPage<PkGenState>>,
    pub last_page_id: u32,
    pub current_data_length: u32,
    pub data_file: File,
}

impl<PkGenState, const INNER_PAGE_SIZE: usize, const PAGE_SIZE: u32> SpaceData<PkGenState, INNER_PAGE_SIZE, PAGE_SIZE> {
    async fn update_data_length(&mut self) -> eyre::Result<()> {
        let offset = (u32::default().aligned_size() * 6) as u32;
        self.data_file
            .seek(SeekFrom::Start((self.last_page_id * PAGE_SIZE + offset) as u64))
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
        let mut changed = false;

        for used in used_links {
            let used_start = u64::from(used.offset);
            let used_end = used_start + u64::from(used.length);
            let mut remaining = Vec::with_capacity(self.info.inner.empty_links_list.len() + 1);

            for free in self.info.inner.empty_links_list.drain(..) {
                if free.page_id != used.page_id {
                    remaining.push(free);
                    continue;
                }

                let free_start = u64::from(free.offset);
                let free_end = free_start + u64::from(free.length);
                let overlap_start = free_start.max(used_start);
                let overlap_end = free_end.min(used_end);
                if overlap_start >= overlap_end {
                    remaining.push(free);
                    continue;
                }

                changed = true;
                if free_start < overlap_start {
                    remaining.push(Link {
                        page_id: free.page_id,
                        offset: free.offset,
                        length: (overlap_start - free_start) as u32,
                    });
                }
                if overlap_end < free_end {
                    remaining.push(Link {
                        page_id: free.page_id,
                        offset: overlap_end as u32,
                        length: (free_end - overlap_end) as u32,
                    });
                }
            }

            self.info.inner.empty_links_list = remaining;
        }

        if changed {
            self.info.inner.empty_links_list.sort_by_key(|link| {
                let page_id: u32 = link.page_id.into();
                (page_id, link.offset)
            });
        }
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
        let page_id = file_length / PAGE_SIZE as u64;
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
        self.current_data_length += link.length;
        self.update_data_length().await?;
        update_at::<{ PAGE_SIZE }>(&mut self.data_file, link, bytes).await?;
        // `update_at` ends with a buffered `write_all` that `tokio::fs::File`
        // completes on a background blocking task. Flush before reporting the
        // save done so the bytes are visible to any other handle.
        self.data_file.flush().await?;
        Ok(())
    }

    async fn save_batch_data(&mut self, batch_data: BatchData) -> eyre::Result<()> {
        let used_links = batch_data.values().flat_map(|ops| ops.iter().map(|(link, _)| *link));
        if self.consume_reusable_ranges(used_links.collect::<Vec<_>>()) {
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

        persist_pages_batch(updated_pages, &mut self.data_file).await?;
        // The batch's last page write is a buffered `write_all`; flush so the
        // batch is visible to other handles once it reports done.
        self.data_file.flush().await?;

        Ok(())
    }

    async fn reclaim_data_pages(&mut self, page_ids: Vec<data_bucket::page::PageId>) -> eyre::Result<()> {
        let mut page_ids = page_ids
            .into_iter()
            .filter(|page_id| {
                let id: u32 = (*page_id).into();
                id != 0 && id <= self.last_page_id
            })
            .collect::<Vec<_>>();
        page_ids.sort_unstable();
        page_ids.dedup();

        if page_ids.is_empty() {
            return Ok(());
        }

        self.info
            .inner
            .empty_links_list
            .retain(|link| !page_ids.contains(&link.page_id));
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
