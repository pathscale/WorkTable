use std::collections::HashMap;

use data_bucket::Link;
use data_bucket::page::PageId;

use crate::in_memory::EmptyLinkRegistry;

/// Fragmentation info for a single data [`Page`].
///
/// [`Page`]: crate::in_memory::Data
#[derive(Debug, Copy, Clone)]
pub struct PageFragmentationInfo<const DATA_LENGTH: usize> {
    pub page_id: PageId,
    pub empty_bytes: u32,
    /// Ratio of filled bytes to empty bytes. Higher means more utilized.
    pub filled_empty_ratio: f64,
}

impl<const DATA_LENGTH: usize> EmptyLinkRegistry<DATA_LENGTH> {
    pub fn get_page_empty_links(&self, page_id: PageId) -> Vec<Link> {
        self.page_links_map.get(&page_id).map(|(_, link)| *link).collect()
    }

    pub fn get_per_page_info(&self) -> Vec<PageFragmentationInfo<DATA_LENGTH>> {
        let mut page_empty_bytes: HashMap<PageId, u32> = HashMap::new();

        for (page_id, link) in self.page_links_map.iter() {
            let entry = page_empty_bytes.entry(*page_id).or_insert(0);
            *entry += link.length;
        }

        let mut per_page_data: Vec<PageFragmentationInfo<DATA_LENGTH>> = page_empty_bytes
            .into_iter()
            .map(|(page_id, empty_bytes)| {
                let filled_empty_ratio = if empty_bytes > 0 {
                    let filled_bytes = DATA_LENGTH.saturating_sub(empty_bytes as usize);
                    filled_bytes as f64 / empty_bytes as f64
                } else {
                    0.0
                };

                PageFragmentationInfo {
                    page_id,
                    empty_bytes,
                    filled_empty_ratio,
                }
            })
            .collect();

        per_page_data.sort_by_key(|info| info.page_id);

        per_page_data
    }
}
