//! Fragmentation analysis for vacuum operations.
//!
//! This module provides types and methods for analyzing data page fragmentation
//! in [`WorkTable`]. Fragmentation information is used by the vacuum system to
//! identify pages that need defragmentation or migration.
//!
//! # Overview
//!
//! - [`FragmentationInfo`] - Aggregated fragmentation metrics for an entire table
//! - [`PageFragmentationInfo`] - Per-page fragmentation details
//! - Extension methods on [`EmptyLinkRegistry`] for calculating fragmentation
//!
//! [`WorkTable`]: crate::table::WorkTable

use std::collections::HashMap;

use data_bucket::page::PageId;
use data_bucket::{INNER_PAGE_SIZE, Link};

use crate::in_memory::EmptyLinkRegistry;

/// Aggregated fragmentation information for a full [`WorkTable`].
///
/// [`WorkTable`]: crate::table::WorkTable
#[derive(Debug, Clone)]
pub struct FragmentationInfo {
    pub table_name: &'static str,
    pub total_pages: usize,
    pub page_size: usize,
    pub per_page_info: Vec<PageFragmentationInfo>,
    pub overall_fragmentation_ratio: f64,
    pub total_empty_bytes: u64,
}

impl FragmentationInfo {
    /// Creates new fragmentation info from component parts of
    /// [`PageFragmentationInfo`].
    pub fn new(
        table_name: &'static str,
        total_pages: usize,
        per_page_info: Vec<PageFragmentationInfo>,
    ) -> Self {
        let page_size = per_page_info
            .first()
            .map(|i| i.page_size)
            .unwrap_or(INNER_PAGE_SIZE);
        let total_empty_bytes: u64 = per_page_info.iter().map(|i| i.empty_bytes as u64).sum();
        let filled_bytes = total_pages as u64 * page_size as u64;
        Self {
            page_size,
            table_name,
            total_pages,
            per_page_info,
            overall_fragmentation_ratio: filled_bytes as f64 / total_empty_bytes as f64,
            total_empty_bytes,
        }
    }
}

/// Fragmentation information for a single data [`Page`].
///
/// [`Page`]: crate::in_memory::Data
#[derive(Debug, Clone)]
pub struct PageFragmentationInfo {
    pub page_id: PageId,
    pub links: Vec<Link>,
    pub page_size: usize,
    pub empty_bytes: u32,
    pub filled_empty_ratio: f64,
}

impl<const DATA_LENGTH: usize> EmptyLinkRegistry<DATA_LENGTH> {
    /// Returns all empty [`Link`]s for a specific page.
    pub fn get_page_empty_links(&self, page_id: PageId) -> Vec<Link> {
        self.page_links_map
            .get(&page_id)
            .map(|(_, link)| *link)
            .collect()
    }

    /// Calculates [`PageFragmentationInfo`] information for all pages with
    /// empty [`Link`]s.
    pub fn get_per_page_info(&self) -> Vec<PageFragmentationInfo> {
        let mut page_empty_data: HashMap<PageId, (u32, Vec<Link>)> = HashMap::new();

        {
            let _op_lock = self.op_lock.lock();
            let iter = self.page_links_map.iter();
            for (page_id, link) in iter {
                let entry = page_empty_data.entry(*page_id).or_default();
                entry.0 += link.length;
                entry.1.push(link.clone());
            }
        }

        let mut per_page_data: Vec<PageFragmentationInfo> = page_empty_data
            .into_iter()
            .map(|(page_id, (empty_bytes, links))| {
                let filled_empty_ratio = if empty_bytes > 0 {
                    let filled_bytes = DATA_LENGTH.saturating_sub(empty_bytes as usize);
                    filled_bytes as f64 / empty_bytes as f64
                } else {
                    0.0
                };

                PageFragmentationInfo {
                    page_size: DATA_LENGTH,
                    links,
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
