use data_bucket::{update_at, Link};
use std::fs::File;

pub trait SpaceDataOps {
    fn save_data(&mut self, link: Link, bytes: &[u8]) -> eyre::Result<()>;
}

#[derive(Debug)]
pub struct SpaceData<const DATA_LENGTH: usize> {
    pub data_file: File,
}

impl<const DATA_LENGTH: usize> SpaceDataOps for SpaceData<DATA_LENGTH> {
    fn save_data(&mut self, link: Link, bytes: &[u8]) -> eyre::Result<()> {
        update_at::<{ DATA_LENGTH }>(&mut self.data_file, link, bytes)
    }
}
