use std::fs::File;

use data_bucket::{update_at, Link};

use crate::persistence::space::open_or_create_file;
use crate::persistence::SpaceDataOps;

#[derive(Debug)]
pub struct SpaceData<const DATA_LENGTH: usize> {
    pub data_file: File,
}

impl<const DATA_LENGTH: usize> SpaceDataOps for SpaceData<DATA_LENGTH> {
    fn from_table_files_path<S: AsRef<str>>(path: S) -> eyre::Result<Self> {
        let data_file = open_or_create_file(path)?;
        Ok(Self { data_file })
    }

    fn save_data(&mut self, link: Link, bytes: &[u8]) -> eyre::Result<()> {
        update_at::<{ DATA_LENGTH }>(&mut self.data_file, link, bytes)
    }
}
