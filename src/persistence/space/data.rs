use std::fs::File;

use data_bucket::{update_at, Link};

use crate::persistence::space::open_or_create_file;
use crate::persistence::SpaceDataOps;
use crate::prelude::WT_DATA_EXTENSION;

#[derive(Debug)]
pub struct SpaceData<const DATA_LENGTH: u32> {
    pub data_file: File,
}

impl<const DATA_LENGTH: u32> SpaceDataOps for SpaceData<DATA_LENGTH> {
    fn from_table_files_path<S: AsRef<str>>(path: S) -> eyre::Result<Self> {
        let path = format!("{}/{}", path.as_ref(), WT_DATA_EXTENSION);
        let data_file = open_or_create_file(path)?;
        println!("data ok");
        Ok(Self { data_file })
    }

    fn save_data(&mut self, link: Link, bytes: &[u8]) -> eyre::Result<()> {
        update_at::<{ DATA_LENGTH }>(&mut self.data_file, link, bytes)
    }
}
