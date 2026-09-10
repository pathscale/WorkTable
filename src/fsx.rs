//! The filesystem, without a runtime attached to it.
//!
//! A thin shim over [`nagoya::io`], which is where these operations live now.
//! They were here first, privately, and `data_bucket` could not name them at
//! all, so the two halves of one storage engine disagreed about what a file
//! was. The whole crate still goes through this module, so swapping backends
//! is still swapping one file.
//!
//! # Why the calls block
//!
//! Neither `tokio::fs` nor `async-fs` performs asynchronous file I/O: both hand
//! a blocking `std::fs` call to a thread pool, and what that buys is not
//! occupying a runtime worker rather than any actual overlap. It is not free.
//! Measured on this crate's scattered-update path, `tokio::fs` ran 12,316 rows
//! per second against 74,728 for the same code on blocking `std::fs`, and cold
//! reopen is 2.8x faster on an Arctic index without it.
//!
//! Because the calls block, the persistence engine should own a thread rather
//! than share a runtime's worker pool. It was never waiting on the disk through
//! a runtime anyway: the path measures 89 voluntary context switches across
//! 25,000 inserts.

/// A file this crate reads and writes.
pub type File = nagoya::io::HostFile;

/// Open a snapshot for reading without requesting write permission.
pub async fn open_read_only(path: impl AsRef<std::path::Path>) -> Result<File, nagoya::io::Error> {
    std::fs::File::open(path).map(File::new).map_err(Into::into)
}

pub use nagoya::io::{
    Error, SeekFrom, append, create, create_dir_all, open, open_or_create, read, remove_dir_all, remove_file, rename,
    write,
};

/// How long the file at `path` is, without opening it.
///
/// Named for what it does. `nagoya::io::metadata` returns the length rather
/// than a metadata handle, because a length is all anything here ever wanted.
pub use nagoya::io::metadata;

/// The operations no I/O trait carries, as free functions.
///
/// They are methods on [`nagoya::io::File`]; these wrappers exist so call sites
/// read the same as they did when this module owned the implementation, and so
/// that a backend swap stays a change to one file.
pub async fn sync_all(file: &mut File) -> Result<(), Error> {
    nagoya::io::File::sync_all(file).await
}

pub async fn sync_data(file: &mut File) -> Result<(), Error> {
    nagoya::io::File::sync_data(file).await
}

pub async fn set_len(file: &mut File, length: u64) -> Result<(), Error> {
    nagoya::io::File::set_length(file, length).await
}

/// How long an already-open file is.
pub async fn file_metadata(file: &mut File) -> Result<u64, Error> {
    nagoya::io::File::length(file).await
}
