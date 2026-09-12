#[cfg(feature = "s3-support")]
pub mod database_s3;
#[cfg(feature = "s3-support")]
pub mod s3_support;

#[cfg(feature = "s3-support")]
pub use database_s3::{DatabaseS3DiskConfig, DatabaseS3PersistenceEngine};

#[cfg(feature = "s3-support")]
pub use s3_support::*;
