mod data;
mod link;

pub use data::DATA_INNER_LENGTH;
pub use link::Link;
pub use {data::Data, data::ExecutionError as DataExecutionError};
