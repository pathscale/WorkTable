use rkyv::{Archive, Deserialize, Serialize};

use crate::in_memory::space;
use crate::persistence::page::GeneralHeader;

/// Length of [`GeneralHeader`].
///
/// ## Rkyv representation
///
/// Length of the values are:
///
/// * `page_id` - 4 bytes,
/// * `previous_id` - 4 bytes,
/// * `next_id` - 4 bytes,
/// * `page_type` - 2 bytes,
/// * `space_id` - 4 bytes,
///
/// **2 bytes are added by rkyv implicitly.**
pub const HEADER_LENGTH: usize = 20;

pub type SpaceName = String;

/// Header that appears on every page before page's data.
#[derive(
    Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct SpaceInfo {
    id: space::Id,
    page_count: u32,
    name: SpaceName,
}