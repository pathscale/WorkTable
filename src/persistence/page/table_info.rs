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

// TODO: This must be modified to describe table structure. I think page intervals
//       can describe what lays in them. Like page 2-3 is primary index, 3 secondary1,
//       4-... data pages, so we need some way to describe this.

// TODO: Test all pages united in one file, start from basic situation with just
//       3 pages: info, primary index and data. And then try to modify this more.

// TODO: Minor. Add some schema description in `SpaceIndo`

/// Header that appears on every page before page's data.
#[derive(
    Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct SpaceInfo {
    id: space::Id,
    page_count: u32,
    name: SpaceName,
}