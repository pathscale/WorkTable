use super::MemStat;

#[macro_export]
macro_rules! impl_memstat_zero {
    ($($t:ty),*) => {
        $(
            impl MemStat for $t {
                fn heap_size(&self) -> usize { 0 }
                fn used_size(&self) -> usize { 0 }
            }
        )*
    };
}

impl_memstat_zero!(
    u8,
    i8,
    u16,
    i16,
    u32,
    i32,
    u64,
    i64,
    usize,
    isize,
    f32,
    f64,
    bool,
    char,
    u128,
    i128,
    core::num::NonZeroU8,
    core::num::NonZeroU16,
    core::num::NonZeroU32,
    core::num::NonZeroU64,
    core::num::NonZeroU128,
    core::num::NonZeroUsize,
    core::num::NonZeroI8,
    core::num::NonZeroI16,
    core::num::NonZeroI32,
    core::num::NonZeroI64,
    core::num::NonZeroI128,
    core::num::NonZeroIsize,
    core::time::Duration,
    std::time::SystemTime,
    std::time::Instant
);

impl_memstat_zero!(
    [u8],
    [i8],
    [u16],
    [i16],
    [u32],
    [i32],
    [u64],
    [i64],
    [usize],
    [isize],
    [f32],
    [f64],
    [bool],
    [char],
    [u128],
    [i128]
);
