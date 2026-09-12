use uuid::Uuid;

#[cfg(feature = "std")]
pub(crate) fn new_operation_uuid() -> Uuid {
    Uuid::now_v7()
}

// ContextV7 preserves process-local ordering across equal or regressing clock
// readings. OS entropy remains supplied by uuid/getrandom without Rust std.
#[cfg(not(feature = "std"))]
pub(crate) fn new_operation_uuid() -> Uuid {
    static CONTEXT: parking_lot::Mutex<uuid::ContextV7> = parking_lot::Mutex::new(uuid::ContextV7::new());
    let (seconds, nanos) = unix_time();
    let context = CONTEXT.lock();
    Uuid::new_v7(uuid::Timestamp::from_unix(&*context, seconds, nanos))
}

#[cfg(all(not(feature = "std"), unix))]
fn unix_time() -> (u64, u32) {
    let mut value = core::mem::MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: the OS writes a timespec to a valid, aligned output pointer.
    let result = unsafe { libc::clock_gettime(libc::CLOCK_REALTIME, value.as_mut_ptr()) };
    assert_eq!(result, 0, "system clock unavailable for operation identifiers");
    // SAFETY: a successful clock_gettime initialized both fields.
    let value = unsafe { value.assume_init() };
    assert!(value.tv_sec >= 0, "system clock precedes the Unix epoch");
    (value.tv_sec as u64, value.tv_nsec as u32)
}

#[cfg(all(not(feature = "std"), windows))]
fn unix_time() -> (u64, u32) {
    use windows_sys::Win32::Foundation::FILETIME;
    use windows_sys::Win32::System::SystemInformation::GetSystemTimePreciseAsFileTime;
    let mut value = core::mem::MaybeUninit::<FILETIME>::uninit();
    // SAFETY: the API initializes the FILETIME at this valid output pointer.
    let value = unsafe {
        GetSystemTimePreciseAsFileTime(value.as_mut_ptr());
        value.assume_init()
    };
    let ticks = (u64::from(value.dwHighDateTime) << 32) | u64::from(value.dwLowDateTime);
    let ticks = ticks
        .checked_sub(116_444_736_000_000_000)
        .expect("system clock precedes the Unix epoch");
    (ticks / 10_000_000, ((ticks % 10_000_000) * 100) as u32)
}

#[cfg(all(not(feature = "std"), not(any(unix, windows))))]
compile_error!("WorkTable currently requires Unix or Windows OS services without std");
