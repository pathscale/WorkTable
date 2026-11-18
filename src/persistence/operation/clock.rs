use uuid::Uuid;

#[cfg(not(target_os = "none"))]
pub(crate) fn new_operation_uuid() -> Uuid {
    static CONTEXT: parking_lot::Mutex<uuid::ContextV7> = parking_lot::Mutex::new(uuid::ContextV7::new());
    let (seconds, nanos) = unix_time();
    let context = CONTEXT.lock();
    Uuid::new_v7(uuid::Timestamp::from_unix(&*context, seconds, nanos))
}

#[cfg(target_os = "none")]
pub(crate) fn new_operation_uuid() -> Uuid {
    use core::sync::atomic::{AtomicU64, Ordering};
    static LAST_STATE: parking_lot::Mutex<(u64, u16)> = parking_lot::Mutex::new((0, 0));
    static FALLBACK_SEQ: AtomicU64 = AtomicU64::new(1);

    let (seconds, nanos) = unix_time();
    let millis = seconds
        .saturating_mul(1_000)
        .saturating_add(u64::from(nanos / 1_000_000));
    let mut rand_bytes = [0u8; 10];
    if libc_rs::rand::fill_random(&mut rand_bytes).is_err() {
        let seq = FALLBACK_SEQ.fetch_add(1, Ordering::Relaxed);
        rand_bytes[..8].copy_from_slice(&seq.to_be_bytes());
    }
    let mut guard = LAST_STATE.lock();
    let (ts_ms, seq12) = if millis > guard.0 {
        let seed = u16::from_be_bytes([rand_bytes[0], rand_bytes[1]]) & 0x0fff;
        *guard = (millis, seed);
        (millis, seed)
    } else {
        let next_seq = guard.1.wrapping_add(1) & 0x0fff;
        let next_ms = if next_seq == 0 { guard.0.saturating_add(1) } else { guard.0 };
        *guard = (next_ms, next_seq);
        (next_ms, next_seq)
    };
    drop(guard);

    let mut bytes = [0u8; 16];
    let ts_be = ts_ms.to_be_bytes();
    bytes[0..6].copy_from_slice(&ts_be[2..8]);
    bytes[6] = 0x70 | ((seq12 >> 8) as u8 & 0x0f);
    bytes[7] = (seq12 & 0xff) as u8;
    bytes[8..16].copy_from_slice(&rand_bytes[2..10]);
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

#[cfg(target_os = "none")]
fn unix_time() -> (u64, u32) {
    let secs = eko::time::epoch_secs();
    assert!(secs >= 0, "system clock precedes the Unix epoch");
    (secs as u64, 0)
}

#[cfg(unix)]
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

#[cfg(windows)]
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

#[cfg(not(any(unix, windows, target_os = "none")))]
compile_error!("WorkTable currently requires Unix, Windows, or eko OS services without std");
