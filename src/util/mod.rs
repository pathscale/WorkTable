pub(crate) mod epoch;
mod offset_eq_link;
mod optimized_vec;
mod ordered_float;

pub use offset_eq_link::OffsetEqLink;
pub use optimized_vec::OptimizedVec;
pub use ordered_float::{OrderedF32Def, OrderedF64Def};

/// Give up the rest of the timeslice after a spin has stopped paying.
#[inline]
pub(crate) fn yield_now() {
    #[cfg(unix)]
    unsafe {
        let _ = libc::sched_yield();
    }
    #[cfg(not(unix))]
    core::hint::spin_loop();
}
