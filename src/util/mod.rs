pub(crate) mod epoch;
mod offset_eq_link;
mod optimized_vec;
mod ordered_float;

pub use offset_eq_link::OffsetEqLink;
pub use optimized_vec::OptimizedVec;
pub use ordered_float::{OrderedF32Def, OrderedF64Def};

/// Give up the rest of the timeslice after a spin has stopped paying.
///
/// Under `std` that is the operating system's yield. Without one there is no
/// scheduler to yield to, so the spin hint is the whole of what can be done.
#[inline]
pub(crate) fn yield_now() {
    #[cfg(feature = "std")]
    std::thread::yield_now();
    #[cfg(not(feature = "std"))]
    core::hint::spin_loop();
}
