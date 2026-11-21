//! Small utilities: a spin lock and timing helpers.

use std::sync::atomic::{AtomicBool, Ordering};

pub struct SpinLock(AtomicBool);

pub struct SpinGuard<'a>(&'a AtomicBool);

impl SpinLock {
    #[inline]
    pub const fn new() -> Self {
        SpinLock(AtomicBool::new(false))
    }

    #[inline]
    pub fn lock(&self) -> SpinGuard<'_> {
        while self
            .0
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        SpinGuard(&self.0)
    }
}

impl Drop for SpinGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Wall-clock milliseconds for phase timing.
pub fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}
