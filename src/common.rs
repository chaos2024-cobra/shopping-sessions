//! Shared records, the input file table, and small utilities.

use std::sync::atomic::{AtomicBool, Ordering};

pub fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

/// Ask for transparent huge pages on a large table allocation, so random
/// probes into multi-GB tables do not thrash the TLB.
pub fn madvise_hugepage<T>(v: &[T]) {
    extern "C" {
        fn madvise(addr: *mut core::ffi::c_void, len: usize, advice: i32) -> i32;
    }
    const MADV_HUGEPAGE: i32 = 14;
    let ptr = v.as_ptr() as usize;
    let aligned = ptr & !0x1F_FFFF; // round down to 2 MiB
    let end = ptr + v.len() * std::mem::size_of::<T>();
    let len = end - aligned;
    if len >= 0x20_0000 {
        unsafe {
            madvise(aligned as *mut core::ffi::c_void, len, MADV_HUGEPAGE);
        }
    }
}

// ---------------------------------------------------------------- input files

/// Every input CSV is mmap'd once; a string in the input is identified by
/// `loc = (file_index << 48) | byte_offset`, its length carried alongside.
pub struct Files {
    pub maps: Vec<&'static [u8]>,
    /// (kind, byte range) per file for task building. kind: 0 events, 1 clicks, 2 links
    pub kind: Vec<u8>,
}

impl Files {
    #[inline]
    pub fn str_at(&self, loc: u64, len: u16) -> &'static [u8] {
        let f = (loc >> 48) as usize;
        let off = (loc & 0xFFFF_FFFF_FFFF) as usize;
        let end = off + len as usize;
        if f >= self.maps.len() {
            eprintln!("str_at: bad loc {loc:016x} len {len} (files {})", self.maps.len());
        }
        let m = self.maps[f.min(self.maps.len().saturating_sub(1))];
        &m[off.min(m.len())..end.min(m.len())]
    }
}

// ---------------------------------------------------------------- records

/// One parsed event line, held in event-id-hash buckets until dedup. 48 bytes.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct EvRec {
    pub eh_hi: u64,
    pub uh_hi: u64,
    pub loc: u64, // user_id string
    pub ingest: u32,
    pub ts_delta: i32, // ts - window_start; meaningful only if TS_OK
    pub price: u32,
    pub cat: u16,
    pub id_len: u16,
    pub eh_lo: u16,
    pub uh_lo: u16,
    pub kind: u8, // 0 view, 1 cart, 2 purchase, 3 bad type
    pub flags: u8,
}

pub const F_TS: u8 = 1; // ts valid and inside the event window
pub const F_USER: u8 = 2;
pub const F_PROD: u8 = 4;
pub const F_PRICE: u8 = 8;
pub const F_LATE: u8 = 16;

impl EvRec {
    #[inline]
    pub fn valid(&self) -> bool {
        self.flags & F_TS != 0
            && self.flags & F_USER != 0
            && self.flags & F_PROD != 0
            && self.kind != 3
            && (self.kind != 2 || self.flags & F_PRICE != 0)
            && self.flags & F_LATE != 0
    }
}

/// A deduped, valid event, in user-hash partitions. 32 bytes.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct EvRec2 {
    pub uh_hi: u64,
    pub loc: u64, // user_id string
    pub ts_delta: i32,
    pub price: u32,
    pub cat: u16,
    pub uh_lo: u16,
    pub id_len: u16,
    pub kind: u8,
}

/// One valid click, in user-hash partitions (click copies are identical, so
/// dedup is a no-op for the outputs). 40 bytes.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct ClRec {
    pub uh_hi: u64,
    pub user_loc: u64, // user_id string
    pub cid_loc: u64,  // click_id string
    pub ts_delta: i32, // ts - click_window_start
    pub camp: u16,
    pub uh_lo: u16,
    pub user_len: u16,
    pub cid_len: u16,
}

/// Event payload once grouped by person. 12 bytes.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct EvPay {
    pub ts_delta: i32,
    pub price: u32,
    pub cat_kind: u16, // cat << 2 | kind
}

/// Click payload once grouped by person. 16 bytes.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct ClPay {
    pub ts_delta: i32,
    pub camp: u16,
    pub cid_len: u16,
    pub cid_loc: u64,
}

/// One distinct-id group handed from grouping to the per-person walk. 32 bytes.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct GroupEnt {
    pub root_hi: u64,
    pub loc: u64, // this id's string
    pub run_off: u32, // (segment << 26) | offset within segment
    pub len: u32,
    pub root_lo: u16,
    pub id_len: u16,
    pub is_click: u8,
}

/// A link-table entry per partition, sorted by (uh_hi, uh_lo). 32 bytes.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct LinkEnt {
    pub uh_hi: u64,
    pub root_hi: u64,
    pub name_loc: u64, // component name string
    pub root_lo: u16,
    pub name_len: u16,
    pub uh_lo: u16,
}

/// One side of a link row, collected during parsing. 24 bytes.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct LinkSide {
    pub h_hi: u64,
    pub loc: u64,
    pub pair: u64, // (worker << 32) | local pair index
    pub h_lo: u16,
    pub len: u16,
}

// ---------------------------------------------------------------- run_parallel

/// Run `f(thread_id, item)` for item in 0..nitems on `nthreads` scoped threads,
/// items claimed through an atomic counter.
pub fn run_parallel(nthreads: usize, nitems: usize, f: impl Fn(usize, usize) + Sync + Send) {
    if nitems == 0 {
        return;
    }
    let counter = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let n = nthreads.min(nitems).max(1);
        for t in 0..n {
            let counter = &counter;
            let f = &f;
            scope.spawn(move || {
                if t == 0 && n == 1 {
                    // single thread: skip atomics entirely
                    for i in 0..nitems {
                        f(t, i);
                    }
                    return;
                }
                loop {
                    let i = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if i >= nitems {
                        return;
                    }
                    f(t, i);
                }
            });
        }
    });
}

/// A shareable slice pointer, for disjoint per-index mutation behind
/// `run_parallel` (the caller guarantees no aliasing). Access it only through
/// the methods, so closures capture the wrapper (which is Send + Sync)
/// instead of the raw field.
pub struct Ptr<T>(pub *mut [T]);
unsafe impl<T> Send for Ptr<T> {}
unsafe impl<T> Sync for Ptr<T> {}
impl<T> Clone for Ptr<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for Ptr<T> {}
impl<T> Ptr<T> {
    pub fn new(v: &mut [T]) -> Self {
        Ptr(v as *mut [T])
    }
    #[inline]
    pub fn r(&self) -> &[T] {
        unsafe { &*self.0 }
    }
    #[inline]
    pub fn elem(&self, i: usize) -> &T {
        unsafe { &*(self.0 as *mut T).add(i) }
    }
    /// Disjoint-index mutable access; the caller guarantees no aliasing.
    #[inline]
    pub fn elem_mut(&self, i: usize) -> &mut T {
        unsafe { &mut *(self.0 as *mut T).add(i) }
    }
    #[inline]
    pub fn at(&self, i: usize) -> *mut T {
        unsafe { (self.0 as *mut T).add(i) }
    }
}

/// A simple parking-wait flag.
pub struct Flag(AtomicBool);
impl Flag {
    pub const fn new() -> Self {
        Flag(AtomicBool::new(false))
    }
    pub fn set(&self) {
        self.0.store(true, Ordering::Release);
    }
    pub fn wait(&self) {
        while !self.0.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
    }
}
