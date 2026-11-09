//! Dropping a queue must return all of its memory. This is the only test in
//! the binary, and only allocations on the test's own thread are counted.

use rbbq::FastFifo;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicIsize, Ordering};

static LIVE_BYTES: AtomicIsize = AtomicIsize::new(0);

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
}

struct CountingAlloc;

fn record(delta: isize) {
    if COUNTING.try_with(Cell::get).unwrap_or(false) {
        LIVE_BYTES.fetch_add(delta, Ordering::Relaxed);
    }
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size() as isize);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        record(-(layout.size() as isize));
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

#[test]
fn dropping_a_queue_frees_its_blocks_and_entries() {
    COUNTING.with(|c| c.set(true));
    let fifo = FastFifo::new(4, 16);
    assert!(LIVE_BYTES.load(Ordering::Relaxed) > 0);
    for i in 0..20 {
        fifo.push(vec![i; 8]).unwrap();
    }
    drop(fifo.pop());
    drop(fifo);
    COUNTING.with(|c| c.set(false));
    assert_eq!(LIVE_BYTES.load(Ordering::Relaxed), 0);
}
