use crate::{block::Block, fifo_inner::FifoIndex};
use std::sync::atomic::Ordering;

/// Think of this as an allocator giving you exactly one *mut T.
pub struct ProducingEntry<'a, T>(pub(crate) EntryDescription<'a, T>);

impl<'a, T> ProducingEntry<'a, T> {
    pub fn produce_t_in_place<F: FnOnce(*mut T)>(&mut self, producer: F) {
        self.0.modify_t_in_place(producer);
    }
}

impl<'a, T> Drop for ProducingEntry<'a, T> {
    fn drop(&mut self) {
        // All subsequent reads must be visible after this increment.
        self.0.block.committed.fetch_add(1, Ordering::Release);
    }
}

/// Think of this as a deallocator, letting you do what needs to be done with *mut T before it gets freed.
#[repr(transparent)]
pub struct ConsumingEntry<'a, T>(pub(crate) EntryDescription<'a, T>);

impl<'a, T> ConsumingEntry<'a, T> {
    pub fn consume_t_in_place<F: FnOnce(*mut T)>(&mut self, consumer: F) {
        self.0.modify_t_in_place(consumer);
    }
}

impl<'a, T> Drop for ConsumingEntry<'a, T> {
    fn drop(&mut self) {
        self.0.block.consumed.fetch_add(1, Ordering::Release);
    }
}

pub(crate) struct EntryDescription<'a, T> {
    pub(crate) block: &'a Block<T>,
    pub(crate) index: FifoIndex,
    #[allow(dead_code)]
    pub(crate) version: usize,
}

impl<'a, T> EntryDescription<'a, T> {
    /// Modify *mut T in-place
    pub fn modify_t_in_place<F: FnOnce(*mut T)>(&mut self, modifier: F) {
        modifier(unsafe { &*self.block.entries }[self.index.sub_block_idx].as_ptr() as *mut T)
    }
}
