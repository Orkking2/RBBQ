use super::{
    Error, Result,
    atomic::AtomicField,
    block::{AllocState, Block, ReserveState},
    entries::{ConsumingEntry, ProducingEntry},
};
use crate::field::{Field, FieldConfig};
use std::{fmt::Debug, mem::MaybeUninit, sync::atomic::Ordering};

pub(crate) struct FastFifoInner<T> {
    phead: AtomicField,
    chead: AtomicField,
    num_blocks: usize,
    block_size: usize,
    blocks: *mut [Block<T>],
}

#[rustfmt::skip]
unsafe impl<T> Send for FastFifoInner<T> {}
#[rustfmt::skip]
unsafe impl<T> Sync for FastFifoInner<T> {}

enum AdvancePheadState {
    Success,
    NoEntry,
    NotAvailable,
}

#[derive(Clone, Copy)]
pub struct FifoIndex {
    pub block_idx: usize,
    pub sub_block_idx: usize,
}

impl<T> FastFifoInner<T> {
    pub fn new(num_blocks: usize, block_size: usize) -> Self {
        assert!(
            num_blocks > 1,
            "If you want only one block, use a different Fifo."
        );

        Self {
            phead: AtomicField::new(FieldConfig {
                index_max: num_blocks,
                version: 0,
                index: 0,
            }),
            chead: AtomicField::new(FieldConfig {
                index_max: num_blocks,
                version: 0,
                index: 0,
            }),
            blocks: Box::into_raw({
                let mut vec = Vec::with_capacity(num_blocks);
                vec.extend((0..num_blocks).map(|i| {
                    if i == 0 {
                        Block::new(block_size)
                    } else {
                        Block::new_full(block_size)
                    }
                }));
                vec.into_boxed_slice()
            }),

            // array::from_fn(|i| {
            //     if i == 0 {
            //         Default::default()
            //     } else {
            //         // {.idx = BLOCK_SIZE}
            //         Block {
            //             allocated: AtomicField::new(FieldConfig {
            //                 index: BLOCK_SIZE,
            //                 ..Default::default()
            //             }),
            //             committed: AtomicField::new(FieldConfig {
            //                 index: BLOCK_SIZE,
            //                 ..Default::default()
            //             }),
            //             reserved: AtomicField::new(FieldConfig {
            //                 // index: BLOCK_SIZE,
            //                 ..Default::default()
            //             }),
            //             consumed: AtomicField::new(FieldConfig {
            //                 index: BLOCK_SIZE,
            //                 ..Default::default()
            //             }),

            //             ..Default::default()
            //         }
            //     }
            // }),
            num_blocks,
            block_size,
        }
    }

    fn get_phead_and_block(&self) -> (Field, &Block<T>) {
        let ph = self.phead.load(Ordering::Relaxed);
        (ph, &unsafe { &*self.blocks }[ph.get_index()])
    }

    fn advance_phead(&self, ph: Field) -> AdvancePheadState {
        let ref nblk = unsafe { &*self.blocks }[(ph.get_index() + 1) % self.num_blocks];
        // /* retry-new begin
        let consumed = nblk.consumed.load(Ordering::Acquire);

        if consumed.get_version() < ph.get_version()
            || (consumed.get_version() == ph.get_version()
                && consumed.get_index() != self.block_size)
        {
            let reserved = nblk.reserved.load(Ordering::Relaxed);

            if reserved.get_index() == consumed.get_index() {
                AdvancePheadState::NoEntry
            } else {
                AdvancePheadState::NotAvailable
            }
        }
        // */ // retry-new end
        /* drop-old begin
        let committed = nblk.committed.load(Ordering::Aquire);
        if committed.get_version() == ph.get_version() && committed.get_index() != BLOCK_SIZE {
            AdvancePheadState::NotAvailable
        }
        // */ // drop-old end
        else {
            let new_field = FieldConfig {
                index_max: self.block_size,
                version: ph.get_version() + 1,
                index: 0,
            }
            .into();

            nblk.committed.fetch_max(new_field, Ordering::Relaxed);
            nblk.allocated.fetch_max(new_field, Ordering::Relaxed);

            self.phead
                .fetch_max(ph.version_inc_add(1), Ordering::Relaxed);

            AdvancePheadState::Success
        }
    }

    fn get_chead_and_block(&self) -> (Field, &Block<T>) {
        let ch = self.chead.load(Ordering::Relaxed);
        (ch, &unsafe { &*self.blocks }[ch.get_index()])
    }

    #[allow(unused_variables)]
    fn advance_chead(&self, ch: Field, version: usize) -> bool {
        let ref nblk = unsafe { &*self.blocks }[(ch.get_index() + 1) % self.num_blocks];
        let committed = nblk.committed.load(Ordering::Acquire);

        // /* retry-new begin
        if committed.get_version() != ch.get_version() + 1 {
            return false;
        }
        let new_field = FieldConfig {
            index_max: self.block_size,
            version: ch.get_version() + 1,
            index: 0,
        }
        .into();
        nblk.consumed.fetch_max(new_field, Ordering::Relaxed);
        nblk.reserved.fetch_max(new_field, Ordering::Relaxed);
        // */ // retry-new end
        /* drop-old begin
        if committed.get_version() < version + if ch.get_index() == 0 { 1 } else { 0 } {
            return false;
        }
        nblk.reserved.fetch_max(
            FieldConfig {
                index: 0,
                version: committed.get_version(),
            },
            Ordering::Relaxed,
        );
        // */ // drop-old end

        self.chead
            .fetch_max(ch.version_inc_add(1), Ordering::Relaxed);
        true
    }

    /// Try to reserve a production entry
    pub fn get_producer_entry(&self) -> Result<ProducingEntry<'_, T>> {
        loop {
            let (ph, blk) = self.get_phead_and_block();
            match blk.allocate_entry(ph.get_index()) {
                AllocState::Allocated(entry_description) => {
                    break Ok(ProducingEntry(entry_description));
                }
                AllocState::BlockDone => match self.advance_phead(ph) {
                    AdvancePheadState::NoEntry => break Err(Error::Full),
                    AdvancePheadState::NotAvailable => break Err(Error::Busy),
                    AdvancePheadState::Success => { /* continue loop */ }
                },
            }
        }
    }

    /// F produces T at address *mut T
    pub fn push_in_place<F: FnOnce(*mut T)>(&self, producer: F) -> Result<()> {
        self.get_producer_entry()
            .map(|mut entry| entry.produce_t_in_place(producer))
    }

    pub fn push(&self, val: T) -> Result<()> {
        self.push_in_place(|ptr| unsafe { ptr.write(val) })
    }

    pub fn indexed_push(&self, val: T, index: FifoIndex) {
        let blk = unsafe { &(*self.blocks)[index.block_idx] };
        blk.allocated.fetch_add(1, Ordering::Relaxed);
        unsafe { (*blk.entries)[index.sub_block_idx].write(val) };
        blk.committed.fetch_add(1, Ordering::Release);
    }

    pub fn get_consumer_entry(&self) -> Result<ConsumingEntry<'_, T>> {
        loop {
            let (ch, blk) = self.get_chead_and_block();
            match blk.reserve_entry() {
                ReserveState::BlockDone(version) => {
                    if !self.advance_chead(ch, version) {
                        break Err(Error::Empty);
                    } else {
                        /* continue loop */
                    }
                }
                ReserveState::Reserved(entry_description) => {
                    break Ok(ConsumingEntry(entry_description));
                }
                ReserveState::NoEntry => break Err(Error::Empty),
                ReserveState::NotAvailable => break Err(Error::Busy),
            }
        }
    }

    /// F consumes T at address *mut T
    pub fn pop_in_place<F: FnOnce(*mut T)>(&self, consumer: F) -> Result<()> {
        self.get_consumer_entry()
            .map(|mut entry| entry.consume_t_in_place(consumer))
    }

    pub fn pop(&self) -> Result<T> {
        let mut uninit_mem = MaybeUninit::uninit();

        self.pop_in_place(|ptr| {
            uninit_mem.write(unsafe { ptr.read() });
        })
        .map(|()| unsafe { uninit_mem.assume_init() })
    }

    pub fn indexed_pop(&self) -> Result<(T, FifoIndex)> {
        let mut val = MaybeUninit::uninit();
        let mut idx = MaybeUninit::uninit();

        self.get_consumer_entry()
            .map(|mut entry| {
                entry.consume_t_in_place(|ptr| {
                    val.write(unsafe { ptr.read() });
                });
                idx.write(entry.0.index);
            })
            .map(|()| unsafe { (val.assume_init(), idx.assume_init()) })
    }
}

impl<T: Debug> Debug for FastFifoInner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(unsafe { &*self.blocks }).finish()
    }
}

impl<T> Drop for FastFifoInner<T> {
    fn drop(&mut self) {
        unsafe { &mut *self.blocks }
            .iter_mut()
            .for_each(Block::drop);
    }
}
