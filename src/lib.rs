use entries::{ConsumingEntry, ProducingEntry};
use fifo_inner::FastFifoInner;
use std::{fmt::Debug, sync::Arc};

pub(crate) mod atomic;
pub(crate) mod block;
pub(crate) mod entries;
pub(crate) mod error;
pub(crate) mod fifo_inner;
pub(crate) mod field;
pub(crate) mod atom_pair;
#[cfg(test)]
mod test;

/// Compiles and runs the README's example as a doctest.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

pub use error::Error;
pub use fifo_inner::FifoIndex;

pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Clone)]
pub struct FastFifo<T>(Arc<FastFifoInner<T>>);

/// This type allows for the construction of a FastFifo from a CAPACITY instead of a NUM_BLOCKS.
// pub struct CohortFastFifo<T, const CAPACITY: usize, const BLOCK_SIZE: usize>(PhantomData<T>);

// pub const fn ceiling_div(lhs: usize, rhs: usize) -> usize {
//     lhs / rhs + if lhs % rhs != 0 { 1 } else { 0 }
// }

// impl<T, const CAPACITY: usize, const BLOCK_SIZE: usize> CohortFastFifo<T, CAPACITY> {
//     /// At least enough blocks to get CAPACITY size
//     pub fn new() -> FastFifo<T, { ceiling_div(CAPACITY) }> {
//         FastFifo::new()
//     }
// }

impl<T> FastFifo<T> {
    pub fn new(num_blocks: usize, block_size: usize) -> Self {
        Self(Arc::new(FastFifoInner::new(num_blocks, block_size)))
    }

    pub fn try_get_producer_entry(&self) -> Result<ProducingEntry<'_, T>> {
        self.0.get_producer_entry()
    }

    pub fn push_in_place<F: FnOnce(*mut T)>(&self, producer: F) -> Result<()> {
        self.0.push_in_place(producer)
    }

    pub fn push(&self, val: T) -> Result<()> {
        self.0.push(val)
    }

    pub fn indexed_push(&self, val: T, index: FifoIndex) {
        self.0.indexed_push(val, index);
    }

    pub fn try_get_consumer_entry(&self) -> Result<ConsumingEntry<'_, T>> {
        self.0.get_consumer_entry()
    }

    pub fn pop_in_place<F: FnOnce(*mut T)>(&self, consumer: F) -> Result<()> {
        self.0.pop_in_place(consumer)
    }

    pub fn pop(&self) -> Result<T> {
        self.0.pop()
    }

    pub fn indexed_pop(&self) -> Result<(T, FifoIndex)> {
        self.0.indexed_pop()
    }
}

impl<T: Debug> Debug for FastFifo<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0.as_ref())
    }
}
