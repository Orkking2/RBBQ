use crate::field::Field;
use std::ops::Deref;

#[cfg(not(loom))]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};

// pub trait AtomPair {
//     fn load_take(&self) -> Field;
//     fn fetch_max_take(&self, val: Field) -> Field;
//     fn load_give(&self) -> Field;
//     fn incr_give(&self);
//     fn fetch_max_give(&self, val: Field) -> Field;

//     /// Returns old (give, take)
//     fn fetch_max_both(&self, val: Field) -> (Field, Field) {
//         (self.fetch_max_give(val), self.fetch_max_take(val))
//     }
// }

#[repr(align(128))]
pub struct Line128<T>(T);

impl<T> Deref for Line128<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> From<T> for Line128<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

pub struct AtomicPair {
    index_max: usize,
    take: Line128<AtomicUsize>,
    give: Line128<AtomicUsize>,
}

impl From<Field> for AtomicPair {
    fn from(value: Field) -> Self {
        Self::from_raw_parts(value.get_index_max(), value.get_raw_inner())
    }
}

impl AtomicPair {
    pub fn from_raw_parts(index_max: usize, inner: usize) -> Self {
        Self {
            index_max,
            take: AtomicUsize::new(inner).into(),
            give: AtomicUsize::new(inner).into(),
        }
    }

    pub fn index_max(&self) -> usize {
        self.index_max
    }
    // }

    // impl AtomPair for AtomicPair {
    pub fn load_take(&self) -> Field {
        Field::from_raw_parts(self.index_max, self.take.load(Ordering::Relaxed))
    }

    pub fn fetch_max_take(&self, val: Field) -> Field {
        Field::from_raw_parts(
            self.index_max,
            self.take.fetch_max(val.get_raw_inner(), Ordering::Relaxed),
        )
    }

    /// Must be aquire so previous give stores are seen before this one is loaded
    pub fn load_give(&self) -> Field {
        Field::from_raw_parts(self.index_max, self.give.load(Ordering::Acquire))
    }

    /// Must be release so subsequent give loads are seen after this one is stored
    pub fn incr_give(&self) {
        self.give.fetch_add(1, Ordering::Release);
    }

    /// This is for resetting give idx, it does not need to be ordered
    pub fn fetch_max_give(&self, val: Field) -> Field {
        Field::from_raw_parts(
            self.index_max,
            self.give.fetch_max(val.get_raw_inner(), Ordering::Relaxed),
        )
    }

    pub fn fetch_max_both(&self, val: Field) -> (Field, Field) {
        (self.fetch_max_give(val), self.fetch_max_take(val))
    }
}

// /// Optimized for singular access -- since there is only one thread accessing
// /// we can guarantee that every idx less than give has completed,
// pub struct NonAtomicPair {
//     index_max: usize,
//     give: Line128<AtomicUsize>,
// }

// impl From<Field> for NonAtomicPair {
//     fn from(value: Field) -> Self {
//         Self::from_raw_parts(value.get_index_max(), value.get_raw_inner())
//     }
// }

// impl NonAtomicPair {
//     fn from_raw_parts(index_max: usize, inner: usize) -> Self {
//         Self {
//             index_max,
//             give: AtomicUsize::new(inner).into(),
//         }
//     }
// }

// impl AtomPair for NonAtomicPair {
//     /// Take is always the same as give -- there is no reserving when there is only one thread.
//     fn load_take(&self) -> Field {
//         Field::from_raw_parts(self.index_max, self.give.load(Ordering::Relaxed))
//     }

//     /// Just like `load_take`, this just loads give since there is no take.
//     fn fetch_max_take(&self, _val: Field) -> Field {
//         self.load_take()
//     }

//     /// Must be aquire so previous give stores are seen before this one is loaded
//     fn load_give(&self) -> Field {
//         Field::from_raw_parts(self.index_max, self.give.load(Ordering::Acquire))
//     }

//     /// Must be release so subsequent give loads are seen after this one is stored
//     fn incr_give(&self) {
//         self.give.fetch_add(1, Ordering::Release);
//     }

//     /// This is for resetting give idx, it does not need to be ordered
//     fn fetch_max_give(&self, val: Field) -> Field {
//         Field::from_raw_parts(
//             self.index_max,
//             self.give.fetch_max(val.get_raw_inner(), Ordering::Relaxed),
//         )
//     }
// }
