use crate::field::Field;
use std::{
    fmt::Debug,
    sync::atomic::{AtomicUsize, Ordering},
};

pub struct AtomicField {
    index_max: usize,
    inner: AtomicUsize,
}

impl AtomicField {
    pub fn new<U: Into<Field>>(value: U) -> Self {
        let field: Field = value.into();

        Self {
            index_max: field.get_index_max(),
            inner: AtomicUsize::new(field.get_raw_inner()),
        }
    }

    pub fn load(&self, order: Ordering) -> Field {
        Field::from_raw_parts(self.index_max, self.inner.load(order))
    }

    pub fn fetch_add(&self, val: usize, order: Ordering) -> Field {
        Field::from_raw_parts(self.index_max, self.inner.fetch_add(val, order))
    }

    pub fn fetch_max(&self, val: Field, order: Ordering) -> Field {
        Field::from_raw_parts(
            self.index_max,
            self.inner.fetch_max(val.get_raw_inner(), order),
        )
    }
}

impl Debug for AtomicField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Atomic")
            .field("inner", &self.load(Ordering::Relaxed))
            .finish()
    }
}
