use std::{cmp::Ordering, fmt::Debug};

pub struct FieldConfig {
    pub index_max: usize,
    pub version: usize,
    pub index: usize,
}

#[derive(Clone, Copy)]
pub struct Field {
    index_max: usize,
    inner: usize,
}

impl From<FieldConfig> for Field {
    fn from(
        FieldConfig {
            index_max,
            version,
            index,
        }: FieldConfig,
    ) -> Self {
        Self::from_parts(index_max, version, index)
    }
}

/// Spare index bits above those needed for `index_max`. A producer checks that
/// a block has room and then increments `allocated`, so `allocated` can pass
/// `index_max` by one for each producer in that window. These bits keep such an
/// overshoot out of the version, for up to `2^OVERSHOOT_BITS` producers.
const OVERSHOOT_BITS: u32 = usize::BITS / 4;

impl Field {
    const fn version_shift(index_max: usize) -> u32 {
        usize::BITS - index_max.leading_zeros() + OVERSHOOT_BITS
    }

    const fn index_mask(index_max: usize) -> usize {
        !(usize::MAX << Self::version_shift(index_max))
    }

    pub fn from_parts(index_max: usize, version: usize, index: usize) -> Self {
        Self {
            index_max,
            inner: (version << Self::version_shift(index_max))
                | (index & Self::index_mask(index_max)),
        }
    }

    pub fn from_raw_parts(index_max: usize, inner: usize) -> Self {
        Self { index_max, inner }
    }

    pub const fn get_version(&self) -> usize {
        self.inner >> Self::version_shift(self.index_max)
    }

    pub const fn get_index(&self) -> usize {
        self.inner & Self::index_mask(self.index_max)
    }

    pub const fn get_index_max(&self) -> usize {
        self.index_max
    }

    pub fn get_raw_inner(&self) -> usize {
        self.inner
    }

    pub const fn overflowing_add(self, rhs: usize) -> Self {
        let Self { index_max, inner } = self;

        Self {
            index_max,
            inner: inner + rhs,
        }
    }

    pub fn version_inc_add(self, rhs: usize) -> Self {
        FieldConfig {
            index_max: self.index_max,
            version: self.get_version() + (self.get_index() + rhs) / self.index_max,
            index: (self.get_index() + rhs) % self.index_max,
        }
        .into()
    }

    pub const fn is_full(&self) -> bool {
        self.get_index() >= self.get_index_max()
    }
}

impl Debug for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Field")
            .field("index_max", &self.index_max)
            .field("version", &self.get_version())
            .field("index", &self.get_index())
            .finish()
    }
}

impl PartialOrd for Field {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.inner.partial_cmp(&other.inner)
    }
}

impl PartialEq for Field {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{
        Rng, RngExt, distr::{Distribution, StandardUniform}
        // rng,
    };

    impl Distribution<FieldConfig> for StandardUniform {
        fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> FieldConfig {
            let index_max = rng.random::<u64>() as usize & (usize::MAX >> 1);
            let version = rng.random::<u64>() as usize >> index_max.leading_zeros();
            let index = rng.random::<u64>() as usize & (usize::MAX >> index_max.leading_zeros());

            FieldConfig {
                index_max,
                version,
                index,
            }
        }
    }

    #[test]
    fn test_field_from_parts_and_getters() {
        // let mut rng = rng();

        // for _ in 0..1000 {
        //     let field_config =
        // }

        let index_max = 16;
        let version = 3;
        let index = 5;
        let field = Field::from_parts(index_max, version, index);

        assert_eq!(field.get_index_max(), index_max);
        assert_eq!(field.get_version(), version);
        assert_eq!(field.get_index(), index);
    }

    #[test]
    fn test_field_from_raw_parts() {
        let index_max = 8;
        let inner = 0b1010_0011;
        let field = Field::from_raw_parts(index_max, inner);

        assert_eq!(field.get_index_max(), index_max);
        assert_eq!(field.get_raw_inner(), inner);
    }

    #[test]
    fn test_field_overflowing_add() {
        let field = Field::from_parts(8, 2, 3);
        let added = field.overflowing_add(5);

        assert_eq!(added.get_raw_inner(), field.get_raw_inner() + 5);
        assert_eq!(added.get_index_max(), field.get_index_max());
    }

    #[test]
    fn test_field_index_overshoot_does_not_reach_version() {
        let field = Field::from_parts(7, 3, 7);
        let overshot = field.overflowing_add(1000);

        assert_eq!(overshot.get_version(), 3);
        assert_eq!(overshot.get_index(), 1007);
    }

    #[test]
    fn test_field_version_inc_add_no_wrap() {
        let field = Field::from_parts(10, 1, 2);
        let result = field.version_inc_add(5);

        assert_eq!(result.get_version(), 1);
        assert_eq!(result.get_index(), 7);
    }

    #[test]
    fn test_field_version_inc_add_with_wrap() {
        let field = Field::from_parts(10, 1, 8);
        let result = field.version_inc_add(5);

        // (8 + 5) = 13, so version increases by 1, index is 3
        assert_eq!(result.get_version(), 2);
        assert_eq!(result.get_index(), 3);
    }

    #[test]
    fn test_field_partial_eq_and_ord() {
        let f1 = Field::from_parts(16, 1, 2);
        let f2 = Field::from_parts(16, 1, 2);
        let f3 = Field::from_parts(16, 2, 0);

        assert_eq!(f1, f2);
        assert!(f1 < f3);
        assert!(f3 > f2);
    }

    #[test]
    fn test_field_debug_format() {
        let field = Field::from_parts(8, 2, 4);
        let debug_str = format!("{:?}", field);
        assert!(debug_str.contains("Field"));
        assert!(debug_str.contains("index_max"));
        assert!(debug_str.contains("version"));
        assert!(debug_str.contains("index"));
    }

    #[test]
    fn test_field_from_field_config() {
        let config = FieldConfig {
            index_max: 32,
            version: 4,
            index: 7,
        };
        let field: Field = config.into();
        assert_eq!(field.get_index_max(), 32);
        assert_eq!(field.get_version(), 4);
        assert_eq!(field.get_index(), 7);
    }
}
