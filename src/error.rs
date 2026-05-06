#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    Full,
    Busy,
    Empty,
}
