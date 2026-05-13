pub mod catalog;
pub mod encoding;
pub mod source;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum RowValue {
    Int64(i64),
    Bool(bool),
    Utf8(String),
    TimestampMillis(i64),
    DateDays(i32),
    Numeric(String),
}

pub type RowValues = Vec<RowValue>;
