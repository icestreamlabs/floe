pub mod catalog;
pub mod decimal;
pub mod encoding;
pub mod postgres_types;
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
    Decimal128(i128),
    Numeric(String),
}

pub type RowValues = Vec<RowValue>;
