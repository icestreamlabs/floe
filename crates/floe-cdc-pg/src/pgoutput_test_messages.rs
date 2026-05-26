use std::collections::HashSet;

use bytes::Bytes;

pub(crate) const TEST_PG_BOOL_OID: u32 = 16;
pub(crate) const TEST_PG_INT4_OID: u32 = 23;
pub(crate) const TEST_PG_INT8_OID: u32 = 20;
pub(crate) const TEST_PG_TEXT_OID: u32 = 25;

#[derive(Debug, Clone, Copy)]
pub(crate) struct PgOutputTestColumn {
    name: &'static str,
    type_oid: u32,
    is_key: bool,
}

impl PgOutputTestColumn {
    pub(crate) const fn new(name: &'static str, type_oid: u32, is_key: bool) -> Self {
        Self {
            name,
            type_oid,
            is_key,
        }
    }
}

pub(crate) fn put_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

pub(crate) fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub(crate) fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub(crate) fn put_i32(out: &mut Vec<u8>, value: i32) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub(crate) fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub(crate) fn put_cstring(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(value.as_bytes());
    out.push(0);
}

pub(crate) fn put_text_value(out: &mut Vec<u8>, value: &str) {
    put_u8(out, b't');
    put_i32(out, value.len() as i32);
    out.extend_from_slice(value.as_bytes());
}

pub(crate) fn put_null_value(out: &mut Vec<u8>) {
    put_u8(out, b'n');
}

pub(crate) fn put_unchanged_toast_value(out: &mut Vec<u8>) {
    put_u8(out, b'u');
}

pub(crate) fn orders_relation_message() -> Bytes {
    relation_message_with_columns(
        42,
        "orders",
        &[
            PgOutputTestColumn::new("id", TEST_PG_INT8_OID, true),
            PgOutputTestColumn::new("amount", TEST_PG_INT4_OID, false),
            PgOutputTestColumn::new("status", TEST_PG_TEXT_OID, false),
            PgOutputTestColumn::new("active", TEST_PG_BOOL_OID, false),
        ],
    )
}

pub(crate) fn orders_relation_message_for(relation_id: u32, table: &str) -> Bytes {
    relation_message_with_columns(
        relation_id,
        table,
        &[
            PgOutputTestColumn::new("id", TEST_PG_INT8_OID, true),
            PgOutputTestColumn::new("amount", TEST_PG_INT4_OID, false),
            PgOutputTestColumn::new("status", TEST_PG_TEXT_OID, false),
            PgOutputTestColumn::new("active", TEST_PG_BOOL_OID, false),
        ],
    )
}

pub(crate) fn id_status_relation_message(relation_id: u32, table: &str) -> Bytes {
    relation_message_with_columns(
        relation_id,
        table,
        &[
            PgOutputTestColumn::new("id", TEST_PG_INT8_OID, true),
            PgOutputTestColumn::new("status", TEST_PG_TEXT_OID, false),
        ],
    )
}

pub(crate) fn relation_message_with_columns(
    relation_id: u32,
    table: &str,
    columns: &[PgOutputTestColumn],
) -> Bytes {
    relation_message_with_identity_and_columns(relation_id, table, b'd', columns)
}

pub(crate) fn relation_message_with_identity_and_columns(
    relation_id: u32,
    table: &str,
    replica_identity: u8,
    columns: &[PgOutputTestColumn],
) -> Bytes {
    let mut out = Vec::new();
    put_u8(&mut out, b'R');
    put_u32(&mut out, relation_id);
    put_cstring(&mut out, "public");
    put_cstring(&mut out, table);
    put_u8(&mut out, replica_identity);
    put_u16(&mut out, columns.len() as u16);

    for column in columns {
        put_u8(&mut out, u8::from(column.is_key));
        put_cstring(&mut out, column.name);
        put_u32(&mut out, column.type_oid);
        put_i32(&mut out, -1);
    }

    Bytes::from(out)
}

pub(crate) fn relation_message_with_column_specs(
    relation_id: u32,
    table: &str,
    columns: &[(&'static str, u32, bool)],
) -> Bytes {
    let columns: Vec<PgOutputTestColumn> = columns
        .iter()
        .map(|(name, type_oid, is_key)| PgOutputTestColumn::new(*name, *type_oid, *is_key))
        .collect();
    relation_message_with_columns(relation_id, table, &columns)
}

pub(crate) fn relation_message_with_identity_and_column_specs(
    relation_id: u32,
    table: &str,
    replica_identity: u8,
    columns: &[(&'static str, u32, bool)],
) -> Bytes {
    let columns: Vec<PgOutputTestColumn> = columns
        .iter()
        .map(|(name, type_oid, is_key)| PgOutputTestColumn::new(*name, *type_oid, *is_key))
        .collect();
    relation_message_with_identity_and_columns(relation_id, table, replica_identity, &columns)
}

pub(crate) fn truncate_message(relation_ids: impl IntoIterator<Item = u32>) -> Bytes {
    let relation_ids: Vec<u32> = relation_ids.into_iter().collect();
    let mut out = Vec::new();
    put_u8(&mut out, b'T');
    put_u32(&mut out, relation_ids.len() as u32);
    put_u8(&mut out, 0);
    for relation_id in relation_ids {
        put_u32(&mut out, relation_id);
    }
    Bytes::from(out)
}

pub(crate) fn tuple(values: impl IntoIterator<Item = Option<&'static str>>) -> Vec<u8> {
    let values: Vec<Option<&'static str>> = values.into_iter().collect();
    let mut out = Vec::new();
    put_u16(&mut out, values.len() as u16);
    for value in values {
        match value {
            Some(value) => put_text_value(&mut out, value),
            None => put_null_value(&mut out),
        }
    }
    out
}

pub(crate) fn tuple_with_unchanged_toast(
    values: impl IntoIterator<Item = Option<&'static str>>,
    unchanged_toast_indices: impl IntoIterator<Item = usize>,
) -> Vec<u8> {
    let values: Vec<Option<&'static str>> = values.into_iter().collect();
    let unchanged_toast_indices: HashSet<usize> = unchanged_toast_indices.into_iter().collect();
    let mut out = Vec::new();
    put_u16(&mut out, values.len() as u16);
    for (idx, value) in values.into_iter().enumerate() {
        if unchanged_toast_indices.contains(&idx) {
            put_unchanged_toast_value(&mut out);
        } else {
            match value {
                Some(value) => put_text_value(&mut out, value),
                None => put_null_value(&mut out),
            }
        }
    }
    out
}

pub(crate) fn insert_message(
    relation_id: u32,
    values: impl IntoIterator<Item = Option<&'static str>>,
) -> Bytes {
    let mut out = Vec::new();
    put_u8(&mut out, b'I');
    put_u32(&mut out, relation_id);
    put_u8(&mut out, b'N');
    out.extend_from_slice(&tuple(values));
    Bytes::from(out)
}

pub(crate) fn insert_text_message<T, I>(relation_id: u32, values: I) -> Bytes
where
    T: AsRef<str>,
    I: IntoIterator<Item = T>,
{
    let mut out = Vec::new();
    put_u8(&mut out, b'I');
    put_u32(&mut out, relation_id);
    put_u8(&mut out, b'N');

    let values: Vec<T> = values.into_iter().collect();
    put_u16(&mut out, values.len() as u16);
    for value in values {
        put_text_value(&mut out, value.as_ref());
    }

    Bytes::from(out)
}

pub(crate) fn insert_id_status_message(relation_id: u32, id: i64, status: &str) -> Bytes {
    insert_text_message(relation_id, [id.to_string(), status.to_string()])
}

pub(crate) fn origin_message(commit_lsn: u64, name: &str) -> Bytes {
    let mut out = Vec::new();
    put_u8(&mut out, b'O');
    put_u64(&mut out, commit_lsn);
    put_cstring(&mut out, name);
    Bytes::from(out)
}
