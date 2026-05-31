use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

use bytes::{Buf, BytesMut};
use chrono::{DateTime, Utc};
use floe_executor::mv_changelog::{MvChangelogBatch, MvChangelogStream};
use futures::Stream;
use pgwire::api::results::{DataRowEncoder, FieldInfo};
use pgwire::error::PgWireResult;
use pgwire::messages::copy::CopyData;
use pgwire::messages::data::DataRow;
use tokio_util::sync::CancellationToken;

use super::types::encode_arrow_value;
use super::user_error;

pub(super) struct SubscribeResponseStream {
    schema: Arc<Vec<FieldInfo>>,
    stream: MvChangelogStream,
    cancel: CancellationToken,
    current_batch: Option<MvChangelogBatch>,
    next_row: usize,
}

impl SubscribeResponseStream {
    pub(super) fn new(
        schema: Arc<Vec<FieldInfo>>,
        stream: MvChangelogStream,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            schema,
            stream,
            cancel,
            current_batch: None,
            next_row: 0,
        }
    }
}

impl Drop for SubscribeResponseStream {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

pub(super) struct SubscribeCopyOutStream {
    inner: SubscribeResponseStream,
}

impl SubscribeCopyOutStream {
    pub(super) fn new(inner: SubscribeResponseStream) -> Self {
        Self { inner }
    }
}

impl Stream for SubscribeCopyOutStream {
    type Item = PgWireResult<CopyData>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(row))) => Poll::Ready(Some(data_row_to_copy_text(row))),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Stream for SubscribeResponseStream {
    type Item = PgWireResult<DataRow>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(batch) = self.current_batch.as_ref() {
                if self.next_row < batch.batch.num_rows() {
                    let schema = Arc::clone(&self.schema);
                    let row = encode_subscribe_row(schema, batch, self.next_row);
                    self.next_row += 1;
                    return Poll::Ready(Some(row));
                }
                self.current_batch = None;
                self.next_row = 0;
            }

            match Pin::new(&mut self.stream).poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    if batch.batch.num_rows() == 0 {
                        continue;
                    }
                    self.current_batch = Some(batch);
                    self.next_row = 0;
                }
                Poll::Ready(Some(Err(err))) => {
                    return Poll::Ready(Some(Err(user_error(format!(
                        "SUBSCRIBE execution error: {err}"
                    )))));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn encode_subscribe_row(
    schema: Arc<Vec<FieldInfo>>,
    batch: &MvChangelogBatch,
    row_idx: usize,
) -> PgWireResult<DataRow> {
    let fields = Arc::clone(&schema);
    let mut encoder = DataRowEncoder::new(schema);
    let diff = batch
        .diffs
        .get(row_idx)
        .copied()
        .ok_or_else(|| user_error("SUBSCRIBE batch missing floe_diff".to_string()))?;
    let time = match batch.version_time {
        Some(micros) => Some(
            DateTime::<Utc>::from_timestamp_micros(micros)
                .ok_or_else(|| user_error(format!("timestamp micros {micros} out of range")))?,
        ),
        None => None,
    };
    encoder.encode_field(&Some(batch.version))?;
    encoder.encode_field(&Some(diff))?;
    encoder.encode_field(&time)?;
    let batch_schema = batch.batch.schema();
    let field_offset = 3usize;
    for col_idx in 0..batch.batch.num_columns() {
        let array = batch.batch.column(col_idx);
        let field = batch_schema.field(col_idx);
        let pg_field = fields.get(col_idx + field_offset).ok_or_else(|| {
            user_error(format!(
                "SUBSCRIBE schema missing field metadata for column index {}",
                col_idx + field_offset
            ))
        })?;
        encode_arrow_value(array, field, pg_field, row_idx, &mut encoder)?;
    }
    Ok(encoder.take_row())
}

fn data_row_to_copy_text(row: DataRow) -> PgWireResult<CopyData> {
    let mut fields = row.data;
    let mut output = BytesMut::new();
    for field_idx in 0..row.field_count {
        if field_idx > 0 {
            output.extend_from_slice(b"\t");
        }
        if fields.remaining() < 4 {
            return Err(user_error("COPY row field length truncated".to_string()));
        }
        let len = fields.get_i32();
        if len < 0 {
            output.extend_from_slice(b"\\N");
            continue;
        }
        let len = usize::try_from(len)
            .map_err(|_| user_error("COPY row field length out of range".to_string()))?;
        if fields.remaining() < len {
            return Err(user_error("COPY row field value truncated".to_string()));
        }
        let value = fields.copy_to_bytes(len);
        append_copy_text_field(&mut output, value.as_ref());
    }
    output.extend_from_slice(b"\n");
    Ok(CopyData::new(output.freeze()))
}

fn append_copy_text_field(output: &mut BytesMut, value: &[u8]) {
    for byte in value {
        match byte {
            b'\\' => output.extend_from_slice(b"\\\\"),
            b'\n' => output.extend_from_slice(b"\\n"),
            b'\r' => output.extend_from_slice(b"\\r"),
            b'\t' => output.extend_from_slice(b"\\t"),
            _ => output.extend_from_slice(&[*byte]),
        }
    }
}

pub(super) fn detect_single_subscribe_statement(query: &str) -> Option<&str> {
    let statement = single_statement(query)?;
    if is_subscribe_statement(statement) {
        Some(statement)
    } else {
        None
    }
}

fn is_subscribe_statement(sql: &str) -> bool {
    let trimmed = sql.trim_start_matches(|c: char| c.is_ascii_control() || c.is_whitespace());
    if trimmed.len() < 9 {
        return false;
    }
    if !trimmed[..9].eq_ignore_ascii_case("SUBSCRIBE") {
        return false;
    }
    trimmed[9..]
        .chars()
        .next()
        .is_some_and(|ch| ch.is_whitespace())
}

pub(super) fn detect_copy_subscribe_to_stdout_statement(query: &str) -> Option<&str> {
    let statement = single_statement(query)?;
    let trimmed = statement.trim_start_matches(|c: char| c.is_ascii_control() || c.is_whitespace());
    if trimmed.len() < 4 || !trimmed[..4].eq_ignore_ascii_case("COPY") {
        return None;
    }
    let after_copy = trimmed[4..].trim_start();
    if !after_copy.starts_with('(') {
        return None;
    }
    let close_idx = find_matching_paren(after_copy)?;
    let inner = after_copy[1..close_idx].trim();
    if !is_subscribe_statement(inner) {
        return None;
    }
    let suffix = after_copy[close_idx + 1..].trim_start();
    let suffix = consume_keyword(suffix, "TO")?.trim_start();
    let suffix = consume_keyword(suffix, "STDOUT")?.trim_start();
    if suffix.is_empty() || is_supported_copy_options(suffix) {
        Some(inner)
    } else {
        None
    }
}

fn single_statement(query: &str) -> Option<&str> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return None;
    }
    let statement = trimmed.trim_end_matches(|c: char| c.is_whitespace() || c == ';');
    if statement.is_empty() || statement.contains(';') {
        return None;
    }
    Some(statement)
}

fn find_matching_paren(input: &str) -> Option<usize> {
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut previous = '\0';
    for (idx, ch) in input.char_indices().skip(1) {
        if in_single_quote {
            if ch == '\'' && previous != '\'' {
                in_single_quote = false;
            }
            previous = ch;
            continue;
        }
        if in_double_quote {
            if ch == '"' {
                in_double_quote = false;
            }
            previous = ch;
            continue;
        }
        match ch {
            '\'' => in_single_quote = true,
            '"' => in_double_quote = true,
            ')' => return Some(idx),
            _ => {}
        }
        previous = ch;
    }
    None
}

fn consume_keyword<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    if input.len() < keyword.len() || !input[..keyword.len()].eq_ignore_ascii_case(keyword) {
        return None;
    }
    let rest = &input[keyword.len()..];
    if rest
        .chars()
        .next()
        .is_some_and(|ch| !(ch.is_ascii_control() || ch.is_whitespace()))
    {
        return None;
    }
    Some(rest)
}

fn is_supported_copy_options(suffix: &str) -> bool {
    let Some(rest) = consume_keyword(suffix, "WITH") else {
        return false;
    };
    let rest = rest.trim();
    let rest = rest
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'));
    let Some(options) = rest else {
        return false;
    };
    let normalized = options
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    matches!(
        normalized.as_str(),
        "format text" | "format = text" | "format 'text'" | "format = 'text'"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_subscribe_statement_in_simple_query() {
        let query = "  SUBSCRIBE mv_orders WITH SNAPSHOT;;\n";
        assert_eq!(
            detect_single_subscribe_statement(query),
            Some("SUBSCRIBE mv_orders WITH SNAPSHOT")
        );
        assert!(detect_single_subscribe_statement("SELECT 1;").is_none());
        assert!(detect_single_subscribe_statement("SUBSCRIBE mv_orders; SELECT 1").is_none());
    }

    #[test]
    fn detects_copy_subscribe_to_stdout_statement() {
        assert_eq!(
            detect_copy_subscribe_to_stdout_statement(
                " COPY (SUBSCRIBE mv_orders WITH SNAPSHOT) TO STDOUT;\n"
            ),
            Some("SUBSCRIBE mv_orders WITH SNAPSHOT")
        );
        assert_eq!(
            detect_copy_subscribe_to_stdout_statement(
                "COPY ( SUBSCRIBE mv_orders AS OF 42 ) TO STDOUT WITH (FORMAT text)"
            ),
            Some("SUBSCRIBE mv_orders AS OF 42")
        );
        assert!(
            detect_copy_subscribe_to_stdout_statement(
                "COPY (SUBSCRIBE mv_orders) TO STDOUT WITH (FORMAT csv)"
            )
            .is_none()
        );
        assert!(detect_copy_subscribe_to_stdout_statement("COPY mv TO STDOUT").is_none());
        assert!(detect_copy_subscribe_to_stdout_statement("SUBSCRIBE mv_orders").is_none());
    }

    #[test]
    fn copy_text_escapes_fields() {
        let mut data = BytesMut::new();
        data.extend_from_slice(&3_i32.to_be_bytes());
        data.extend_from_slice(b"a\tb");
        data.extend_from_slice(&(-1_i32).to_be_bytes());
        data.extend_from_slice(&4_i32.to_be_bytes());
        data.extend_from_slice(b"x\\\ny");
        let row = DataRow::new(data, 3);

        let copy = data_row_to_copy_text(row).expect("copy row");
        assert_eq!(copy.data.as_ref(), b"a\\tb\t\\N\tx\\\\\\ny\n");
    }
}
