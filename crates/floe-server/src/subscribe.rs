use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

use chrono::{DateTime, Utc};
use floe_executor::mv_changelog::{MvChangelogBatch, MvChangelogStream};
use futures::Stream;
use pgwire::api::results::{DataRowEncoder, FieldInfo};
use pgwire::error::PgWireResult;
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

pub(super) fn detect_single_subscribe_statement(query: &str) -> Option<&str> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return None;
    }
    let statement = trimmed.trim_end_matches(|c: char| c.is_whitespace() || c == ';');
    if statement.is_empty() || statement.contains(';') {
        return None;
    }
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
}
