use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

use chrono::{DateTime, Utc};
use futures::Stream;
use pgwire::api::results::DataRowEncoder;
use pgwire::api::results::FieldInfo;
use pgwire::error::PgWireResult;
use pgwire::messages::data::DataRow;
use tokio_util::sync::CancellationToken;

use floe_executor::tail::{TailBatch, TailStream};

use super::types::encode_arrow_value;
use super::user_error;

pub(super) struct TailResponseStream {
    schema: Arc<Vec<FieldInfo>>,
    stream: TailStream,
    cancel: CancellationToken,
    current_batch: Option<TailBatch>,
    next_row: usize,
}

impl TailResponseStream {
    pub(super) fn new(
        schema: Arc<Vec<FieldInfo>>,
        stream: TailStream,
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

impl Drop for TailResponseStream {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl Stream for TailResponseStream {
    type Item = PgWireResult<DataRow>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(batch) = self.current_batch.as_ref() {
                if self.next_row < batch.batch.num_rows() {
                    let schema = Arc::clone(&self.schema);
                    let row = encode_tail_row(schema, batch, self.next_row);
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
                        "TAIL execution error: {err}"
                    )))));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn encode_tail_row(
    schema: Arc<Vec<FieldInfo>>,
    batch: &TailBatch,
    row_idx: usize,
) -> PgWireResult<DataRow> {
    let mut encoder = DataRowEncoder::new(schema);
    let op = batch
        .ops
        .get(row_idx)
        .copied()
        .ok_or_else(|| user_error("TAIL batch missing __op".to_string()))?;
    let time = match batch.times.get(row_idx).cloned().unwrap_or(None) {
        Some(micros) => Some(
            DateTime::<Utc>::from_timestamp_micros(micros)
                .ok_or_else(|| user_error(format!("timestamp micros {micros} out of range")))?,
        ),
        None => None,
    };
    encoder.encode_field(&Some(batch.version))?;
    encoder.encode_field(&Some(i64::from(op)))?;
    encoder.encode_field(&time)?;
    for col_idx in 0..batch.batch.num_columns() {
        let array = batch.batch.column(col_idx);
        let data_type = batch.batch.schema().field(col_idx).data_type().clone();
        encode_arrow_value(array.as_ref(), row_idx, &data_type, &mut encoder)?;
    }
    encoder.finish()
}

pub(super) fn detect_single_tail_statement(query: &str) -> Option<&str> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return None;
    }
    let statement = trimmed.trim_end_matches(|c: char| c.is_whitespace() || c == ';');
    if statement.is_empty() {
        return None;
    }
    if statement.contains(';') {
        return None;
    }
    if is_tail_statement(statement) {
        Some(statement)
    } else {
        None
    }
}

fn is_tail_statement(sql: &str) -> bool {
    let trimmed = sql.trim_start_matches(|c: char| c.is_ascii_control() || c.is_whitespace());
    if trimmed.len() < 4 {
        return false;
    }
    if !trimmed[..4].eq_ignore_ascii_case("TAIL") {
        return false;
    }
    trimmed[4..]
        .chars()
        .next()
        .is_some_and(|ch| ch.is_whitespace())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_tail_statement_in_simple_query() {
        let query = "  TAIL mv_orders WITH SNAPSHOT;;\n";
        assert_eq!(
            detect_single_tail_statement(query),
            Some("TAIL mv_orders WITH SNAPSHOT")
        );
        assert!(detect_single_tail_statement("SELECT 1;").is_none());
        assert!(detect_single_tail_statement("TAIL mv_orders; SELECT 1").is_none());
    }
}
