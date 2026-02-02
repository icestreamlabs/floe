use std::time::Duration;

use anyhow::{Context, Result};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::scalar::ScalarValue;
use floe_executor::FloeQueryContext;
use floe_executor::MaterializedViewRegistry;
use floe_executor::tail::{TailBatch, TailParams, execute_tail, is_tail_canceled_error};
use futures::StreamExt;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use reqwest::Client;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::{SinkConfig, SinkSpec};

pub fn spawn_sinks(
    sinks: Vec<SinkSpec>,
    query: FloeQueryContext,
    registry: std::sync::Arc<MaterializedViewRegistry>,
    cancel: CancellationToken,
) -> Vec<JoinHandle<()>> {
    let mut handles = Vec::new();
    for sink in sinks {
        let ctx = query.clone();
        let registry = registry.clone();
        let cancel = cancel.clone();
        handles.push(tokio::spawn(async move {
            let name = sink.name.clone();
            if let Err(err) = run_sink(sink, ctx, registry, cancel).await {
                if is_tail_canceled_error(&err) {
                    tracing::info!(sink = %name, "sink canceled");
                } else {
                    tracing::error!(sink = %name, error = %err, "sink failed");
                }
            }
        }));
    }
    handles
}

async fn run_sink(
    sink: SinkSpec,
    query: FloeQueryContext,
    registry: std::sync::Arc<MaterializedViewRegistry>,
    cancel: CancellationToken,
) -> Result<()> {
    match sink.config {
        SinkConfig::Kafka {
            brokers,
            topic,
            mv,
            with_snapshot,
            as_of,
            ..
        } => {
            run_kafka_sink(
                &query,
                registry,
                cancel,
                &brokers,
                &topic,
                &mv,
                with_snapshot.unwrap_or(false),
                as_of,
            )
            .await
        }
        SinkConfig::File {
            path,
            mv,
            with_snapshot,
            as_of,
            append,
            ..
        } => {
            run_file_sink(
                &query,
                registry,
                cancel,
                &path,
                &mv,
                with_snapshot.unwrap_or(false),
                as_of,
                append.unwrap_or(true),
            )
            .await
        }
        SinkConfig::Http {
            url,
            mv,
            with_snapshot,
            as_of,
            batch_size,
            ..
        } => {
            run_http_sink(
                &query,
                registry,
                cancel,
                &url,
                &mv,
                with_snapshot.unwrap_or(false),
                as_of,
                batch_size.unwrap_or(1),
            )
            .await
        }
    }
}

async fn run_kafka_sink(
    query: &FloeQueryContext,
    registry: std::sync::Arc<MaterializedViewRegistry>,
    cancel: CancellationToken,
    brokers: &str,
    topic: &str,
    mv: &str,
    with_snapshot: bool,
    as_of: Option<i64>,
) -> Result<()> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .context("create kafka producer")?;
    let mut stream = execute_tail(
        &query.session(),
        registry.as_ref(),
        TailParams {
            mv_name: mv.to_string(),
            with_snapshot,
            as_of,
        },
        cancel,
    )
    .await?;

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let schema = batch.batch.schema();
        for row_idx in 0..batch.batch.num_rows() {
            let json = tail_row_to_json(&batch, row_idx, &schema)?;
            let payload = serde_json::to_string(&json).context("serialize sink row")?;
            let record = FutureRecord::<(), _>::to(topic).payload(&payload);
            let _ = producer.send(record, Duration::from_secs(0)).await;
        }
    }
    Ok(())
}

async fn run_file_sink(
    query: &FloeQueryContext,
    registry: std::sync::Arc<MaterializedViewRegistry>,
    cancel: CancellationToken,
    path: &str,
    mv: &str,
    with_snapshot: bool,
    as_of: Option<i64>,
    append: bool,
) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(append)
        .truncate(!append)
        .open(path)
        .await
        .with_context(|| format!("open sink file {path}"))?;

    let mut stream = execute_tail(
        &query.session(),
        registry.as_ref(),
        TailParams {
            mv_name: mv.to_string(),
            with_snapshot,
            as_of,
        },
        cancel,
    )
    .await?;

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let schema = batch.batch.schema();
        for row_idx in 0..batch.batch.num_rows() {
            let json = tail_row_to_json(&batch, row_idx, &schema)?;
            let payload = serde_json::to_string(&json).context("serialize sink row")?;
            file.write_all(payload.as_bytes()).await?;
            file.write_all(b"\n").await?;
        }
    }
    file.flush().await?;
    Ok(())
}

async fn run_http_sink(
    query: &FloeQueryContext,
    registry: std::sync::Arc<MaterializedViewRegistry>,
    cancel: CancellationToken,
    url: &str,
    mv: &str,
    with_snapshot: bool,
    as_of: Option<i64>,
    batch_size: usize,
) -> Result<()> {
    let client = Client::new();
    let mut buffer: Vec<serde_json::Value> = Vec::new();
    let mut stream = execute_tail(
        &query.session(),
        registry.as_ref(),
        TailParams {
            mv_name: mv.to_string(),
            with_snapshot,
            as_of,
        },
        cancel,
    )
    .await?;

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let schema = batch.batch.schema();
        for row_idx in 0..batch.batch.num_rows() {
            let json = tail_row_to_json(&batch, row_idx, &schema)?;
            buffer.push(json);
            if buffer.len() >= batch_size {
                post_http_batch(&client, url, &buffer).await?;
                buffer.clear();
            }
        }
    }

    if !buffer.is_empty() {
        post_http_batch(&client, url, &buffer).await?;
    }
    Ok(())
}

async fn post_http_batch(client: &Client, url: &str, batch: &[serde_json::Value]) -> Result<()> {
    let payload = if batch.len() == 1 {
        batch[0].clone()
    } else {
        serde_json::Value::Array(batch.to_vec())
    };
    client
        .post(url)
        .json(&payload)
        .send()
        .await
        .context("post sink batch")?
        .error_for_status()
        .context("sink http error")?;
    Ok(())
}

fn tail_row_to_json(
    batch: &TailBatch,
    row_idx: usize,
    schema: &SchemaRef,
) -> Result<serde_json::Value> {
    let mut object = serde_json::Map::new();
    object.insert(
        "__mv_version".to_string(),
        serde_json::Value::from(batch.version),
    );
    object.insert(
        "__op".to_string(),
        serde_json::Value::from(batch.ops.get(row_idx).copied().unwrap_or(0)),
    );
    let time = batch.times.get(row_idx).copied().flatten();
    if let Some(time) = time {
        object.insert("__time".to_string(), serde_json::Value::from(time));
    } else {
        object.insert("__time".to_string(), serde_json::Value::Null);
    }

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let array = batch.batch.column(col_idx);
        let value = array_value_to_json(array, row_idx)?;
        object.insert(field.name().clone(), value);
    }

    Ok(serde_json::Value::Object(object))
}

fn array_value_to_json(array: &ArrayRef, row_idx: usize) -> Result<serde_json::Value> {
    let scalar = ScalarValue::try_from_array(array, row_idx)?;
    Ok(scalar_to_json(&scalar))
}

fn scalar_to_json(value: &ScalarValue) -> serde_json::Value {
    match value {
        ScalarValue::Boolean(Some(v)) => serde_json::Value::from(*v),
        ScalarValue::Int8(Some(v)) => serde_json::Value::from(*v),
        ScalarValue::Int16(Some(v)) => serde_json::Value::from(*v),
        ScalarValue::Int32(Some(v)) => serde_json::Value::from(*v),
        ScalarValue::Int64(Some(v)) => serde_json::Value::from(*v),
        ScalarValue::UInt8(Some(v)) => serde_json::Value::from(*v),
        ScalarValue::UInt16(Some(v)) => serde_json::Value::from(*v),
        ScalarValue::UInt32(Some(v)) => serde_json::Value::from(*v),
        ScalarValue::UInt64(Some(v)) => serde_json::Value::from(*v),
        ScalarValue::Float32(Some(v)) => serde_json::Value::from(*v as f64),
        ScalarValue::Float64(Some(v)) => serde_json::Value::from(*v),
        ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => {
            serde_json::Value::from(v.clone())
        }
        ScalarValue::TimestampMicrosecond(Some(v), _)
        | ScalarValue::TimestampMillisecond(Some(v), _)
        | ScalarValue::TimestampNanosecond(Some(v), _)
        | ScalarValue::TimestampSecond(Some(v), _) => serde_json::Value::from(*v),
        ScalarValue::Null => serde_json::Value::Null,
        other => serde_json::Value::String(other.to_string()),
    }
}
