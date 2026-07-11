use datafusion::arrow::record_batch::RecordBatch;
pub use floe_core::source::{
    AppendIngestEvent, AppendIngestResumeToken, SourceDefinition, SourceRegistry,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::{Mutex, mpsc, oneshot};

pub type AppendIngestEventBatch = Vec<AppendIngestEvent>;
pub type RoutedAppendIngestEventReceiver = mpsc::Receiver<RoutedAppendIngestEventBatch>;

#[derive(Debug)]
pub struct KafkaRawIngestRecord {
    pub payload: Vec<u8>,
    pub topic: Arc<str>,
    pub partition: i32,
    pub offset: i64,
    pub event_time_ms: Option<u64>,
}

#[derive(Debug)]
pub struct KafkaRawIngestBatch {
    pub source: String,
    pub records: Vec<KafkaRawIngestRecord>,
}

impl KafkaRawIngestBatch {
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct KafkaArrowIngestRecord {
    pub topic: Arc<str>,
    pub partition: i32,
    pub offset: i64,
    pub event_time_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct KafkaArrowIngestJournalRange {
    pub topic: Arc<str>,
    pub partition: i32,
    pub start_offset: i64,
    pub end_offset: i64,
    pub row_count: u64,
    pub checksum: u64,
}

#[derive(Debug)]
pub struct KafkaArrowIngestBatch {
    pub source: String,
    pub execution: RecordBatch,
    pub query: Option<RecordBatch>,
    pub records: Vec<KafkaArrowIngestRecord>,
    pub kafka_metadata_ranges: Vec<KafkaArrowIngestJournalRange>,
}

impl KafkaArrowIngestBatch {
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn split_off(&mut self, at: usize) -> Self {
        assert!(
            self.kafka_metadata_ranges.is_empty(),
            "Kafka Arrow batches with precomputed metadata ranges cannot be split"
        );
        let len = self.len();
        assert!(at <= len, "Kafka Arrow split index out of bounds");
        let records = self.records.split_off(at);
        let execution = self.execution.slice(at, len - at);
        self.execution = self.execution.slice(0, at);
        let query = self.query.as_mut().map(|query| {
            let remaining = query.slice(at, len - at);
            *query = query.slice(0, at);
            remaining
        });
        Self {
            source: self.source.clone(),
            execution,
            query,
            records,
            kafka_metadata_ranges: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct RoutedAppendIngestEventBatch {
    pub connector_id: usize,
    pub payload: RoutedIngestPayload,
    pub commit_ack: Option<CommitAck>,
}

#[derive(Debug)]
pub enum RoutedIngestPayload {
    Events(AppendIngestEventBatch),
    KafkaRaw(KafkaRawIngestBatch),
    KafkaArrow(KafkaArrowIngestBatch),
}

pub type CommitAckReceiver = oneshot::Receiver<Result<(), String>>;

#[derive(Debug, Clone)]
pub struct CommitAck {
    inner: Arc<CommitAckInner>,
}

#[derive(Debug)]
struct CommitAckInner {
    remaining: AtomicUsize,
    sender: Mutex<Option<oneshot::Sender<Result<(), String>>>>,
}

impl CommitAck {
    fn new(row_count: usize, sender: oneshot::Sender<Result<(), String>>) -> Self {
        Self {
            inner: Arc::new(CommitAckInner {
                remaining: AtomicUsize::new(row_count),
                sender: Mutex::new(Some(sender)),
            }),
        }
    }

    pub async fn record_committed(&self) {
        if self.inner.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.send(Ok(())).await;
        }
    }

    pub async fn record_failed(&self, message: impl Into<String>) {
        self.send(Err(message.into())).await;
    }

    async fn send(&self, result: Result<(), String>) {
        if let Some(sender) = self.inner.sender.lock().await.take() {
            let _ = sender.send(result);
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct PendingAppendIngestEventCounter {
    pending: Arc<AtomicUsize>,
}

impl PendingAppendIngestEventCounter {
    pub fn pending(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }

    pub fn record_enqueue(&self, count: usize) {
        if count == 0 {
            return;
        }
        self.pending.fetch_add(count, Ordering::AcqRel);
    }

    pub fn record_dequeue(&self, count: usize) {
        if count == 0 {
            return;
        }
        self.pending
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(count))
            })
            .ok();
    }
}

#[derive(Clone)]
pub struct AppendIngestEventSender {
    connector_id: usize,
    sender: mpsc::Sender<RoutedAppendIngestEventBatch>,
    pending: PendingAppendIngestEventCounter,
}

pub fn routed_channel(
    capacity: usize,
) -> (
    mpsc::Sender<RoutedAppendIngestEventBatch>,
    RoutedAppendIngestEventReceiver,
) {
    mpsc::channel(capacity)
}

pub fn channel(capacity: usize) -> (AppendIngestEventSender, RoutedAppendIngestEventReceiver) {
    let (sender, receiver) = routed_channel(capacity);
    (
        routed_sender(0, sender, PendingAppendIngestEventCounter::default()),
        receiver,
    )
}

pub fn routed_sender(
    connector_id: usize,
    sender: mpsc::Sender<RoutedAppendIngestEventBatch>,
    pending: PendingAppendIngestEventCounter,
) -> AppendIngestEventSender {
    AppendIngestEventSender {
        connector_id,
        sender,
        pending,
    }
}

impl AppendIngestEventSender {
    pub fn pending_events(&self) -> usize {
        self.pending.pending()
    }
}

pub async fn send_event(
    sender: &AppendIngestEventSender,
    event: AppendIngestEvent,
) -> Result<(), SendError<AppendIngestEventBatch>> {
    send_batch(sender, vec![event]).await
}

pub async fn send_batch(
    sender: &AppendIngestEventSender,
    events: AppendIngestEventBatch,
) -> Result<(), SendError<AppendIngestEventBatch>> {
    if events.is_empty() {
        return Ok(());
    }
    let count = events.len();
    sender.pending.record_enqueue(count);
    if let Err(err) = sender
        .sender
        .send(RoutedAppendIngestEventBatch {
            connector_id: sender.connector_id,
            payload: RoutedIngestPayload::Events(events),
            commit_ack: None,
        })
        .await
    {
        sender.pending.record_dequeue(count);
        let RoutedIngestPayload::Events(events) = err.0.payload else {
            unreachable!("event send returned a different ingest payload")
        };
        return Err(SendError(events));
    }
    Ok(())
}
pub async fn send_batch_with_commit_ack(
    sender: &AppendIngestEventSender,
    events: AppendIngestEventBatch,
) -> Result<CommitAckReceiver, SendError<AppendIngestEventBatch>> {
    if events.is_empty() {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Ok(()));
        return Ok(rx);
    }
    let count = events.len();
    let (ack_tx, ack_rx) = oneshot::channel();
    sender.pending.record_enqueue(count);
    if let Err(err) = sender
        .sender
        .send(RoutedAppendIngestEventBatch {
            connector_id: sender.connector_id,
            payload: RoutedIngestPayload::Events(events),
            commit_ack: Some(CommitAck::new(count, ack_tx)),
        })
        .await
    {
        sender.pending.record_dequeue(count);
        let RoutedIngestPayload::Events(events) = err.0.payload else {
            unreachable!("acknowledged event send returned a different ingest payload")
        };
        return Err(SendError(events));
    }
    Ok(ack_rx)
}
pub async fn send_kafka_raw_batch(
    sender: &AppendIngestEventSender,
    batch: KafkaRawIngestBatch,
) -> Result<(), SendError<KafkaRawIngestBatch>> {
    if batch.is_empty() {
        return Ok(());
    }
    let count = batch.len();
    sender.pending.record_enqueue(count);
    if let Err(err) = sender
        .sender
        .send(RoutedAppendIngestEventBatch {
            connector_id: sender.connector_id,
            payload: RoutedIngestPayload::KafkaRaw(batch),
            commit_ack: None,
        })
        .await
    {
        sender.pending.record_dequeue(count);
        let RoutedIngestPayload::KafkaRaw(batch) = err.0.payload else {
            unreachable!("raw Kafka send returned a different ingest payload")
        };
        return Err(SendError(batch));
    }
    Ok(())
}
pub async fn send_kafka_arrow_batch(
    sender: &AppendIngestEventSender,
    batch: KafkaArrowIngestBatch,
) -> Result<(), SendError<KafkaArrowIngestBatch>> {
    if batch.is_empty() {
        return Ok(());
    }
    let count = batch.len();
    sender.pending.record_enqueue(count);
    if let Err(err) = sender
        .sender
        .send(RoutedAppendIngestEventBatch {
            connector_id: sender.connector_id,
            payload: RoutedIngestPayload::KafkaArrow(batch),
            commit_ack: None,
        })
        .await
    {
        sender.pending.record_dequeue(count);
        let RoutedIngestPayload::KafkaArrow(batch) = err.0.payload else {
            unreachable!("Arrow Kafka send returned a different ingest payload")
        };
        return Err(SendError(batch));
    }
    Ok(())
}
