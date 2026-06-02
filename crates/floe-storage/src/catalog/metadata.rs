use super::*;

impl MaterializedViewMetadata {
    pub fn new(name: impl Into<String>, query: impl Into<String>, if_not_exists: bool) -> Self {
        Self {
            name: name.into(),
            query: query.into(),
            if_not_exists,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn query(&self) -> &str {
        &self.query
    }

    pub fn if_not_exists(&self) -> bool {
        self.if_not_exists
    }
}

impl ReplicationPipelineCheckpoint {
    pub fn new(
        pipeline_name: impl Into<String>,
        source_name: impl Into<String>,
        source_position: CdcSourcePosition,
        transaction_id: Option<CdcTransactionId>,
        target_state: BTreeMap<String, String>,
        committed_at_unix_ms: u64,
    ) -> Result<Self> {
        let pipeline_name = pipeline_name.into();
        let source_name = source_name.into();
        ensure!(
            !pipeline_name.trim().is_empty(),
            "replication pipeline checkpoint name cannot be empty"
        );
        ensure!(
            !source_name.trim().is_empty(),
            "replication pipeline checkpoint source name cannot be empty"
        );
        Ok(Self {
            pipeline_name,
            source_name,
            source_position,
            transaction_id,
            target_state,
            committed_at_unix_ms,
        })
    }

    pub fn pipeline_name(&self) -> &str {
        &self.pipeline_name
    }

    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    pub fn source_position(&self) -> &CdcSourcePosition {
        &self.source_position
    }

    pub fn transaction_id(&self) -> Option<&CdcTransactionId> {
        self.transaction_id.as_ref()
    }

    pub fn target_state(&self) -> &BTreeMap<String, String> {
        &self.target_state
    }

    pub fn committed_at_unix_ms(&self) -> u64 {
        self.committed_at_unix_ms
    }
}

impl ReplicationPipelineDlqEntry {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pipeline_name: impl Into<String>,
        dlq_id: impl Into<String>,
        source_name: impl Into<String>,
        source_position: CdcSourcePosition,
        transaction_id: Option<CdcTransactionId>,
        error_class: impl Into<String>,
        error_message: impl Into<String>,
        attempt_count: u32,
        payload_object_key: Option<String>,
        payload_format: Option<String>,
        payload_bytes: usize,
        target_state: BTreeMap<String, String>,
        created_at_unix_ms: u64,
    ) -> Result<Self> {
        let entry = Self {
            pipeline_name: pipeline_name.into(),
            dlq_id: dlq_id.into(),
            source_name: source_name.into(),
            source_position,
            transaction_id,
            error_class: error_class.into(),
            error_message: error_message.into(),
            attempt_count,
            payload_object_key,
            payload_format,
            payload_bytes,
            target_state,
            status: ReplicationPipelineDlqStatus::Pending,
            status_reason: None,
            created_at_unix_ms,
            last_updated_at_unix_ms: created_at_unix_ms,
        };
        entry.validate()?;
        Ok(entry)
    }

    pub fn pipeline_name(&self) -> &str {
        &self.pipeline_name
    }

    pub fn dlq_id(&self) -> &str {
        &self.dlq_id
    }

    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    pub fn source_position(&self) -> &CdcSourcePosition {
        &self.source_position
    }

    pub fn transaction_id(&self) -> Option<&CdcTransactionId> {
        self.transaction_id.as_ref()
    }

    pub fn error_class(&self) -> &str {
        &self.error_class
    }

    pub fn error_message(&self) -> &str {
        &self.error_message
    }

    pub fn attempt_count(&self) -> u32 {
        self.attempt_count
    }

    pub fn payload_object_key(&self) -> Option<&str> {
        self.payload_object_key.as_deref()
    }

    pub fn payload_format(&self) -> Option<&str> {
        self.payload_format.as_deref()
    }

    pub fn payload_bytes(&self) -> usize {
        self.payload_bytes
    }

    pub fn target_state(&self) -> &BTreeMap<String, String> {
        &self.target_state
    }

    pub fn status(&self) -> ReplicationPipelineDlqStatus {
        self.status
    }

    pub fn status_reason(&self) -> Option<&str> {
        self.status_reason.as_deref()
    }

    pub fn created_at_unix_ms(&self) -> u64 {
        self.created_at_unix_ms
    }

    pub fn last_updated_at_unix_ms(&self) -> u64 {
        self.last_updated_at_unix_ms
    }

    pub fn with_status(
        mut self,
        status: ReplicationPipelineDlqStatus,
        last_updated_at_unix_ms: u64,
    ) -> Self {
        self.status = status;
        self.status_reason = None;
        self.last_updated_at_unix_ms = last_updated_at_unix_ms;
        self
    }

    pub fn with_status_reason(
        mut self,
        status: ReplicationPipelineDlqStatus,
        reason: Option<String>,
        last_updated_at_unix_ms: u64,
    ) -> Self {
        self.status = status;
        self.status_reason = reason.filter(|reason| !reason.trim().is_empty());
        self.last_updated_at_unix_ms = last_updated_at_unix_ms;
        self
    }

    pub fn record_attempt(mut self, last_updated_at_unix_ms: u64) -> Self {
        self.attempt_count = self.attempt_count.saturating_add(1);
        self.last_updated_at_unix_ms = last_updated_at_unix_ms;
        self
    }

    pub(super) fn validate(&self) -> Result<()> {
        ensure!(
            !self.pipeline_name.trim().is_empty(),
            "replication pipeline DLQ entry pipeline name cannot be empty"
        );
        ensure!(
            !self.dlq_id.trim().is_empty(),
            "replication pipeline DLQ entry id cannot be empty"
        );
        ensure!(
            !self.dlq_id.contains('/'),
            "replication pipeline DLQ entry id cannot contain '/'"
        );
        ensure!(
            !self.source_name.trim().is_empty(),
            "replication pipeline DLQ entry source name cannot be empty"
        );
        ensure!(
            !self.error_class.trim().is_empty(),
            "replication pipeline DLQ entry error class cannot be empty"
        );
        ensure!(
            !self.error_message.trim().is_empty(),
            "replication pipeline DLQ entry error message cannot be empty"
        );
        if let Some(payload_object_key) = self.payload_object_key.as_deref() {
            ensure!(
                !payload_object_key.trim().is_empty(),
                "replication pipeline DLQ entry payload object key cannot be empty"
            );
        }
        if let Some(payload_format) = self.payload_format.as_deref() {
            ensure!(
                !payload_format.trim().is_empty(),
                "replication pipeline DLQ entry payload format cannot be empty"
            );
        }
        Ok(())
    }
}

impl ReplicationPipelineDlqStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Replayed => "replayed",
            Self::Discarded => "discarded",
        }
    }
}
