use anyhow::{Result, anyhow};
pub use floe_core::catalog::{
    ColumnType as SqlColumnType, PostgresCdcSchemaEvolutionPolicy, ReplicationBufferMode,
    ReplicationBufferPolicy, ReplicationErrorPolicy, ReplicationErrorPolicyMode,
    ReplicationPipelineFormat, ReplicationPipelineTarget,
};

#[derive(Debug, Clone, PartialEq)]
pub enum FloeStatement {
    CreateSource(CreateSourceDefinition),
    CreateTable(CreateTableDefinition),
    CreateMaterializedView(MaterializedViewDefinition),
    CreateSink(SinkDefinition),
    CreateReplicationPipeline(ReplicationPipelineDefinition),
    Subscribe {
        mv_name: String,
        with_snapshot: bool,
        as_of: Option<i64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableDefinition {
    name: String,
    columns: Vec<CreateTableColumnDefinition>,
    source: Option<CreateTableSourceDefinition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableColumnDefinition {
    name: String,
    data_type: SqlColumnType,
    nullable: bool,
    primary_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableSourceDefinition {
    source_name: String,
    upstream_table: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateSourceDefinition {
    name: String,
    connector: SourceConnector,
    columns: Vec<CreateTableColumnDefinition>,
    if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SourceConnector {
    Kafka(KafkaSourceOptions),
    File(FileSourceOptions),
    Http(HttpSourceOptions),
    Generator(GeneratorSourceOptions),
    ObjectStore(ObjectStoreSourceOptions),
    PostgresCdc(PostgresCdcSourceOptions),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaSourceOptions {
    brokers: String,
    topics: Vec<String>,
    group_id: Option<String>,
    default_source: Option<String>,
    poll_ms: Option<u64>,
    max_messages_per_tick: Option<usize>,
    format: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSourceOptions {
    path: String,
    default_source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpSourceOptions {
    host: Option<String>,
    port: u16,
    default_source: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GeneratorSourceOptions {
    events_per_second: Option<f64>,
    max_events: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectStoreSourceOptions {
    url: String,
    default_source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresCdcSourceOptions {
    connection: String,
    slot: String,
    publication: Option<String>,
    include_schema_in_source: Option<bool>,
    schema_evolution_policy: PostgresCdcSchemaEvolutionPolicy,
    auto_create_slot: bool,
    auto_create_publication: bool,
}

impl CreateTableDefinition {
    pub fn new(name: impl Into<String>, columns: Vec<CreateTableColumnDefinition>) -> Result<Self> {
        Self::new_with_source(name, columns, None)
    }

    pub fn new_with_source(
        name: impl Into<String>,
        columns: Vec<CreateTableColumnDefinition>,
        source: Option<CreateTableSourceDefinition>,
    ) -> Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(anyhow!("table name cannot be empty"));
        }
        if columns.is_empty() {
            return Err(anyhow!("table {name} must declare at least one column"));
        }
        let pk_count = columns.iter().filter(|column| column.primary_key).count();
        if pk_count != 1 {
            return Err(anyhow!(
                "table {name} must declare exactly one primary key column"
            ));
        }
        Ok(Self {
            name,
            columns,
            source,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[CreateTableColumnDefinition] {
        &self.columns
    }

    pub fn source(&self) -> Option<&CreateTableSourceDefinition> {
        self.source.as_ref()
    }
}

impl CreateTableColumnDefinition {
    pub fn new(
        name: impl Into<String>,
        data_type: SqlColumnType,
        nullable: bool,
        primary_key: bool,
    ) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
            primary_key,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &SqlColumnType {
        &self.data_type
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }

    pub fn primary_key(&self) -> bool {
        self.primary_key
    }
}

impl CreateTableSourceDefinition {
    pub fn new(source_name: impl Into<String>, upstream_table: impl Into<String>) -> Result<Self> {
        let source_name = source_name.into();
        let upstream_table = upstream_table.into();
        if source_name.trim().is_empty() {
            return Err(anyhow!("source name cannot be empty"));
        }
        if upstream_table.trim().is_empty() {
            return Err(anyhow!("upstream table cannot be empty"));
        }
        Ok(Self {
            source_name,
            upstream_table,
        })
    }

    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    pub fn upstream_table(&self) -> &str {
        &self.upstream_table
    }
}

impl CreateSourceDefinition {
    pub fn new(name: impl Into<String>, connector: SourceConnector) -> Result<Self> {
        Self::new_with_columns(name, connector, Vec::new())
    }

    pub fn new_with_columns(
        name: impl Into<String>,
        connector: SourceConnector,
        columns: Vec<CreateTableColumnDefinition>,
    ) -> Result<Self> {
        Self::new_with_columns_and_if_not_exists(name, connector, columns, false)
    }

    pub fn new_with_columns_and_if_not_exists(
        name: impl Into<String>,
        connector: SourceConnector,
        columns: Vec<CreateTableColumnDefinition>,
        if_not_exists: bool,
    ) -> Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(anyhow!("source name cannot be empty"));
        }
        Ok(Self {
            name,
            connector,
            columns,
            if_not_exists,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn connector(&self) -> &SourceConnector {
        &self.connector
    }

    pub fn columns(&self) -> &[CreateTableColumnDefinition] {
        &self.columns
    }

    pub fn if_not_exists(&self) -> bool {
        self.if_not_exists
    }
}

impl KafkaSourceOptions {
    pub fn new(
        brokers: impl Into<String>,
        topics: Vec<String>,
        group_id: Option<String>,
        default_source: Option<String>,
        poll_ms: Option<u64>,
        max_messages_per_tick: Option<usize>,
        format: Option<String>,
    ) -> Result<Self> {
        let brokers = brokers.into();
        if brokers.trim().is_empty() {
            return Err(anyhow!("Kafka source brokers cannot be empty"));
        }
        if topics.is_empty() {
            return Err(anyhow!("Kafka source topics cannot be empty"));
        }
        if topics.iter().any(|topic| topic.trim().is_empty()) {
            return Err(anyhow!("Kafka source topics cannot contain empty values"));
        }
        Ok(Self {
            brokers,
            topics,
            group_id,
            default_source,
            poll_ms,
            max_messages_per_tick,
            format,
        })
    }

    pub fn brokers(&self) -> &str {
        &self.brokers
    }

    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    pub fn group_id(&self) -> Option<&str> {
        self.group_id.as_deref()
    }

    pub fn default_source(&self) -> Option<&str> {
        self.default_source.as_deref()
    }

    pub fn poll_ms(&self) -> Option<u64> {
        self.poll_ms
    }

    pub fn max_messages_per_tick(&self) -> Option<usize> {
        self.max_messages_per_tick
    }

    pub fn format(&self) -> Option<&str> {
        self.format.as_deref()
    }
}

impl FileSourceOptions {
    pub fn new(path: impl Into<String>, default_source: Option<String>) -> Result<Self> {
        let path = path.into();
        if path.trim().is_empty() {
            return Err(anyhow!("file source path cannot be empty"));
        }
        Ok(Self {
            path,
            default_source,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn default_source(&self) -> Option<&str> {
        self.default_source.as_deref()
    }
}

impl HttpSourceOptions {
    pub fn new(host: Option<String>, port: u16, default_source: Option<String>) -> Result<Self> {
        if port == 0 {
            return Err(anyhow!("HTTP source port must be greater than zero"));
        }
        Ok(Self {
            host,
            port,
            default_source,
        })
    }

    pub fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn default_source(&self) -> Option<&str> {
        self.default_source.as_deref()
    }
}

impl GeneratorSourceOptions {
    pub fn new(events_per_second: Option<f64>, max_events: Option<u64>) -> Result<Self> {
        if let Some(rate) = events_per_second
            && (!rate.is_finite() || rate <= 0.0)
        {
            return Err(anyhow!(
                "generator source events_per_second must be a positive finite value"
            ));
        }
        if let Some(max_events) = max_events
            && max_events == 0
        {
            return Err(anyhow!(
                "generator source max_events must be greater than zero"
            ));
        }
        Ok(Self {
            events_per_second,
            max_events,
        })
    }

    pub fn events_per_second(&self) -> Option<f64> {
        self.events_per_second
    }

    pub fn max_events(&self) -> Option<u64> {
        self.max_events
    }
}

impl ObjectStoreSourceOptions {
    pub fn new(url: impl Into<String>, default_source: Option<String>) -> Result<Self> {
        let url = url.into();
        if url.trim().is_empty() {
            return Err(anyhow!("object-store source url cannot be empty"));
        }
        Ok(Self {
            url,
            default_source,
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn default_source(&self) -> Option<&str> {
        self.default_source.as_deref()
    }
}

impl PostgresCdcSourceOptions {
    pub fn new(
        connection: impl Into<String>,
        slot: impl Into<String>,
        publication: Option<String>,
        include_schema_in_source: Option<bool>,
    ) -> Result<Self> {
        Self::new_with_schema_evolution_policy(
            connection,
            slot,
            publication,
            include_schema_in_source,
            PostgresCdcSchemaEvolutionPolicy::FailFast,
        )
    }

    pub fn new_with_schema_evolution_policy(
        connection: impl Into<String>,
        slot: impl Into<String>,
        publication: Option<String>,
        include_schema_in_source: Option<bool>,
        schema_evolution_policy: PostgresCdcSchemaEvolutionPolicy,
    ) -> Result<Self> {
        Self::new_with_setup_policy(
            connection,
            slot,
            publication,
            include_schema_in_source,
            schema_evolution_policy,
            true,
            true,
        )
    }

    pub fn new_with_setup_policy(
        connection: impl Into<String>,
        slot: impl Into<String>,
        publication: Option<String>,
        include_schema_in_source: Option<bool>,
        schema_evolution_policy: PostgresCdcSchemaEvolutionPolicy,
        auto_create_slot: bool,
        auto_create_publication: bool,
    ) -> Result<Self> {
        let connection = connection.into();
        let slot = slot.into();
        if connection.trim().is_empty() {
            return Err(anyhow!("Postgres CDC source connection cannot be empty"));
        }
        if slot.trim().is_empty() {
            return Err(anyhow!("Postgres CDC source slot cannot be empty"));
        }
        Ok(Self {
            connection,
            slot,
            publication,
            include_schema_in_source,
            schema_evolution_policy,
            auto_create_slot,
            auto_create_publication,
        })
    }

    pub fn connection(&self) -> &str {
        &self.connection
    }

    pub fn slot(&self) -> &str {
        &self.slot
    }

    pub fn publication(&self) -> Option<&str> {
        self.publication.as_deref()
    }

    pub fn include_schema_in_source(&self) -> Option<bool> {
        self.include_schema_in_source
    }

    pub fn schema_evolution_policy(&self) -> PostgresCdcSchemaEvolutionPolicy {
        self.schema_evolution_policy
    }

    pub fn auto_create_slot(&self) -> bool {
        self.auto_create_slot
    }

    pub fn auto_create_publication(&self) -> bool {
        self.auto_create_publication
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkDefinition {
    name: String,
    mv_name: String,
    connector: SinkConnector,
    with_snapshot: bool,
    as_of: Option<i64>,
    options: SinkOptions,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SinkOptions {
    batch_rows: Option<usize>,
    batch_bytes: Option<usize>,
    queue_capacity: Option<usize>,
    retry_max_attempts: Option<usize>,
    retry_base_ms: Option<u64>,
    retry_max_backoff_ms: Option<u64>,
    transactional_id: Option<String>,
    checkpoint_topic: Option<String>,
    checkpoint_partition: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkConnector {
    Kafka {
        brokers: String,
        topic: String,
        format: Option<String>,
        key_columns: Vec<String>,
    },
    File {
        path: String,
        append: Option<bool>,
    },
    Http {
        url: String,
        batch_size: Option<usize>,
    },
    Postgres {
        connection: String,
        table: String,
        mode: Option<String>,
        primary_key: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationPipelineDefinition {
    name: String,
    source_name: String,
    upstream_table: String,
    target: ReplicationPipelineTarget,
    format: ReplicationPipelineFormat,
    buffer_mode: ReplicationBufferMode,
    buffer_policy: ReplicationBufferPolicy,
    emit_tombstones: bool,
    include_transaction_metadata: bool,
    error_policy: ReplicationErrorPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationPipelineDefinitionParts {
    pub name: String,
    pub source_name: String,
    pub upstream_table: String,
    pub target: ReplicationPipelineTarget,
    pub format: ReplicationPipelineFormat,
    pub buffer_mode: ReplicationBufferMode,
    pub buffer_policy: ReplicationBufferPolicy,
    pub emit_tombstones: bool,
    pub include_transaction_metadata: bool,
    pub error_policy: ReplicationErrorPolicy,
}

impl SinkDefinition {
    pub fn new(
        name: impl Into<String>,
        mv_name: impl Into<String>,
        connector: SinkConnector,
        with_snapshot: bool,
        as_of: Option<i64>,
    ) -> Self {
        Self::new_with_options(
            name,
            mv_name,
            connector,
            with_snapshot,
            as_of,
            SinkOptions::default(),
        )
    }

    pub fn new_with_options(
        name: impl Into<String>,
        mv_name: impl Into<String>,
        connector: SinkConnector,
        with_snapshot: bool,
        as_of: Option<i64>,
        options: SinkOptions,
    ) -> Self {
        Self {
            name: name.into(),
            mv_name: mv_name.into(),
            connector,
            with_snapshot,
            as_of,
            options,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn mv_name(&self) -> &str {
        &self.mv_name
    }

    pub fn connector(&self) -> &SinkConnector {
        &self.connector
    }

    pub fn with_snapshot(&self) -> bool {
        self.with_snapshot
    }

    pub fn as_of(&self) -> Option<i64> {
        self.as_of
    }

    pub fn options(&self) -> &SinkOptions {
        &self.options
    }
}

impl SinkOptions {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        batch_rows: Option<usize>,
        batch_bytes: Option<usize>,
        queue_capacity: Option<usize>,
        retry_max_attempts: Option<usize>,
        retry_base_ms: Option<u64>,
        retry_max_backoff_ms: Option<u64>,
        transactional_id: Option<String>,
        checkpoint_topic: Option<String>,
        checkpoint_partition: Option<i32>,
    ) -> Self {
        Self {
            batch_rows,
            batch_bytes,
            queue_capacity,
            retry_max_attempts,
            retry_base_ms,
            retry_max_backoff_ms,
            transactional_id,
            checkpoint_topic,
            checkpoint_partition,
        }
    }

    pub fn batch_rows(&self) -> Option<usize> {
        self.batch_rows
    }

    pub fn batch_bytes(&self) -> Option<usize> {
        self.batch_bytes
    }

    pub fn queue_capacity(&self) -> Option<usize> {
        self.queue_capacity
    }

    pub fn retry_max_attempts(&self) -> Option<usize> {
        self.retry_max_attempts
    }

    pub fn retry_base_ms(&self) -> Option<u64> {
        self.retry_base_ms
    }

    pub fn retry_max_backoff_ms(&self) -> Option<u64> {
        self.retry_max_backoff_ms
    }

    pub fn transactional_id(&self) -> Option<&str> {
        self.transactional_id.as_deref()
    }

    pub fn checkpoint_topic(&self) -> Option<&str> {
        self.checkpoint_topic.as_deref()
    }

    pub fn checkpoint_partition(&self) -> Option<i32> {
        self.checkpoint_partition
    }
}

impl ReplicationPipelineDefinition {
    pub fn new(parts: ReplicationPipelineDefinitionParts) -> Result<Self> {
        let ReplicationPipelineDefinitionParts {
            name,
            source_name,
            upstream_table,
            target,
            format,
            buffer_mode,
            buffer_policy,
            emit_tombstones,
            include_transaction_metadata,
            error_policy,
        } = parts;
        if name.trim().is_empty() {
            return Err(anyhow!("replication pipeline name cannot be empty"));
        }
        if source_name.trim().is_empty() {
            return Err(anyhow!("replication pipeline source name cannot be empty"));
        }
        if upstream_table.trim().is_empty() {
            return Err(anyhow!(
                "replication pipeline upstream table cannot be empty"
            ));
        }
        target.validate()?;
        Ok(Self {
            name,
            source_name,
            upstream_table,
            target,
            format,
            buffer_mode,
            buffer_policy,
            emit_tombstones,
            include_transaction_metadata,
            error_policy,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    pub fn upstream_table(&self) -> &str {
        &self.upstream_table
    }

    pub fn target(&self) -> &ReplicationPipelineTarget {
        &self.target
    }

    pub fn format(&self) -> ReplicationPipelineFormat {
        self.format
    }

    pub fn buffer_mode(&self) -> ReplicationBufferMode {
        self.buffer_mode
    }

    pub fn buffer_policy(&self) -> ReplicationBufferPolicy {
        self.buffer_policy
    }

    pub fn emit_tombstones(&self) -> bool {
        self.emit_tombstones
    }

    pub fn include_transaction_metadata(&self) -> bool {
        self.include_transaction_metadata
    }

    pub fn error_policy(&self) -> ReplicationErrorPolicy {
        self.error_policy
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedViewDefinition {
    name: String,
    query: String,
    if_not_exists: bool,
}

impl MaterializedViewDefinition {
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
