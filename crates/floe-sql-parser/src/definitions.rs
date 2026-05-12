use anyhow::{Result, anyhow};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FloeStatement {
    CreateSource(CreateSourceDefinition),
    CreateTable(CreateTableDefinition),
    CreateMaterializedView(MaterializedViewDefinition),
    CreateSink(SinkDefinition),
    CreateReplicationPipeline(ReplicationPipelineDefinition),
    Tail {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlColumnType {
    Int64,
    Bool,
    Utf8,
    TimestampMillis,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSourceDefinition {
    name: String,
    connector: SourceConnector,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceConnector {
    PostgresCdc(PostgresCdcSourceOptions),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresCdcSourceOptions {
    connection: String,
    slot: String,
    publication: Option<String>,
    include_schema_in_source: Option<bool>,
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
        let name = name.into();
        if name.trim().is_empty() {
            return Err(anyhow!("source name cannot be empty"));
        }
        Ok(Self { name, connector })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn connector(&self) -> &SourceConnector {
        &self.connector
    }
}

impl PostgresCdcSourceOptions {
    pub fn new(
        connection: impl Into<String>,
        slot: impl Into<String>,
        publication: Option<String>,
        include_schema_in_source: Option<bool>,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkDefinition {
    name: String,
    mv_name: String,
    connector: SinkConnector,
    with_snapshot: bool,
    as_of: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkConnector {
    Kafka {
        brokers: String,
        topic: String,
    },
    File {
        path: String,
        append: Option<bool>,
    },
    Http {
        url: String,
        batch_size: Option<usize>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationPipelineDefinition {
    name: String,
    source_name: String,
    upstream_table: String,
    target: ReplicationPipelineTarget,
    format: ReplicationPipelineFormat,
    delivery: ReplicationDelivery,
    emit_tombstones: bool,
    include_transaction_metadata: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationPipelineTarget {
    Kafka { brokers: String, topic: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationPipelineFormat {
    DebeziumJson,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationDelivery {
    AtLeastOnce,
}

impl SinkDefinition {
    pub fn new(
        name: impl Into<String>,
        mv_name: impl Into<String>,
        connector: SinkConnector,
        with_snapshot: bool,
        as_of: Option<i64>,
    ) -> Self {
        Self {
            name: name.into(),
            mv_name: mv_name.into(),
            connector,
            with_snapshot,
            as_of,
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
}

impl ReplicationPipelineDefinition {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: impl Into<String>,
        source_name: impl Into<String>,
        upstream_table: impl Into<String>,
        target: ReplicationPipelineTarget,
        format: ReplicationPipelineFormat,
        delivery: ReplicationDelivery,
        emit_tombstones: bool,
        include_transaction_metadata: bool,
    ) -> Result<Self> {
        let name = name.into();
        let source_name = source_name.into();
        let upstream_table = upstream_table.into();
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
            delivery,
            emit_tombstones,
            include_transaction_metadata,
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

    pub fn delivery(&self) -> ReplicationDelivery {
        self.delivery
    }

    pub fn emit_tombstones(&self) -> bool {
        self.emit_tombstones
    }

    pub fn include_transaction_metadata(&self) -> bool {
        self.include_transaction_metadata
    }
}

impl ReplicationPipelineTarget {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Kafka { brokers, topic } => {
                if brokers.trim().is_empty() {
                    return Err(anyhow!(
                        "replication pipeline Kafka brokers cannot be empty"
                    ));
                }
                if topic.trim().is_empty() {
                    return Err(anyhow!("replication pipeline Kafka topic cannot be empty"));
                }
            }
        }
        Ok(())
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

    #[allow(dead_code)]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[allow(dead_code)]
    pub fn query(&self) -> &str {
        &self.query
    }

    #[allow(dead_code)]
    pub fn if_not_exists(&self) -> bool {
        self.if_not_exists
    }
}
