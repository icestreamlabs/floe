use anyhow::{Result, anyhow};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FloeStatement {
    CreateTable(CreateTableDefinition),
    CreateMaterializedView(MaterializedViewDefinition),
    CreateSink(SinkDefinition),
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableColumnDefinition {
    name: String,
    data_type: SqlColumnType,
    nullable: bool,
    primary_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlColumnType {
    Int64,
    Bool,
    Utf8,
    TimestampMillis,
}

impl CreateTableDefinition {
    pub fn new(name: impl Into<String>, columns: Vec<CreateTableColumnDefinition>) -> Result<Self> {
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
        Ok(Self { name, columns })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[CreateTableColumnDefinition] {
        &self.columns
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
