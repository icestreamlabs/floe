use std::collections::BTreeMap;

use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};

use crate::ids::CdcSourceId;
use crate::schema::{CdcPrimaryKey, CdcTableDefinition};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CdcSourceCategory {
    AppendOnly,
    Upsert,
    NativeDatabaseCdc,
    Changelog,
    ObjectBatch,
    ExternalTable,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DirectQuerySupport {
    Full,
    StatelessOnly,
    None,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TableMaterializationRequirement {
    Optional,
    RequiredForStatefulQueries,
    AlwaysRequired,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcSourceSemantics {
    category: CdcSourceCategory,
    direct_query_support: DirectQuerySupport,
    table_materialization: TableMaterializationRequirement,
    primary_key_required_for_table: bool,
}

impl CdcSourceSemantics {
    pub fn for_category(category: CdcSourceCategory) -> Self {
        match category {
            CdcSourceCategory::AppendOnly => Self {
                category,
                direct_query_support: DirectQuerySupport::Full,
                table_materialization: TableMaterializationRequirement::Optional,
                primary_key_required_for_table: false,
            },
            CdcSourceCategory::Upsert => Self {
                category,
                direct_query_support: DirectQuerySupport::StatelessOnly,
                table_materialization: TableMaterializationRequirement::RequiredForStatefulQueries,
                primary_key_required_for_table: true,
            },
            CdcSourceCategory::NativeDatabaseCdc => Self {
                category,
                direct_query_support: DirectQuerySupport::None,
                table_materialization: TableMaterializationRequirement::AlwaysRequired,
                primary_key_required_for_table: true,
            },
            CdcSourceCategory::Changelog => Self {
                category,
                direct_query_support: DirectQuerySupport::StatelessOnly,
                table_materialization: TableMaterializationRequirement::RequiredForStatefulQueries,
                primary_key_required_for_table: true,
            },
            CdcSourceCategory::ObjectBatch | CdcSourceCategory::ExternalTable => Self {
                category,
                direct_query_support: DirectQuerySupport::Full,
                table_materialization: TableMaterializationRequirement::Optional,
                primary_key_required_for_table: false,
            },
        }
    }

    pub fn category(&self) -> CdcSourceCategory {
        self.category
    }

    pub fn direct_query_support(&self) -> DirectQuerySupport {
        self.direct_query_support
    }

    pub fn table_materialization(&self) -> TableMaterializationRequirement {
        self.table_materialization
    }

    pub fn primary_key_required_for_table(&self) -> bool {
        self.primary_key_required_for_table
    }

    pub fn validate_direct_query(&self, stateful: bool) -> Result<()> {
        match (self.direct_query_support, stateful) {
            (DirectQuerySupport::Full, _) | (DirectQuerySupport::StatelessOnly, false) => Ok(()),
            (DirectQuerySupport::StatelessOnly, true) => bail!(
                "{:?} sources must be materialized as tables before stateful queries",
                self.category
            ),
            (DirectQuerySupport::None, _) => bail!(
                "{:?} sources must be materialized as tables before they can be queried",
                self.category
            ),
        }
    }

    pub fn validate_table_primary_key(&self, primary_key: Option<&CdcPrimaryKey>) -> Result<()> {
        if self.primary_key_required_for_table && primary_key.is_none() {
            bail!("{:?} tables require a primary key", self.category);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcSourceDefinition {
    source_id: CdcSourceId,
    connector: String,
    semantics: CdcSourceSemantics,
    #[serde(default)]
    properties: BTreeMap<String, String>,
}

impl CdcSourceDefinition {
    pub fn new(
        source_id: CdcSourceId,
        connector: impl Into<String>,
        semantics: CdcSourceSemantics,
    ) -> Result<Self> {
        let connector = connector.into();
        ensure!(
            !connector.trim().is_empty(),
            "CDC source connector cannot be empty"
        );
        Ok(Self {
            source_id,
            connector,
            semantics,
            properties: BTreeMap::new(),
        })
    }

    pub fn postgres(source_id: CdcSourceId) -> Result<Self> {
        Self::new(
            source_id,
            "postgres-cdc",
            CdcSourceSemantics::for_category(CdcSourceCategory::NativeDatabaseCdc),
        )
    }

    pub fn source_id(&self) -> &CdcSourceId {
        &self.source_id
    }

    pub fn connector(&self) -> &str {
        &self.connector
    }

    pub fn semantics(&self) -> CdcSourceSemantics {
        self.semantics
    }

    pub fn properties(&self) -> &BTreeMap<String, String> {
        &self.properties
    }

    pub fn property(&self, key: &str) -> Option<&str> {
        self.properties.get(key).map(String::as_str)
    }

    pub fn with_property(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self> {
        self.set_property(key, value)?;
        Ok(self)
    }

    pub fn set_property(&mut self, key: impl Into<String>, value: impl Into<String>) -> Result<()> {
        let key = key.into();
        ensure!(
            !key.trim().is_empty(),
            "CDC source property key cannot be empty"
        );
        self.properties.insert(key, value.into());
        Ok(())
    }

    pub fn validate_table_definition(&self, table: &CdcTableDefinition) -> Result<()> {
        ensure!(
            table.source_id() == &self.source_id,
            "CDC table '{}' belongs to source '{}', not '{}'",
            table.table_id().as_str(),
            table.source_id().as_str(),
            self.source_id.as_str()
        );
        self.semantics
            .validate_table_primary_key(Some(table.schema().primary_key()))
    }
}
