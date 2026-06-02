use super::*;

impl DbspUnionNode {
    pub fn try_new(input_schemas: Vec<Arc<RowSchema>>) -> Result<Self> {
        if input_schemas.is_empty() {
            bail!("union requires at least one input");
        }
        let first = input_schemas[0].clone();
        for schema in &input_schemas[1..] {
            if schema.fields() != first.fields() {
                bail!("all union inputs must share the same schema");
            }
        }
        Ok(Self {
            output_schema: first,
        })
    }

    pub fn output_schema(&self) -> &Arc<RowSchema> {
        &self.output_schema
    }
}

#[derive(Clone, Debug)]
pub struct DbspDistinctNode {
    input_schema: Arc<RowSchema>,
}

impl DbspDistinctNode {
    pub fn new(input_schema: Arc<RowSchema>) -> Self {
        Self { input_schema }
    }

    pub fn output_schema(&self) -> &Arc<RowSchema> {
        &self.input_schema
    }
}

#[derive(Clone, Debug)]
pub struct DbspSinkNode {
    pub name: String,
    input_schema: Arc<RowSchema>,
}

impl DbspSinkNode {
    pub fn new(name: impl Into<String>, input_schema: Arc<RowSchema>) -> Self {
        Self {
            name: name.into(),
            input_schema,
        }
    }

    pub fn input_schema(&self) -> &Arc<RowSchema> {
        &self.input_schema
    }
}

#[derive(Clone, Debug)]
pub enum DbspNodeKind {
    Source(DbspSourceNode),
    Select(DbspSelectNode),
    Project(DbspProjectNode),
    Join(Box<DbspJoinNode>),
    Aggregate(DbspAggregateNode),
    WindowAggregate(DbspWindowAggregateNode),
    TopN(DbspTopNNode),
    Union(DbspUnionNode),
    Distinct(DbspDistinctNode),
    Passthrough,
    Sink(DbspSinkNode),
}
