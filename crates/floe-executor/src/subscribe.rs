use std::sync::Arc;

use anyhow::{Result, bail};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use tokio_util::sync::CancellationToken;

use crate::mv_changelog::{
    MvCatalog, MvChangelogExecutionConfig, MvChangelogParams, MvChangelogStream,
    execute_mv_changelog_with_config,
};
use floe_sql_parser::{FloeStatement, parse_floe_statement};

pub type SubscribeParams = MvChangelogParams;
pub type SubscribeExecutionConfig = MvChangelogExecutionConfig;
pub type SubscribeStream = MvChangelogStream;

pub async fn execute_subscribe_with_config<C>(
    catalog: &C,
    params: SubscribeParams,
    config: SubscribeExecutionConfig,
    cancel: CancellationToken,
) -> Result<SubscribeStream>
where
    C: MvCatalog + ?Sized,
{
    execute_mv_changelog_with_config(catalog, params, config, cancel).await
}

pub fn parse_subscribe_sql(sql: &str) -> Result<SubscribeParams> {
    match parse_floe_statement(sql)? {
        FloeStatement::Subscribe {
            mv_name,
            with_snapshot,
            as_of,
        } => Ok(SubscribeParams {
            mv_name,
            with_snapshot,
            as_of,
        }),
        other => bail!("unexpected statement parsed as {other:?}"),
    }
}

pub fn subscribe_output_schema<C>(catalog: &C, mv_name: &str) -> Result<SchemaRef>
where
    C: MvCatalog + ?Sized,
{
    let base = catalog.schema(mv_name).ok_or_else(|| {
        anyhow::anyhow!("materialized view '{}' is missing schema metadata", mv_name)
    })?;
    let mut fields = Vec::with_capacity(base.fields().len() + 3);
    fields.push(Field::new("floe_version", DataType::Int64, false));
    fields.push(Field::new("floe_diff", DataType::Int64, false));
    fields.push(Field::new(
        "floe_time",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        true,
    ));
    fields.extend(base.fields().iter().map(|field| (**field).clone()));
    Ok(Arc::new(Schema::new(fields)))
}
