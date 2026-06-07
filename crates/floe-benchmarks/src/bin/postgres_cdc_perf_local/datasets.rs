use anyhow::{Result, bail};

use super::{BenchMode, Config, Dataset, TargetKind};

#[derive(Debug, Clone)]
pub(super) struct DatasetPlan {
    pub(super) source_table: String,
    pub(super) upstream_table: String,
    pub(super) pipeline_name: String,
    pub(super) upstream_tables: Vec<String>,
    pub(super) pipeline_names: Vec<String>,
    pub(super) topics: Vec<String>,
    pub(super) target_tables: Vec<String>,
}

#[derive(Debug, Clone)]
pub(super) struct LoadPlan {
    pub(super) initial_rows: u64,
    pub(super) live_insert_rows: u64,
    pub(super) live_update_rows: u64,
}

impl LoadPlan {
    pub(super) fn for_config(config: &Config) -> Self {
        match config.bench_mode {
            BenchMode::LiveInsert => Self {
                initial_rows: 0,
                live_insert_rows: config.rows,
                live_update_rows: 0,
            },
            BenchMode::SnapshotLiveUpdate => Self {
                initial_rows: config.rows,
                live_insert_rows: 0,
                live_update_rows: config.rows,
            },
            BenchMode::Snapshot => Self {
                initial_rows: config.rows,
                live_insert_rows: 0,
                live_update_rows: 0,
            },
        }
    }

    pub(super) fn source_rows(&self) -> u64 {
        self.initial_rows + self.live_insert_rows + self.live_update_rows
    }
}

pub(super) fn dataset_plan(config: &Config) -> Result<DatasetPlan> {
    let mut plan = match config.dataset {
        Dataset::SyntheticOrders => DatasetPlan::single(
            "orders",
            "public.orders",
            "pg_orders_to_kafka",
            &config.topic,
        ),
        Dataset::TpchLineitemFlat => DatasetPlan::single(
            "lineitem_flat",
            "public.lineitem_flat",
            "pg_lineitem_flat_to_kafka",
            default_topic(config, "floe_cdc_bench_lineitem_flat").as_str(),
        ),
        Dataset::TpchLineitem => DatasetPlan::single(
            "lineitem",
            "public.lineitem",
            "pg_lineitem_to_kafka",
            default_topic(config, "floe_cdc_bench_lineitem").as_str(),
        ),
        Dataset::TpchTop2 => {
            let topic = default_topic(config, "floe_cdc_bench_tpch_top2");
            DatasetPlan::multi(
                "orders,lineitem",
                "public.orders,public.lineitem",
                "pg_orders_to_kafka,pg_lineitem_to_kafka",
                ["public.orders", "public.lineitem"],
                ["pg_orders_to_kafka", "pg_lineitem_to_kafka"],
                [format!("{topic}_orders"), format!("{topic}_lineitem")],
            )
        }
        Dataset::TpchAll => {
            let topic = default_topic(config, "floe_cdc_bench_tpch");
            DatasetPlan::multi(
                "region,nation,supplier,customer,part,partsupp,orders,lineitem",
                "public.region,public.nation,public.supplier,public.customer,public.part,public.partsupp,public.orders,public.lineitem",
                "pg_region_to_kafka,pg_nation_to_kafka,pg_supplier_to_kafka,pg_customer_to_kafka,pg_part_to_kafka,pg_partsupp_to_kafka,pg_orders_to_kafka,pg_lineitem_to_kafka",
                [
                    "public.region",
                    "public.nation",
                    "public.supplier",
                    "public.customer",
                    "public.part",
                    "public.partsupp",
                    "public.orders",
                    "public.lineitem",
                ],
                [
                    "pg_region_to_kafka",
                    "pg_nation_to_kafka",
                    "pg_supplier_to_kafka",
                    "pg_customer_to_kafka",
                    "pg_part_to_kafka",
                    "pg_partsupp_to_kafka",
                    "pg_orders_to_kafka",
                    "pg_lineitem_to_kafka",
                ],
                [
                    format!("{topic}_region"),
                    format!("{topic}_nation"),
                    format!("{topic}_supplier"),
                    format!("{topic}_customer"),
                    format!("{topic}_part"),
                    format!("{topic}_partsupp"),
                    format!("{topic}_orders"),
                    format!("{topic}_lineitem"),
                ],
            )
        }
    };
    plan.target_tables = plan
        .upstream_tables
        .iter()
        .map(|table| postgres_target_table_for_upstream(table))
        .collect();
    if config.target == TargetKind::Postgres {
        for name in &mut plan.pipeline_names {
            *name = name.replace("_to_kafka", "_to_postgres");
        }
        plan.pipeline_name = plan.pipeline_names.join(",");
    }
    Ok(plan)
}

impl DatasetPlan {
    fn single(source_table: &str, upstream_table: &str, pipeline_name: &str, topic: &str) -> Self {
        Self {
            source_table: source_table.to_string(),
            upstream_table: upstream_table.to_string(),
            pipeline_name: pipeline_name.to_string(),
            upstream_tables: vec![upstream_table.to_string()],
            pipeline_names: vec![pipeline_name.to_string()],
            topics: vec![topic.to_string()],
            target_tables: Vec::new(),
        }
    }

    fn multi<const N: usize>(
        source_table: &str,
        upstream_table: &str,
        pipeline_name: &str,
        upstream_tables: [&str; N],
        pipeline_names: [&str; N],
        topics: [String; N],
    ) -> Self {
        Self {
            source_table: source_table.to_string(),
            upstream_table: upstream_table.to_string(),
            pipeline_name: pipeline_name.to_string(),
            upstream_tables: upstream_tables
                .iter()
                .map(|value| value.to_string())
                .collect(),
            pipeline_names: pipeline_names
                .iter()
                .map(|value| value.to_string())
                .collect(),
            topics: topics.into_iter().collect(),
            target_tables: Vec::new(),
        }
    }

    pub(super) fn topic_list(&self) -> String {
        self.topics.join(",")
    }

    pub(super) fn target_table_list(&self) -> String {
        self.target_tables.join(",")
    }
}

fn default_topic(config: &Config, default: &str) -> String {
    if config.topic == "floe_cdc_bench_orders" {
        default.to_string()
    } else {
        config.topic.clone()
    }
}

fn postgres_target_table_for_upstream(upstream: &str) -> String {
    let (schema, table) = upstream
        .split_once('.')
        .map_or(("public", upstream), |(schema, table)| (schema, table));
    format!("{schema}.{table}_sink")
}

pub(super) fn create_postgres_sink_table_sql(upstream: &str, target: &str) -> String {
    format!(
        "DROP TABLE IF EXISTS {target};\nCREATE TABLE {target} (LIKE {upstream} INCLUDING ALL);"
    )
}

pub(super) fn replication_sql(config: &Config, plan: &DatasetPlan) -> String {
    let mut sql = format!(
        "CREATE SOURCE pg_main WITH (\n  connector = 'postgres-cdc',\n  connection = '{}',\n  slot.name = '{}',\n  publication.name = '{}'\n);\n",
        config.pg_dsn(),
        config.slot,
        config.publication
    );
    for (idx, pipeline) in plan.pipeline_names.iter().enumerate() {
        sql.push_str(&format!(
            "CREATE REPLICATION PIPELINE {pipeline}\nFROM pg_main TABLE '{}'\n",
            plan.upstream_tables[idx]
        ));
        match config.target {
            TargetKind::Kafka => sql.push_str(&format!(
                "INTO KAFKA WITH (\n  brokers = '{}',\n  topic = '{}',\n",
                config.brokers, plan.topics[idx]
            )),
            TargetKind::Postgres => sql.push_str(&format!(
                "INTO POSTGRES WITH (\n  connection = '{}',\n  table = '{}',\n",
                config.pg_dsn(),
                plan.target_tables[idx]
            )),
        }
        sql.push_str(&format!(
            "  format = '{}',\n  durable_buffer = {},\n",
            config.pipeline_format, config.durable_replication_buffer
        ));
        if let Some(value) = config.buffer_max_pending_bytes {
            sql.push_str(&format!("  buffer.max_pending_bytes = {value},\n"));
        }
        if let Some(value) = config.buffer_max_pending_records {
            sql.push_str(&format!("  buffer.max_pending_records = {value},\n"));
        }
        if let Some(value) = config.buffer_max_pending_objects {
            sql.push_str(&format!("  buffer.max_pending_objects = {value},\n"));
        }
        if let Some(value) = config.buffer_max_pending_age_ms {
            sql.push_str(&format!("  buffer.max_pending_age_ms = {value},\n"));
        }
        sql.push_str("  tombstones = false,\n  transaction_metadata = false\n);\n");
    }
    sql
}

pub(super) fn validate_dataset_mode(config: &Config) -> Result<()> {
    match (config.dataset, config.bench_mode) {
        (
            Dataset::TpchLineitemFlat | Dataset::TpchLineitem | Dataset::TpchAll,
            BenchMode::Snapshot,
        ) => Ok(()),
        (Dataset::TpchLineitemFlat | Dataset::TpchLineitem | Dataset::TpchAll, _) => {
            bail!(
                "DATASET={} currently supports BENCH_MODE=snapshot only",
                config.dataset.as_str()
            )
        }
        (Dataset::TpchTop2, BenchMode::SnapshotLiveUpdate) => {
            bail!("DATASET=tpch-top2 currently supports BENCH_MODE=snapshot or live_insert")
        }
        _ => Ok(()),
    }
}

pub(super) fn expected_insert_messages(rows: u64, config: &Config) -> Result<u64> {
    match normalized_format(&config.pipeline_format).as_str() {
        "debezium_json" | "floe_json" | "compact_json" => Ok(rows),
        "arrow_ipc" => Ok(rows.div_ceil(config.arrow_ipc_rows_per_record)),
        other => bail!("unsupported PIPELINE_FORMAT={other}"),
    }
}

pub(super) fn expected_update_messages(rows: u64, config: &Config) -> Result<u64> {
    match normalized_format(&config.pipeline_format).as_str() {
        "debezium_json" | "floe_json" | "compact_json" => Ok(rows),
        "arrow_ipc" => Ok((rows * 2).div_ceil(config.arrow_ipc_rows_per_record)),
        other => bail!("unsupported PIPELINE_FORMAT={other}"),
    }
}

pub(super) fn expected_messages_for_chunks(
    rows: u64,
    chunk: u64,
    config: &Config,
    update: bool,
) -> Result<u64> {
    if chunk == 0 || chunk >= rows {
        return if update {
            expected_update_messages(rows, config)
        } else {
            expected_insert_messages(rows, config)
        };
    }
    let full_chunks = rows / chunk;
    let remainder = rows % chunk;
    let per_full = if update {
        expected_update_messages(chunk, config)?
    } else {
        expected_insert_messages(chunk, config)?
    };
    let mut total = full_chunks * per_full;
    if remainder > 0 {
        total += if update {
            expected_update_messages(remainder, config)?
        } else {
            expected_insert_messages(remainder, config)?
        };
    }
    Ok(total)
}

pub(super) fn expected_tpch_top2_live_insert_messages(
    rows: u64,
    chunk: u64,
    config: &Config,
) -> Result<u64> {
    let chunk = if chunk == 0 || chunk > rows {
        rows
    } else {
        chunk
    };
    let mut remaining = rows;
    let mut total = 0;
    while remaining > 0 {
        let chunk_rows = chunk.min(remaining);
        let order_rows = tpch_top2_chunk_orders(chunk_rows);
        let lineitem_rows = chunk_rows - order_rows;
        total += expected_insert_messages(order_rows, config)?;
        if lineitem_rows > 0 {
            total += expected_insert_messages(lineitem_rows, config)?;
        }
        remaining -= chunk_rows;
    }
    Ok(total)
}

pub(super) fn tpch_top2_chunk_orders(chunk_rows: u64) -> u64 {
    chunk_rows.div_ceil(5).min(chunk_rows)
}

fn normalized_format(format: &str) -> String {
    format.to_ascii_lowercase().replace('-', "_")
}

pub(super) fn synthetic_orders_sql(initial_rows: u64, publication: &str) -> String {
    format!(
        "DROP PUBLICATION IF EXISTS {publication};\nDROP TABLE IF EXISTS public.orders;\nCREATE TABLE public.orders (\n  id BIGINT PRIMARY KEY,\n  customer_id BIGINT NOT NULL,\n  amount BIGINT NOT NULL,\n  status TEXT,\n  created_at BIGINT NOT NULL\n);\nINSERT INTO public.orders\nSELECT\n  gs::BIGINT AS id,\n  (gs % 100000)::BIGINT AS customer_id,\n  (100 + (gs % 10000))::BIGINT AS amount,\n  CASE WHEN gs % 3 = 0 THEN 'paid' WHEN gs % 3 = 1 THEN 'open' ELSE 'cancelled' END AS status,\n  (1700000000000 + gs)::BIGINT AS created_at\nFROM generate_series(1, {initial_rows}) AS gs;"
    )
}

pub(super) fn synthetic_live_insert_sql(start: u64, end: u64) -> String {
    format!(
        "INSERT INTO public.orders\nSELECT\n  gs::BIGINT AS id,\n  (gs % 100000)::BIGINT AS customer_id,\n  (100 + (gs % 10000))::BIGINT AS amount,\n  CASE WHEN gs % 3 = 0 THEN 'paid' WHEN gs % 3 = 1 THEN 'open' ELSE 'cancelled' END AS status,\n  (1700000000000 + gs)::BIGINT AS created_at\nFROM generate_series({start}, {end}) AS gs;"
    )
}

pub(super) fn synthetic_live_update_sql(start: u64, end: u64) -> String {
    format!(
        "UPDATE public.orders\nSET amount = amount + 1,\n    status = 'updated'\nWHERE id BETWEEN {start} AND {end};"
    )
}

pub(super) fn tpch_top2_live_insert_sql(
    order_start: u64,
    order_end: u64,
    lineitem_start: u64,
    lineitem_end: u64,
) -> String {
    format!(
        "BEGIN;
INSERT INTO public.orders
SELECT
  gs::BIGINT AS o_orderkey,
  ((gs % 150000) + 1)::BIGINT AS o_custkey,
  'O'::CHAR(1) AS o_orderstatus,
  ((100000 + (gs % 100000))::NUMERIC / 100)::NUMERIC(15,2) AS o_totalprice,
  (DATE '1992-01-01' + ((gs % 2500)::INT))::DATE AS o_orderdate,
  '5-LOW'::CHAR(15) AS o_orderpriority,
  ('Clerk#' || LPAD((gs % 1000)::TEXT, 9, '0'))::CHAR(15) AS o_clerk,
  0::BIGINT AS o_shippriority,
  ('live order ' || gs)::VARCHAR(79) AS o_comment
FROM generate_series({order_start}, {order_end}) AS gs;

INSERT INTO public.lineitem
SELECT
  (((gs - 1) / 4) + 1)::BIGINT AS l_orderkey,
  ((gs % 200000) + 1)::BIGINT AS l_partkey,
  ((gs % 10000) + 1)::BIGINT AS l_suppkey,
  (((gs - 1) % 4) + 1)::BIGINT AS l_linenumber,
  ((1 + (gs % 50))::NUMERIC)::NUMERIC(15,2) AS l_quantity,
  ((10000 + (gs % 100000))::NUMERIC / 100)::NUMERIC(15,2) AS l_extendedprice,
  ((gs % 10)::NUMERIC / 100)::NUMERIC(15,2) AS l_discount,
  ((gs % 8)::NUMERIC / 100)::NUMERIC(15,2) AS l_tax,
  'N'::CHAR(1) AS l_returnflag,
  'O'::CHAR(1) AS l_linestatus,
  (DATE '1992-01-01' + ((gs % 2500)::INT))::DATE AS l_shipdate,
  (DATE '1992-01-02' + ((gs % 2500)::INT))::DATE AS l_commitdate,
  (DATE '1992-01-03' + ((gs % 2500)::INT))::DATE AS l_receiptdate,
  'DELIVER IN PERSON'::CHAR(25) AS l_shipinstruct,
  'AIR'::CHAR(10) AS l_shipmode,
  ('live lineitem ' || gs)::VARCHAR(44) AS l_comment
FROM generate_series({lineitem_start}, {lineitem_end}) AS gs;
COMMIT;"
    )
}

pub(super) fn lineitem_schema_sql(publication: &str) -> String {
    format!(
        "DROP PUBLICATION IF EXISTS {publication};\nDROP TABLE IF EXISTS public.lineitem;\nCREATE TABLE public.lineitem (\n  l_orderkey BIGINT NOT NULL,\n  l_partkey BIGINT NOT NULL,\n  l_suppkey BIGINT NOT NULL,\n  l_linenumber BIGINT NOT NULL,\n  l_quantity NUMERIC(15,2) NOT NULL,\n  l_extendedprice NUMERIC(15,2) NOT NULL,\n  l_discount NUMERIC(15,2) NOT NULL,\n  l_tax NUMERIC(15,2) NOT NULL,\n  l_returnflag CHAR(1) NOT NULL,\n  l_linestatus CHAR(1) NOT NULL,\n  l_shipdate DATE NOT NULL,\n  l_commitdate DATE NOT NULL,\n  l_receiptdate DATE NOT NULL,\n  l_shipinstruct CHAR(25) NOT NULL,\n  l_shipmode CHAR(10) NOT NULL,\n  l_comment VARCHAR(44) NOT NULL,\n  PRIMARY KEY (l_orderkey, l_linenumber)\n);"
    )
}

pub(super) fn lineitem_flat_stage_sql(publication: &str) -> String {
    format!(
        "DROP PUBLICATION IF EXISTS {publication};\nDROP TABLE IF EXISTS public.lineitem_flat_stage;\nDROP TABLE IF EXISTS public.lineitem_flat;\nCREATE TABLE public.lineitem_flat_stage (\n  l_orderkey TEXT NOT NULL,\n  l_partkey TEXT NOT NULL,\n  l_suppkey TEXT NOT NULL,\n  l_linenumber TEXT NOT NULL,\n  l_quantity TEXT NOT NULL,\n  l_extendedprice TEXT NOT NULL,\n  l_discount TEXT NOT NULL,\n  l_tax TEXT NOT NULL,\n  l_returnflag TEXT NOT NULL,\n  l_linestatus TEXT NOT NULL,\n  l_shipdate TEXT NOT NULL,\n  l_commitdate TEXT NOT NULL,\n  l_receiptdate TEXT NOT NULL,\n  l_shipinstruct TEXT NOT NULL,\n  l_shipmode TEXT NOT NULL,\n  l_comment TEXT NOT NULL\n);"
    )
}

pub(super) fn lineitem_flat_finish_sql() -> &'static str {
    "CREATE TABLE public.lineitem_flat (
  l_orderkey BIGINT NOT NULL,
  l_partkey BIGINT NOT NULL,
  l_suppkey BIGINT NOT NULL,
  l_linenumber BIGINT NOT NULL,
  l_quantity BIGINT NOT NULL,
  l_extendedprice_cents BIGINT NOT NULL,
  l_discount_bps BIGINT NOT NULL,
  l_tax_bps BIGINT NOT NULL,
  l_returnflag TEXT NOT NULL,
  l_linestatus TEXT NOT NULL,
  l_shipdate_days BIGINT NOT NULL,
  l_commitdate_days BIGINT NOT NULL,
  l_receiptdate_days BIGINT NOT NULL,
  l_shipinstruct TEXT NOT NULL,
  l_shipmode TEXT NOT NULL,
  l_comment TEXT NOT NULL,
  PRIMARY KEY (l_orderkey, l_linenumber)
);
INSERT INTO public.lineitem_flat
SELECT
  l_orderkey::BIGINT,
  l_partkey::BIGINT,
  l_suppkey::BIGINT,
  l_linenumber::BIGINT,
  ROUND(l_quantity::NUMERIC)::BIGINT,
  ROUND(l_extendedprice::NUMERIC * 100)::BIGINT,
  ROUND(l_discount::NUMERIC * 10000)::BIGINT,
  ROUND(l_tax::NUMERIC * 10000)::BIGINT,
  l_returnflag,
  l_linestatus,
  (l_shipdate::DATE - DATE '1970-01-01')::BIGINT,
  (l_commitdate::DATE - DATE '1970-01-01')::BIGINT,
  (l_receiptdate::DATE - DATE '1970-01-01')::BIGINT,
  l_shipinstruct,
  l_shipmode,
  l_comment
FROM public.lineitem_flat_stage;
DROP TABLE public.lineitem_flat_stage;"
}

pub(super) fn tpch_top2_schema_sql(publication: &str) -> String {
    format!(
        "DROP PUBLICATION IF EXISTS {publication};\nDROP TABLE IF EXISTS public.lineitem;\nDROP TABLE IF EXISTS public.orders;\n{}\n{}",
        orders_schema_sql(),
        lineitem_table_sql()
    )
}

pub(super) fn tpch_all_schema_sql(publication: &str) -> String {
    format!(
        "DROP PUBLICATION IF EXISTS {publication};
DROP TABLE IF EXISTS public.lineitem;
DROP TABLE IF EXISTS public.orders;
DROP TABLE IF EXISTS public.partsupp;
DROP TABLE IF EXISTS public.part;
DROP TABLE IF EXISTS public.customer;
DROP TABLE IF EXISTS public.supplier;
DROP TABLE IF EXISTS public.nation;
DROP TABLE IF EXISTS public.region;
CREATE TABLE public.region (
  r_regionkey BIGINT PRIMARY KEY,
  r_name CHAR(25) NOT NULL,
  r_comment VARCHAR(152) NOT NULL
);
CREATE TABLE public.nation (
  n_nationkey BIGINT PRIMARY KEY,
  n_name CHAR(25) NOT NULL,
  n_regionkey BIGINT NOT NULL,
  n_comment VARCHAR(152) NOT NULL
);
CREATE TABLE public.supplier (
  s_suppkey BIGINT PRIMARY KEY,
  s_name CHAR(25) NOT NULL,
  s_address VARCHAR(40) NOT NULL,
  s_nationkey BIGINT NOT NULL,
  s_phone CHAR(15) NOT NULL,
  s_acctbal NUMERIC(15,2) NOT NULL,
  s_comment VARCHAR(101) NOT NULL
);
CREATE TABLE public.customer (
  c_custkey BIGINT PRIMARY KEY,
  c_name VARCHAR(25) NOT NULL,
  c_address VARCHAR(40) NOT NULL,
  c_nationkey BIGINT NOT NULL,
  c_phone CHAR(15) NOT NULL,
  c_acctbal NUMERIC(15,2) NOT NULL,
  c_mktsegment CHAR(10) NOT NULL,
  c_comment VARCHAR(117) NOT NULL
);
CREATE TABLE public.part (
  p_partkey BIGINT PRIMARY KEY,
  p_name VARCHAR(55) NOT NULL,
  p_mfgr CHAR(25) NOT NULL,
  p_brand CHAR(10) NOT NULL,
  p_type VARCHAR(25) NOT NULL,
  p_size BIGINT NOT NULL,
  p_container CHAR(10) NOT NULL,
  p_retailprice NUMERIC(15,2) NOT NULL,
  p_comment VARCHAR(23) NOT NULL
);
CREATE TABLE public.partsupp (
  ps_partkey BIGINT NOT NULL,
  ps_suppkey BIGINT NOT NULL,
  ps_availqty BIGINT NOT NULL,
  ps_supplycost NUMERIC(15,2) NOT NULL,
  ps_comment VARCHAR(199) NOT NULL,
  PRIMARY KEY (ps_partkey, ps_suppkey)
);
{}
{}",
        orders_schema_sql(),
        lineitem_table_sql()
    )
}

fn orders_schema_sql() -> &'static str {
    "CREATE TABLE public.orders (
  o_orderkey BIGINT PRIMARY KEY,
  o_custkey BIGINT NOT NULL,
  o_orderstatus CHAR(1) NOT NULL,
  o_totalprice NUMERIC(15,2) NOT NULL,
  o_orderdate DATE NOT NULL,
  o_orderpriority CHAR(15) NOT NULL,
  o_clerk CHAR(15) NOT NULL,
  o_shippriority BIGINT NOT NULL,
  o_comment VARCHAR(79) NOT NULL
);"
}

fn lineitem_table_sql() -> &'static str {
    "CREATE TABLE public.lineitem (
  l_orderkey BIGINT NOT NULL,
  l_partkey BIGINT NOT NULL,
  l_suppkey BIGINT NOT NULL,
  l_linenumber BIGINT NOT NULL,
  l_quantity NUMERIC(15,2) NOT NULL,
  l_extendedprice NUMERIC(15,2) NOT NULL,
  l_discount NUMERIC(15,2) NOT NULL,
  l_tax NUMERIC(15,2) NOT NULL,
  l_returnflag CHAR(1) NOT NULL,
  l_linestatus CHAR(1) NOT NULL,
  l_shipdate DATE NOT NULL,
  l_commitdate DATE NOT NULL,
  l_receiptdate DATE NOT NULL,
  l_shipinstruct CHAR(25) NOT NULL,
  l_shipmode CHAR(10) NOT NULL,
  l_comment VARCHAR(44) NOT NULL,
  PRIMARY KEY (l_orderkey, l_linenumber)
);"
}
