use clap::Parser;
use floe_node_core::tail_client::{TailConfig, build_tail_sql, run};

#[derive(Parser, Debug)]
#[command(name = "floe-tail", about = "Stream TAIL results over pgwire")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 6432)]
    port: u16,
    #[arg(long, default_value = "postgres")]
    user: String,
    #[arg(long, default_value = "postgres")]
    database: String,
    #[arg(long, required_unless_present = "sql", conflicts_with = "sql")]
    mv: Option<String>,
    #[arg(long, required_unless_present = "mv", conflicts_with = "mv")]
    sql: Option<String>,
    #[arg(long)]
    with_snapshot: bool,
    #[arg(long)]
    as_of: Option<i64>,
    #[arg(long)]
    max_rows: Option<usize>,
    #[arg(long)]
    no_header: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let sql = match args.sql.as_ref() {
        Some(sql) => sql.to_string(),
        None => {
            let mv = args
                .mv
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("--mv is required when --sql is not set"))?;
            build_tail_sql(mv, args.with_snapshot, args.as_of)
        }
    };

    let config = TailConfig {
        host: args.host,
        port: args.port,
        user: args.user,
        database: args.database,
        sql,
        max_rows: args.max_rows,
        no_header: args.no_header,
    };

    run(config)
}
