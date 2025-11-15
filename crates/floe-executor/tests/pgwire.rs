use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use bytes::BytesMut;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::scalar::ScalarValue;
use fallible_iterator::FallibleIterator;
use floe_executor::FloeQueryContext;
use floe_executor::PgwireServer;
use floe_executor::dbsp_bridge::DbspBridge;
use floe_executor::encoding::encode_projected_row_key;
use floe_executor::materialized_view::{DbspPersistedState, MaterializedViewRegistry};
use floe_storage::SlateCatalog;
use futures::FutureExt;
use object_store::memory::InMemory;
use postgres_protocol::IsNull;
use postgres_protocol::message::backend::{
    DataRowBody, ErrorResponseBody, Message, ReadyForQueryBody, RowDescriptionBody,
};
use postgres_protocol::message::frontend::{self, BindError};
use slatedb::Db;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};

const VIEW_NAME: &str = "mv_pgwire_test";

#[tokio::test]
async fn pgwire_executes_selects_and_version_filters() -> Result<()> {
    let rows = vec![10, 20];
    let fixture = PgwireFixture::start("pgwire-positive", &rows).await?;

    let mut client = PgwireClient::connect(fixture.addr).await?;
    client
        .parse("all", "SELECT value FROM mv_pgwire_test ORDER BY value")
        .await?;
    client.expect_parse_complete().await?;

    client.describe_statement("all").await?;
    client.expect_parameter_description(0).await?;
    let fields = client.expect_row_description().await?;
    assert_eq!(fields, vec!["value", "__mv_version"]);

    client.bind("portal_all", "all", &[]).await?;
    client.expect_bind_complete().await?;

    client.execute("portal_all", 0).await?;
    let exec_fields = client.expect_row_description().await?;
    assert_eq!(exec_fields, vec!["value", "__mv_version"]);
    let rows_all = client.consume_data_rows().await?;
    assert_eq!(rows_all.len(), 2);
    assert_eq!(rows_all[0][0], Some("10".into()));
    assert_eq!(rows_all[1][0], Some("20".into()));

    // Query with mv_version filter (oldest version should return first row only).
    let version_literal = fixture.versions[0].to_string();
    client
        .parse(
            "by_version",
            "SELECT value FROM mv_pgwire_test WHERE __mv_version = $1 ORDER BY value",
        )
        .await?;
    client.expect_parse_complete().await?;
    client.describe_statement("by_version").await?;
    client.expect_parameter_description(1).await?;
    let fields = client.expect_row_description().await?;
    assert_eq!(fields, vec!["value", "__mv_version"]);
    client
        .bind("portal_version", "by_version", &[&version_literal])
        .await?;
    client.expect_bind_complete().await?;
    client.execute("portal_version", 0).await?;
    let _ = client.expect_row_description().await?;
    let rows_filtered = client.consume_data_rows().await?;
    assert_eq!(rows_filtered.len(), 1);
    assert_eq!(rows_filtered[0][0], Some("10".into()));
    assert_eq!(rows_filtered[0][1], Some(version_literal.clone()));

    client.terminate().await?;
    fixture.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn pgwire_errors_on_unknown_table() -> Result<()> {
    let rows = vec![1];
    let fixture = PgwireFixture::start("pgwire-unknown", &rows).await?;

    let mut client = PgwireClient::connect(fixture.addr).await?;
    client
        .parse("missing", "SELECT value FROM mv_missing_table")
        .await?;
    client.expect_parse_complete().await?;
    client.bind("portal_missing", "missing", &[]).await?;
    client.expect_bind_complete().await?;
    client.execute("portal_missing", 0).await?;

    let error = client.expect_error().await?;
    assert!(
        error
            .get(&'M')
            .is_some_and(|msg| msg.contains("mv_missing_table")),
        "unexpected error fields: {error:?}"
    );
    client.expect_ready_with_status(b'E').await?;

    client.terminate().await?;
    fixture.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn pgwire_errors_on_parameter_mismatch() -> Result<()> {
    let rows = vec![5];
    let fixture = PgwireFixture::start("pgwire-param-mismatch", &rows).await?;

    let mut client = PgwireClient::connect(fixture.addr).await?;
    client
        .parse(
            "needs_param",
            "SELECT value FROM mv_pgwire_test WHERE value = $1",
        )
        .await?;
    client.expect_parse_complete().await?;
    client
        .bind("portal_needs_param", "needs_param", &[])
        .await?;
    client.expect_bind_complete().await?;
    client.execute("portal_needs_param", 0).await?;
    let error = client.expect_error().await?;
    assert!(
        error
            .get(&'M')
            .is_some_and(|msg| msg.contains("parameter $1 missing")),
        "unexpected error fields: {error:?}"
    );
    client.expect_ready_with_status(b'E').await?;

    client.terminate().await?;
    fixture.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn pgwire_handles_client_drop_mid_stream() -> Result<()> {
    let rows: Vec<i64> = (0..5000).collect();
    let fixture = PgwireFixture::start("pgwire-client-drop", &rows).await?;

    let mut client = PgwireClient::connect(fixture.addr).await?;
    client
        .parse("all", "SELECT value FROM mv_pgwire_test ORDER BY value")
        .await?;
    client.expect_parse_complete().await?;
    client.describe_statement("all").await?;
    client.expect_parameter_description(0).await?;
    client.expect_row_description().await?;
    client.bind("portal_all", "all", &[]).await?;
    client.expect_bind_complete().await?;
    client.execute("portal_all", 0).await?;
    client.expect_row_description().await?;
    let first_row = client.read_single_data_row().await?;
    assert_eq!(first_row[0], Some("0".into()));

    drop(client);
    fixture.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn pgwire_streams_rows_incrementally() -> Result<()> {
    let rows: Vec<i64> = (0..2000).collect();
    let fixture = PgwireFixture::start("pgwire-stream-incremental", &rows).await?;

    let mut client = PgwireClient::connect(fixture.addr).await?;
    client
        .parse(
            "stream_all",
            "SELECT value FROM mv_pgwire_test ORDER BY value",
        )
        .await?;
    client.expect_parse_complete().await?;
    client.describe_statement("stream_all").await?;
    client.expect_parameter_description(0).await?;
    client.expect_row_description().await?;
    client.bind("portal_stream", "stream_all", &[]).await?;
    client.expect_bind_complete().await?;
    client.execute("portal_stream", 0).await?;
    client.expect_row_description().await?;

    let first_row = timeout(Duration::from_secs(1), client.read_single_data_row())
        .await
        .context("timeout waiting for first streamed row")??;
    assert_eq!(first_row[0], Some("0".into()));

    let mut streamed_rows = vec![first_row];
    streamed_rows.extend(client.consume_data_rows().await?);
    assert_eq!(streamed_rows.len(), rows.len());
    for (idx, row) in streamed_rows.iter().enumerate() {
        assert_eq!(row[0], Some(rows[idx].to_string()));
    }

    client.terminate().await?;
    fixture.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn pgwire_streaming_respects_version_snapshots() -> Result<()> {
    let rows = vec![5, 15];
    let fixture = PgwireFixture::start("pgwire-stream-versioned", &rows).await?;
    let version_old = fixture.versions[0];
    let version_latest = *fixture.versions.last().unwrap();

    let mut client = PgwireClient::connect(fixture.addr).await?;

    client
        .parse(
            "by_version",
            "SELECT value, __mv_version FROM mv_pgwire_test \
             WHERE __mv_version = $1 ORDER BY value",
        )
        .await?;
    client.expect_parse_complete().await?;
    client.describe_statement("by_version").await?;
    client.expect_parameter_description(1).await?;
    client.expect_row_description().await?;
    client
        .bind("portal_version", "by_version", &[&version_old.to_string()])
        .await?;
    client.expect_bind_complete().await?;
    client.execute("portal_version", 0).await?;
    client.expect_row_description().await?;
    let filtered_rows = client.consume_data_rows().await?;
    assert_eq!(filtered_rows.len(), 1);
    assert_eq!(filtered_rows[0][0], Some("5".into()));
    assert_eq!(
        filtered_rows[0][1],
        Some(version_old.to_string()),
        "expected only rows from requested version"
    );

    client
        .parse(
            "latest",
            "SELECT value, __mv_version FROM mv_pgwire_test ORDER BY value",
        )
        .await?;
    client.expect_parse_complete().await?;
    client.describe_statement("latest").await?;
    client.expect_parameter_description(0).await?;
    client.expect_row_description().await?;
    client.bind("portal_latest", "latest", &[]).await?;
    client.expect_bind_complete().await?;
    client.execute("portal_latest", 0).await?;
    client.expect_row_description().await?;
    let latest_rows = client.consume_data_rows().await?;
    assert_eq!(latest_rows.len(), 2);
    assert_eq!(latest_rows[0][0], Some("5".into()));
    assert_eq!(latest_rows[1][0], Some("15".into()));
    for row in latest_rows {
        assert_eq!(row[1], Some(version_latest.to_string()));
    }

    client.terminate().await?;
    fixture.shutdown().await;
    Ok(())
}

struct PgwireFixture {
    addr: SocketAddr,
    versions: Vec<u64>,
    join: JoinHandle<()>,
}

impl PgwireFixture {
    async fn start(test_name: &str, rows: &[i64]) -> Result<Self> {
        let db = test_db(test_name).await;
        let schema = test_schema();
        let (state, versions) = seed_view_state(Arc::clone(&db), rows, Arc::clone(&schema)).await?;

        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema(VIEW_NAME.to_string(), Arc::clone(&schema));
        registry
            .register(VIEW_NAME.to_string())
            .set_dbsp_state(state);

        let catalog = Arc::new(SlateCatalog::in_memory().await.expect("catalog"));
        let ctx = FloeQueryContext::new(Arc::clone(&catalog));
        let bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let server = Arc::new(PgwireServer::new(ctx, Arc::clone(&registry), bridge));

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server_task = {
            let server = Arc::clone(&server);
            tokio::spawn(async move {
                server.serve(listener).await.expect("pgwire serve");
            })
        };

        Ok(Self {
            addr,
            versions,
            join: server_task,
        })
    }

    async fn shutdown(self) {
        let _ = self.join.map(|res| res.expect("server task")).await;
    }
}

struct PgwireClient {
    stream: TcpStream,
}

impl PgwireClient {
    async fn connect(addr: SocketAddr) -> Result<Self> {
        let mut stream = TcpStream::connect(addr).await?;
        let mut buf = BytesMut::new();
        frontend::startup_message([("user", "test"), ("database", "postgres")], &mut buf)?;
        stream.write_all(&buf).await?;
        stream.flush().await?;
        loop {
            match read_message(&mut stream).await? {
                Message::ReadyForQuery(_) => break,
                _ => continue,
            }
        }
        Ok(Self { stream })
    }

    async fn parse(&mut self, name: &str, sql: &str) -> Result<()> {
        let mut buf = BytesMut::new();
        frontend::parse(name, sql, std::iter::empty(), &mut buf)?;
        self.stream.write_all(&buf).await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn describe_statement(&mut self, name: &str) -> Result<()> {
        let mut buf = BytesMut::new();
        frontend::describe(b'S', name, &mut buf)?;
        self.stream.write_all(&buf).await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn bind(&mut self, portal: &str, statement: &str, params: &[&str]) -> Result<()> {
        let mut buf = BytesMut::new();
        frontend::bind(
            portal,
            statement,
            std::iter::empty::<i16>(),
            params.iter().map(|value| *value),
            |value, out| {
                out.extend_from_slice(value.as_bytes());
                Ok(IsNull::No)
            },
            std::iter::empty::<i16>(),
            &mut buf,
        )
        .map_err(bind_error)?;
        self.stream.write_all(&buf).await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn execute(&mut self, portal: &str, max_rows: i32) -> Result<()> {
        let mut buf = BytesMut::new();
        frontend::execute(portal, max_rows, &mut buf)?;
        self.stream.write_all(&buf).await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn expect_parse_complete(&mut self) -> Result<()> {
        match read_message(&mut self.stream).await? {
            Message::ParseComplete => Ok(()),
            other => bail!("expected ParseComplete, got {}", describe_message(&other)),
        }
    }

    async fn expect_bind_complete(&mut self) -> Result<()> {
        match read_message(&mut self.stream).await? {
            Message::BindComplete => Ok(()),
            other => bail!("expected BindComplete, got {}", describe_message(&other)),
        }
    }

    async fn expect_parameter_description(&mut self, expected: usize) -> Result<()> {
        match read_message(&mut self.stream).await? {
            Message::ParameterDescription(body) => {
                let mut params = body.parameters();
                let mut count = 0usize;
                while let Some(_) = params.next().context("read parameter oid")? {
                    count += 1;
                }
                if count != expected {
                    bail!("expected {expected} parameters, got {count}");
                }
                Ok(())
            }
            other => bail!(
                "expected ParameterDescription, got {}",
                describe_message(&other)
            ),
        }
    }

    async fn expect_row_description(&mut self) -> Result<Vec<String>> {
        match read_message(&mut self.stream).await? {
            Message::RowDescription(body) => extract_field_names(&body),
            other => bail!("expected RowDescription, got {}", describe_message(&other)),
        }
    }

    async fn consume_data_rows(&mut self) -> Result<Vec<Vec<Option<String>>>> {
        let mut rows = Vec::new();
        loop {
            match read_message(&mut self.stream).await? {
                Message::DataRow(body) => rows.push(extract_data_row(&body)?),
                Message::CommandComplete(_) => {}
                Message::ReadyForQuery(body) => {
                    ensure_ready_ok(&body, b'I')?;
                    break;
                }
                other => bail!(
                    "unexpected message while collecting rows: {}",
                    describe_message(&other)
                ),
            }
        }
        Ok(rows)
    }

    async fn read_single_data_row(&mut self) -> Result<Vec<Option<String>>> {
        match read_message(&mut self.stream).await? {
            Message::DataRow(body) => extract_data_row(&body),
            other => bail!("expected DataRow, got {}", describe_message(&other)),
        }
    }

    async fn expect_error(&mut self) -> Result<HashMap<char, String>> {
        match read_message(&mut self.stream).await? {
            Message::ErrorResponse(body) => extract_error_fields(&body),
            other => bail!("expected ErrorResponse, got {}", describe_message(&other)),
        }
    }

    async fn expect_ready_with_status(&mut self, status: u8) -> Result<()> {
        match read_message(&mut self.stream).await? {
            Message::ReadyForQuery(body) => {
                ensure_ready_ok(&body, status)?;
                Ok(())
            }
            other => bail!("expected ReadyForQuery, got {}", describe_message(&other)),
        }
    }

    async fn terminate(mut self) -> Result<()> {
        let mut buf = BytesMut::new();
        frontend::terminate(&mut buf);
        self.stream.write_all(&buf).await?;
        Ok(())
    }
}

fn ensure_ready_ok(body: &ReadyForQueryBody, expected: u8) -> Result<()> {
    if body.status() != expected {
        bail!(
            "expected ReadyForQuery status {}, got {}",
            expected as char,
            body.status() as char
        );
    }
    Ok(())
}

fn bind_error(err: BindError) -> anyhow::Error {
    match err {
        BindError::Conversion(inner) => anyhow!(inner),
        BindError::Serialization(inner) => anyhow!(inner),
    }
}

fn describe_message(message: &Message) -> &'static str {
    match message {
        Message::AuthenticationCleartextPassword => "AuthenticationCleartextPassword",
        Message::AuthenticationGss => "AuthenticationGss",
        Message::AuthenticationKerberosV5 => "AuthenticationKerberosV5",
        Message::AuthenticationMd5Password(_) => "AuthenticationMd5Password",
        Message::AuthenticationOk => "AuthenticationOk",
        Message::AuthenticationScmCredential => "AuthenticationScmCredential",
        Message::AuthenticationSspi => "AuthenticationSspi",
        Message::AuthenticationGssContinue(_) => "AuthenticationGssContinue",
        Message::AuthenticationSasl(_) => "AuthenticationSasl",
        Message::AuthenticationSaslContinue(_) => "AuthenticationSaslContinue",
        Message::AuthenticationSaslFinal(_) => "AuthenticationSaslFinal",
        Message::BackendKeyData(_) => "BackendKeyData",
        Message::BindComplete => "BindComplete",
        Message::CloseComplete => "CloseComplete",
        Message::CommandComplete(_) => "CommandComplete",
        Message::CopyData(_) => "CopyData",
        Message::CopyDone => "CopyDone",
        Message::CopyInResponse(_) => "CopyInResponse",
        Message::CopyOutResponse(_) => "CopyOutResponse",
        Message::DataRow(_) => "DataRow",
        Message::EmptyQueryResponse => "EmptyQueryResponse",
        Message::ErrorResponse(_) => "ErrorResponse",
        Message::NoData => "NoData",
        Message::NoticeResponse(_) => "NoticeResponse",
        Message::NotificationResponse(_) => "NotificationResponse",
        Message::ParameterDescription(_) => "ParameterDescription",
        Message::ParameterStatus(_) => "ParameterStatus",
        Message::ParseComplete => "ParseComplete",
        Message::PortalSuspended => "PortalSuspended",
        Message::ReadyForQuery(_) => "ReadyForQuery",
        Message::RowDescription(_) => "RowDescription",
        _ => "UnknownBackendMessage",
    }
}

async fn read_message(stream: &mut TcpStream) -> Result<Message> {
    let mut header = [0u8; 5];
    stream
        .read_exact(&mut header)
        .await
        .context("read message header")?;
    let len = i32::from_be_bytes(header[1..5].try_into().unwrap());
    let mut buf = BytesMut::from(&header[..]);
    let mut body = vec![0u8; (len - 4) as usize];
    stream
        .read_exact(&mut body)
        .await
        .context("read message body")?;
    buf.extend_from_slice(&body);
    let mut bytes = buf;
    match Message::parse(&mut bytes) {
        Ok(Some(msg)) => Ok(msg),
        Ok(None) => bail!("incomplete backend message"),
        Err(err) => Err(anyhow!(err)),
    }
}

fn extract_field_names(body: &RowDescriptionBody) -> Result<Vec<String>> {
    let mut names = Vec::new();
    let mut fields = body.fields();
    while let Some(field) = fields.next().context("parse row description")? {
        names.push(field.name().to_string());
    }
    Ok(names)
}

fn extract_data_row(body: &DataRowBody) -> Result<Vec<Option<String>>> {
    let mut rows = Vec::new();
    let mut ranges = body.ranges();
    while let Some(range_opt) = ranges.next().context("read data row range")? {
        match range_opt {
            Some(range) => {
                let slice = &body.buffer()[range.clone()];
                let text = std::str::from_utf8(slice).context("decode datarow value as utf8")?;
                rows.push(Some(text.to_string()));
            }
            None => rows.push(None),
        }
    }
    Ok(rows)
}

fn extract_error_fields(body: &ErrorResponseBody) -> Result<HashMap<char, String>> {
    let mut map = HashMap::new();
    let mut fields = body.fields();
    while let Some(field) = fields.next().context("parse error field")? {
        let label = field.type_() as char;
        let value = std::str::from_utf8(field.value_bytes())
            .context("decode error field")?
            .to_string();
        map.insert(label, value);
    }
    Ok(map)
}

async fn seed_view_state(
    db: Arc<Db>,
    rows: &[i64],
    schema: SchemaRef,
) -> Result<(DbspPersistedState, Vec<u64>)> {
    let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
    let mut view = bridge.new_view(VIEW_NAME).await?;
    let mut versions = Vec::new();
    for value in rows {
        let key = encode_projected_row_key(&[ScalarValue::Int64(Some(*value))])?;
        view.add_delta(key, 1);
        let handle = view.flush().await?;
        versions.push(handle.version);
    }
    bridge
        .save_mv_schema(VIEW_NAME, Arc::clone(&schema))
        .await?;
    let handle_view = view.latest_handle_view();
    let (dict, table, namespace, version) = handle_view.into_parts();
    Ok((
        DbspPersistedState::new(dict, table, namespace, version),
        versions,
    ))
}

async fn test_db(name: &str) -> Arc<Db> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    Arc::new(Db::open(name, store).await.expect("open SlateDB"))
}

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}
