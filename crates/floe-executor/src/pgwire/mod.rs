use std::collections::{HashMap, HashSet};
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::str;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use bytes::{BufMut, BytesMut};
use chrono::{DateTime, FixedOffset};
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::scalar::ScalarValue;
use futures::StreamExt;
use postgres_protocol::message::backend::READY_FOR_QUERY_TAG;
use postgres_types::Type;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::dbsp_bridge::DbspBridge;
use crate::load_or_register_mv;
use crate::{FloeQueryContext, MaterializedViewRegistry, namespaces};

pub mod encode;
pub mod tail;
use encode::encode_tail_batch;
use tail::{
    TailBatch, TailStream, execute_tail, is_tail_canceled_error, parse_tail_sql, tail_output_schema,
};

const SERVER_VERSION: &str = "16.0 (Floe)";
const CLIENT_ENCODING: &str = "UTF8";
const DATE_STYLE: &str = "ISO, MDY";
const TIME_ZONE: &str = "UTC";
const SUPPORTED_PROTOCOL_MAJOR: i32 = 3;
const SSL_REQUEST_CODE: i32 = 80_877_103;
const CANCEL_REQUEST_CODE: i32 = 80_877_102;

const INT8_OID: u32 = 20;
const FLOAT8_OID: u32 = 701;
const TEXT_OID: u32 = 25;
const BOOL_OID: u32 = 16;
const TIMESTAMPTZ_OID: u32 = 1184;
const DEFAULT_DATA_ROW_FLUSH_LIMIT: usize = 1024;
const MV_VERSION_COLUMN: &str = "__mv_version";

static PROCESS_ID_ALLOC: AtomicU32 = AtomicU32::new(10_000);
static SECRET_KEY_ALLOC: AtomicU32 = AtomicU32::new(90_000);

#[derive(Clone)]
pub struct PgwireServerConfig {
    data_row_flush_limit: usize,
}

impl PgwireServerConfig {
    pub fn with_data_row_flush_limit(mut self, limit: usize) -> Self {
        let limit = limit.max(1);
        self.data_row_flush_limit = limit;
        self
    }

    pub fn data_row_flush_limit(&self) -> usize {
        self.data_row_flush_limit
    }
}

impl Default for PgwireServerConfig {
    fn default() -> Self {
        Self {
            data_row_flush_limit: DEFAULT_DATA_ROW_FLUSH_LIMIT,
        }
    }
}

pub struct QueryResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

pub struct PgwireServer {
    ctx: FloeQueryContext,
    registry: Arc<MaterializedViewRegistry>,
    bridge: Arc<Mutex<DbspBridge>>,
    config: PgwireServerConfig,
}

impl PgwireServer {
    pub fn new(
        ctx: FloeQueryContext,
        registry: Arc<MaterializedViewRegistry>,
        bridge: DbspBridge,
    ) -> Self {
        Self::with_config(ctx, registry, bridge, PgwireServerConfig::default())
    }

    pub fn with_config(
        ctx: FloeQueryContext,
        registry: Arc<MaterializedViewRegistry>,
        bridge: DbspBridge,
        config: PgwireServerConfig,
    ) -> Self {
        Self {
            ctx,
            registry,
            bridge: Arc::new(Mutex::new(bridge)),
            config,
        }
    }

    pub async fn serve(&self, listener: TcpListener) -> Result<()> {
        let (stream, addr) = listener
            .accept()
            .await
            .context("accept pgwire TCP connection")?;
        info!(%addr, "pgwire client connected");
        PgwireConnection::new(
            stream,
            self.ctx.clone(),
            Arc::clone(&self.registry),
            Arc::clone(&self.bridge),
            addr,
            self.config.clone(),
            CancellationToken::new(),
        )
        .serve()
        .await
        .with_context(|| format!("serve pgwire client {addr}"))?;
        Ok(())
    }

    pub async fn handle_query(&self, sql: &str) -> Result<QueryResult> {
        let mv_names = find_mv_names(sql);
        if !mv_names.is_empty() {
            let session = self.ctx.session();
            let mut bridge = self.bridge.lock().await;
            for mv in mv_names {
                load_or_register_mv(&session, Arc::clone(&self.registry), &mut bridge, &mv)
                    .await
                    .with_context(|| format!("ensure materialized view '{mv}' is registered"))?;
            }
        }

        let df = self
            .ctx
            .session()
            .sql(sql)
            .await
            .context("plan SQL via DataFusion")?;
        let batches = df.collect().await.context("execute query plan")?;
        Ok(to_query_result(batches))
    }
}

enum ConnAction {
    Continue,
    Terminate,
}

fn to_query_result(batches: Vec<RecordBatch>) -> QueryResult {
    let schema = batches
        .get(0)
        .map(|batch| batch.schema())
        .unwrap_or_else(|| Arc::new(Schema::new(Vec::<Field>::new())));
    QueryResult { schema, batches }
}

fn find_mv_names(sql: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut seen = HashSet::new();
    for raw in sql.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '"')) {
        if raw.is_empty() {
            continue;
        }
        if let Some(name) = normalize_identifier(raw) {
            if seen.insert(name.clone()) {
                names.push(name);
            }
        }
    }
    names
}

fn normalize_identifier(raw: &str) -> Option<String> {
    let quoted = raw.starts_with('"') && raw.ends_with('"') && raw.len() >= 2;
    let inner = if quoted { &raw[1..raw.len() - 1] } else { raw };
    if inner.is_empty() {
        return None;
    }
    let normalized = if quoted {
        inner.to_string()
    } else {
        inner.to_ascii_lowercase()
    };
    if normalized.starts_with("mv_") && namespaces::materialized_view(&normalized).is_ok() {
        Some(normalized)
    } else {
        None
    }
}

fn is_tail_statement(sql: &str) -> bool {
    let trimmed = sql.trim_start_matches(|c: char| c.is_ascii_control() || c.is_whitespace());
    if !trimmed
        .get(..4)
        .map_or(false, |prefix| prefix.eq_ignore_ascii_case("TAIL"))
    {
        return false;
    }
    trimmed[4..]
        .chars()
        .next()
        .map_or(true, |ch| ch.is_whitespace())
}

#[derive(Debug)]
struct PreparedStmt {
    sql: String,
    param_types: Vec<u32>,
    inferred_param_types: Vec<u32>,
    result_schema: Option<SchemaRef>,
    schema_includes_mv_version: bool,
}

#[allow(dead_code)]
#[derive(Debug)]
struct Portal {
    stmt_name: String,
    params: Vec<BoundParam>,
    row_limit: Option<i32>,
    param_formats: Vec<i16>,
    result_formats: Vec<i16>,
    tail_state: Option<TailPortalState>,
}

impl Portal {
    fn clear_tail(&mut self) {
        if let Some(state) = self.tail_state.take() {
            state.cancel.cancel();
        }
    }
}

#[derive(Debug)]
struct TailPortalState {
    stream: TailStream,
    schema: SchemaRef,
    current_batch: Option<TailBatchCursor>,
    finished: bool,
    cancel: CancellationToken,
}

#[derive(Debug)]
struct TailBatchCursor {
    batch: TailBatch,
    next_row: usize,
}

enum TailStreamOutcome {
    Suspended { rows: usize },
    Completed { rows: usize },
    Canceled { rows: usize },
}

#[derive(Debug, PartialEq, Clone)]
enum BoundParam {
    Null,
    Int8(i64),
    Float8(f64),
    Text(String),
    Bool(bool),
    Timestamp(i64),
}

struct StartupMessage {
    parameters: HashMap<String, String>,
}

struct ConnState {
    statements: HashMap<String, PreparedStmt>,
    #[allow(dead_code)]
    portals: HashMap<String, Portal>,
    #[allow(dead_code)]
    client_parameters: HashMap<String, String>,
    #[allow(dead_code)]
    process_id: i32,
    #[allow(dead_code)]
    secret_key: i32,
}

impl ConnState {
    fn new(process_id: i32, secret_key: i32, parameters: HashMap<String, String>) -> Self {
        Self {
            statements: HashMap::new(),
            portals: HashMap::new(),
            client_parameters: parameters,
            process_id,
            secret_key,
        }
    }
}

struct PgwireConnection {
    stream: TcpStream,
    ctx: FloeQueryContext,
    registry: Arc<MaterializedViewRegistry>,
    bridge: Arc<Mutex<DbspBridge>>,
    peer_addr: SocketAddr,
    write_buf: BytesMut,
    read_buf: Vec<u8>,
    config: PgwireServerConfig,
    shutdown: CancellationToken,
}

impl PgwireConnection {
    fn new(
        stream: TcpStream,
        ctx: FloeQueryContext,
        registry: Arc<MaterializedViewRegistry>,
        bridge: Arc<Mutex<DbspBridge>>,
        peer_addr: SocketAddr,
        config: PgwireServerConfig,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            stream,
            ctx,
            registry,
            bridge,
            peer_addr,
            write_buf: BytesMut::with_capacity(1024),
            read_buf: Vec::with_capacity(1024),
            config,
            shutdown,
        }
    }

    async fn serve(mut self) -> Result<()> {
        let mut state = self.handle_startup().await?;
        self.connection_loop(&mut state).await?;
        self.shutdown.cancel();
        Ok(())
    }

    async fn handle_startup(&mut self) -> Result<ConnState> {
        let startup = self.read_startup_message().await?;
        let (process_id, secret_key) = generate_backend_key();
        self.send_startup_responses(process_id, secret_key).await?;
        Ok(ConnState::new(process_id, secret_key, startup.parameters))
    }

    async fn connection_loop(&mut self, state: &mut ConnState) -> Result<()> {
        loop {
            let Some((tag, len)) = self.read_message_header().await? else {
                break;
            };
            let body_len = len
                .checked_sub(4)
                .ok_or_else(|| anyhow!("invalid message length {len}"))?
                as usize;
            self.read_buf.resize(body_len, 0);
            if body_len > 0 {
                self.stream
                    .read_exact(&mut self.read_buf)
                    .await
                    .context("read pgwire message body")?;
            }
            let payload = self.read_buf[..body_len].to_vec();
            match self.dispatch_message(tag, &payload, state).await {
                Ok(action) => {
                    if matches!(action, ConnAction::Terminate) {
                        break;
                    }
                }
                Err(err) => {
                    if is_disconnect_error(&err) {
                        info!(%self.peer_addr, "pgwire client disconnected during request");
                        break;
                    }
                    if let Err(send_err) = self.send_error_response(&err).await {
                        if is_disconnect_error(&send_err) {
                            info!(
                                %self.peer_addr,
                                "pgwire client disconnected before error response could be sent"
                            );
                            break;
                        }
                        return Err(send_err);
                    }
                    if let Err(send_err) = self.send_ready_for_query(b'E').await {
                        if is_disconnect_error(&send_err) {
                            info!(
                                %self.peer_addr,
                                "pgwire client disconnected before ReadyForQuery could be sent"
                            );
                            break;
                        }
                        return Err(send_err);
                    }
                }
            }
        }
        Ok(())
    }

    async fn read_startup_message(&mut self) -> Result<StartupMessage> {
        loop {
            let mut len_buf = [0u8; 4];
            if let Err(err) = self
                .stream
                .read_exact(&mut len_buf)
                .await
                .context("read startup packet length")
            {
                if err
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == ErrorKind::UnexpectedEof)
                {
                    bail!("client disconnected before startup finished");
                }
                return Err(err);
            }
            let len = i32::from_be_bytes(len_buf);
            if len < 8 {
                bail!("invalid startup packet length {len}");
            }
            let mut body = vec![0u8; (len - 4) as usize];
            self.stream
                .read_exact(&mut body)
                .await
                .context("read startup packet body")?;
            let protocol = i32::from_be_bytes(body[0..4].try_into().unwrap());
            match protocol {
                SSL_REQUEST_CODE => {
                    self.stream
                        .write_all(b"N")
                        .await
                        .context("respond SSLRequest")?;
                    continue;
                }
                CANCEL_REQUEST_CODE => {
                    bail!("cancel requests are not supported yet");
                }
                _ => {
                    let major = protocol >> 16;
                    if major != SUPPORTED_PROTOCOL_MAJOR {
                        bail!("unsupported protocol version {protocol}");
                    }
                    let params = parse_startup_parameters(&body[4..])?;
                    return Ok(StartupMessage { parameters: params });
                }
            }
        }
    }

    async fn send_startup_responses(&mut self, process_id: i32, secret_key: i32) -> Result<()> {
        self.write_buf.clear();
        push_authentication_ok(&mut self.write_buf);
        push_parameter_status(&mut self.write_buf, "server_version", SERVER_VERSION);
        push_parameter_status(&mut self.write_buf, "client_encoding", CLIENT_ENCODING);
        push_parameter_status(&mut self.write_buf, "DateStyle", DATE_STYLE);
        push_parameter_status(&mut self.write_buf, "TimeZone", TIME_ZONE);
        push_backend_key_data(&mut self.write_buf, process_id, secret_key);
        push_ready_for_query(&mut self.write_buf, b'I');
        self.stream
            .write_all(&self.write_buf)
            .await
            .context("send startup responses")?;
        Ok(())
    }

    async fn send_ready_for_query(&mut self, status: u8) -> Result<()> {
        self.write_buf.clear();
        push_ready_for_query(&mut self.write_buf, status);
        self.stream
            .write_all(&self.write_buf)
            .await
            .context("send ReadyForQuery response")?;
        Ok(())
    }

    async fn send_error_response(&mut self, err: &anyhow::Error) -> Result<()> {
        self.write_buf.clear();
        let message = err.root_cause().to_string();
        let detail = err.to_string();
        push_error_response(
            &mut self.write_buf,
            "ERROR",
            "XX000",
            &message,
            detail.as_str(),
        );
        self.stream
            .write_all(&self.write_buf)
            .await
            .context("send ErrorResponse")?;
        Ok(())
    }

    async fn send_query_canceled(&mut self) -> Result<()> {
        self.write_buf.clear();
        push_error_response(
            &mut self.write_buf,
            "ERROR",
            "57014",
            "canceling statement due to user request",
            "",
        );
        self.stream
            .write_all(&self.write_buf)
            .await
            .context("send query canceled")?;
        Ok(())
    }

    async fn write_command_complete(&mut self, rows: usize) -> Result<()> {
        self.write_buf.clear();
        push_command_complete(&mut self.write_buf, rows);
        self.stream
            .write_all(&self.write_buf)
            .await
            .context("send CommandComplete")?;
        Ok(())
    }

    async fn flush_row_buffer(&mut self, pending_rows: &mut usize) -> Result<()> {
        if *pending_rows == 0 || self.write_buf.is_empty() {
            return Ok(());
        }
        self.stream
            .write_all(&self.write_buf)
            .await
            .context("send buffered DataRow messages")?;
        self.stream.flush().await.context("flush DataRow buffer")?;
        self.write_buf.clear();
        *pending_rows = 0;
        Ok(())
    }

    async fn read_message_header(&mut self) -> Result<Option<(u8, i32)>> {
        let mut header = [0u8; 5];
        match self.stream.read_exact(&mut header).await {
            Ok(_) => {
                let tag = header[0];
                let len = i32::from_be_bytes(header[1..5].try_into().unwrap());
                if len < 4 {
                    bail!("invalid message length {len}");
                }
                Ok(Some((tag, len)))
            }
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => Ok(None),
            Err(err) if err.kind() == ErrorKind::ConnectionReset => Ok(None),
            Err(err) => Err(err).context("read pgwire message header"),
        }
    }

    async fn dispatch_message(
        &mut self,
        tag: u8,
        payload: &[u8],
        state: &mut ConnState,
    ) -> Result<ConnAction> {
        match tag {
            b'X' => Ok(ConnAction::Terminate),
            b'S' => self.handle_sync().await,
            b'P' => {
                self.handle_parse(payload, state).await?;
                Ok(ConnAction::Continue)
            }
            b'B' => {
                self.handle_bind(payload, state).await?;
                Ok(ConnAction::Continue)
            }
            b'D' => {
                self.handle_describe(payload, state).await?;
                Ok(ConnAction::Continue)
            }
            b'E' => {
                self.handle_execute(payload, state).await?;
                Ok(ConnAction::Continue)
            }
            b'H' => {
                self.handle_flush().await?;
                Ok(ConnAction::Continue)
            }
            b'C' => {
                self.handle_close(payload, state).await?;
                Ok(ConnAction::Continue)
            }
            _ => Ok(ConnAction::Continue),
        }
    }

    async fn handle_parse(&mut self, payload: &[u8], state: &mut ConnState) -> Result<()> {
        let mut cursor = MessageCursor::new(payload);
        let name = cursor
            .read_cstr_owned()
            .context("parse message missing statement name")?;
        let sql = cursor
            .read_cstr_owned()
            .context("parse message missing SQL text")?;
        let param_count = cursor
            .read_i16()
            .context("parse message missing parameter count")?;
        if param_count < 0 {
            bail!("invalid parameter count {param_count}");
        }
        let mut param_types = Vec::with_capacity(param_count as usize);
        for _ in 0..param_count {
            let oid = cursor
                .read_u32()
                .context("parse message truncated parameter type list")?;
            param_types.push(oid);
        }
        state.statements.insert(
            name,
            PreparedStmt {
                sql,
                param_types,
                inferred_param_types: Vec::new(),
                result_schema: None,
                schema_includes_mv_version: false,
            },
        );
        self.write_buf.clear();
        push_parse_complete(&mut self.write_buf);
        self.stream
            .write_all(&self.write_buf)
            .await
            .context("send ParseComplete response")?;
        Ok(())
    }

    async fn handle_sync(&mut self) -> Result<ConnAction> {
        self.send_ready_for_query(b'I').await?;
        Ok(ConnAction::Continue)
    }

    async fn handle_flush(&mut self) -> Result<()> {
        self.stream.flush().await.context("flush pgwire stream")?;
        Ok(())
    }

    async fn handle_close(&mut self, payload: &[u8], state: &mut ConnState) -> Result<()> {
        let mut cursor = MessageCursor::new(payload);
        let variant = cursor.read_u8().context("close message missing variant")?;
        let name = cursor
            .read_cstr_owned()
            .context("close message missing name")?;
        match variant {
            b'S' => {
                state.statements.remove(&name);
            }
            b'P' => {
                if let Some(mut portal) = state.portals.remove(&name) {
                    portal.clear_tail();
                }
            }
            other => bail!("unsupported Close variant {other:?}"),
        }
        self.write_buf.clear();
        push_close_complete(&mut self.write_buf);
        self.stream
            .write_all(&self.write_buf)
            .await
            .context("send CloseComplete")?;
        Ok(())
    }

    async fn handle_bind(&mut self, payload: &[u8], state: &mut ConnState) -> Result<()> {
        let mut cursor = MessageCursor::new(payload);
        let portal_name = cursor
            .read_cstr_owned()
            .context("bind message missing portal name")?;
        let stmt_name = cursor
            .read_cstr_owned()
            .context("bind message missing statement name")?;
        let stmt = state
            .statements
            .get_mut(&stmt_name)
            .ok_or_else(|| anyhow!("statement '{stmt_name}' not found"))?;
        let format_count = cursor
            .read_i16()
            .context("bind message missing format code count")?;
        if format_count < 0 {
            bail!("invalid format code count {format_count}");
        }
        let mut param_formats = if format_count == 0 {
            vec![0]
        } else {
            let mut fmts = Vec::with_capacity(format_count as usize);
            for _ in 0..format_count {
                fmts.push(
                    cursor
                        .read_i16()
                        .context("bind message truncated format codes")?,
                );
            }
            fmts
        };
        if param_formats.is_empty() {
            param_formats.push(0);
        }

        let param_count = cursor
            .read_i16()
            .context("bind message missing parameter count")?;
        if param_count < 0 {
            bail!("invalid parameter count {param_count}");
        }
        let param_count = param_count as usize;
        if stmt.param_types.len() < param_count {
            stmt.param_types.resize(param_count, 0);
        }

        let mut params = Vec::with_capacity(param_count);
        let mut inferred_types = Vec::with_capacity(param_count);
        for idx in 0..param_count {
            let len = cursor
                .read_i32()
                .context("bind message truncated parameter length")?;
            if len < -1 {
                bail!("invalid parameter length {len}");
            }
            if len == -1 {
                params.push(BoundParam::Null);
                let ty = stmt.param_types.get(idx).copied().unwrap_or(0);
                inferred_types.push(if ty == 0 { Type::TEXT.oid() } else { ty });
                continue;
            }
            let len = len as usize;
            let value_bytes = cursor
                .read_bytes(len)
                .context("bind message truncated parameter value")?;
            let format_code = format_code_for_index(&param_formats, idx);
            if format_code != 0 {
                bail!("binary parameter formats are not supported yet");
            }
            let value = str::from_utf8(value_bytes)
                .context("parameter value is not valid UTF-8 in text mode")?;
            let type_oid = stmt.param_types.get(idx).copied().unwrap_or(0);
            let (param, final_type) = convert_text_param(value, type_oid)?;
            params.push(param);
            inferred_types.push(final_type);
        }
        stmt.inferred_param_types = inferred_types;

        let result_format_count = cursor
            .read_i16()
            .context("bind message missing result format count")?;
        if result_format_count < 0 {
            bail!("invalid result format count {result_format_count}");
        }
        let mut result_formats = Vec::with_capacity(result_format_count as usize);
        for _ in 0..result_format_count {
            result_formats.push(
                cursor
                    .read_i16()
                    .context("bind message truncated result formats")?,
            );
        }

        let portal = Portal {
            stmt_name: stmt_name.clone(),
            params,
            row_limit: None,
            param_formats,
            result_formats,
            tail_state: None,
        };
        state.portals.insert(portal_name, portal);

        self.write_buf.clear();
        push_bind_complete(&mut self.write_buf);
        self.stream
            .write_all(&self.write_buf)
            .await
            .context("send BindComplete response")?;
        Ok(())
    }

    async fn handle_describe(&mut self, payload: &[u8], state: &mut ConnState) -> Result<()> {
        let mut cursor = MessageCursor::new(payload);
        let variant = cursor
            .read_u8()
            .context("describe message missing variant")?;
        let name = cursor
            .read_cstr_owned()
            .context("describe message missing object name")?;
        match variant {
            b'S' => {
                let stmt = state
                    .statements
                    .get_mut(&name)
                    .ok_or_else(|| anyhow!("statement '{name}' not found"))?;
                let placeholder_count = count_placeholders(&stmt.sql);
                if stmt.param_types.len() < placeholder_count {
                    stmt.param_types.resize(placeholder_count, 0);
                }
                self.write_buf.clear();
                push_parameter_description(&mut self.write_buf, &stmt.param_types);
                if is_tail_statement(&stmt.sql) {
                    let tail_params = parse_tail_sql(&stmt.sql)?;
                    let schema = tail_output_schema(self.registry.as_ref(), &tail_params.mv_name)?;
                    push_row_description(&mut self.write_buf, &schema);
                } else {
                    self.ensure_statement_schema(stmt).await?;
                    if let Some(schema) = &stmt.result_schema {
                        let has_mv = !find_mv_names(&stmt.sql).is_empty();
                        let schema = if has_mv {
                            maybe_append_mv_version(schema, stmt.schema_includes_mv_version)
                        } else {
                            Arc::clone(schema)
                        };
                        push_row_description(&mut self.write_buf, &schema);
                    } else {
                        push_no_data(&mut self.write_buf);
                    }
                }
                self.stream
                    .write_all(&self.write_buf)
                    .await
                    .context("send Describe response")?;
            }
            b'P' => {
                let (stmt_name, portal_schema) = {
                    let portal = state
                        .portals
                        .get(&name)
                        .ok_or_else(|| anyhow!("portal '{name}' not found"))?;
                    (
                        portal.stmt_name.clone(),
                        portal.tail_state.as_ref().map(|s| Arc::clone(&s.schema)),
                    )
                };
                let stmt = state.statements.get_mut(&stmt_name).ok_or_else(|| {
                    anyhow!("statement '{stmt_name}' not found for portal '{name}'")
                })?;
                self.write_buf.clear();
                if is_tail_statement(&stmt.sql) {
                    let schema = if let Some(schema) = portal_schema {
                        schema
                    } else {
                        let tail_params = parse_tail_sql(&stmt.sql)?;
                        tail_output_schema(self.registry.as_ref(), &tail_params.mv_name)?
                    };
                    push_row_description(&mut self.write_buf, &schema);
                } else {
                    self.ensure_statement_schema(stmt).await?;
                    let placeholder_count = count_placeholders(&stmt.sql);
                    if stmt.param_types.len() < placeholder_count {
                        stmt.param_types.resize(placeholder_count, 0);
                    }
                    if let Some(schema) = &stmt.result_schema {
                        let has_mv = !find_mv_names(&stmt.sql).is_empty();
                        let schema = if has_mv {
                            maybe_append_mv_version(schema, stmt.schema_includes_mv_version)
                        } else {
                            Arc::clone(schema)
                        };
                        push_row_description(&mut self.write_buf, &schema);
                    } else {
                        push_no_data(&mut self.write_buf);
                    }
                }
                self.stream
                    .write_all(&self.write_buf)
                    .await
                    .context("send Describe portal response")?;
            }
            other => bail!("unsupported Describe variant {other:?}"),
        }
        Ok(())
    }

    async fn handle_execute(&mut self, payload: &[u8], state: &mut ConnState) -> Result<()> {
        let mut cursor = MessageCursor::new(payload);
        let portal_name = cursor
            .read_cstr_owned()
            .context("execute message missing portal name")?;
        let max_rows = cursor
            .read_i32()
            .context("execute message missing row limit")?;
        let (stmt_name, params) = {
            let portal = state
                .portals
                .get(&portal_name)
                .ok_or_else(|| anyhow!("portal '{portal_name}' not found"))?;
            (portal.stmt_name.clone(), portal.params.clone())
        };
        let placeholder_count = {
            let stmt = state
                .statements
                .get(&stmt_name)
                .ok_or_else(|| anyhow!("statement '{stmt_name}' not found"))?;
            count_placeholders(&stmt.sql)
        };
        if params.len() < placeholder_count {
            let missing = params.len() + 1;
            let err = anyhow!(format!("parameter ${missing} missing"));
            self.send_error_response(&err).await?;
            self.send_ready_for_query(b'E').await?;
            return Ok(());
        }
        {
            let stmt = state
                .statements
                .get_mut(&stmt_name)
                .ok_or_else(|| anyhow!("statement '{stmt_name}' not found"))?;
            if !is_tail_statement(&stmt.sql) {
                self.ensure_statement_schema(stmt).await?;
            }
        }
        let stmt = state
            .statements
            .get(&stmt_name)
            .ok_or_else(|| anyhow!("statement '{stmt_name}' not found"))?;
        let limit = if max_rows <= 0 {
            None
        } else {
            Some(max_rows as usize)
        };
        if is_tail_statement(&stmt.sql) {
            let outcome = {
                let portal = state
                    .portals
                    .get_mut(&portal_name)
                    .ok_or_else(|| anyhow!("portal '{portal_name}' not found"))?;
                self.execute_tail_portal(stmt, portal, limit).await?
            };
            match outcome {
                TailStreamOutcome::Suspended { rows } => {
                    self.write_buf.clear();
                    push_portal_suspended(&mut self.write_buf);
                    self.stream
                        .write_all(&self.write_buf)
                        .await
                        .context("send PortalSuspended")?;
                    self.write_buf.clear();
                    self.send_ready_for_query(b'I').await?;
                    info!(
                        %self.peer_addr,
                        rows,
                        statement = %stmt.sql,
                        "TAIL portal suspended"
                    );
                }
                TailStreamOutcome::Completed { rows } => {
                    if let Some(mut portal) = state.portals.remove(&portal_name) {
                        portal.clear_tail();
                    }
                    self.write_command_complete(rows).await?;
                    self.send_ready_for_query(b'I').await?;
                    info!(
                        %self.peer_addr,
                        rows,
                        statement = %stmt.sql,
                        "TAIL portal completed"
                    );
                }
                TailStreamOutcome::Canceled { rows } => {
                    if let Some(mut portal) = state.portals.remove(&portal_name) {
                        portal.clear_tail();
                    }
                    self.send_query_canceled().await?;
                    self.send_ready_for_query(b'I').await?;
                    info!(
                        %self.peer_addr,
                        rows,
                        statement = %stmt.sql,
                        "TAIL portal canceled"
                    );
                }
            }
            return Ok(());
        }
        let started = Instant::now();
        let rows = self
            .execute_statement(stmt, params.as_slice(), limit)
            .await?;
        let elapsed_ms = started.elapsed().as_millis();
        self.write_command_complete(rows).await?;
        self.send_ready_for_query(b'I').await?;
        info!(
            %self.peer_addr,
            statement = %stmt_name,
            rows,
            elapsed_ms,
            "pgwire execute completed"
        );
        Ok(())
    }

    async fn execute_statement(
        &mut self,
        stmt: &PreparedStmt,
        params: &[BoundParam],
        row_limit: Option<usize>,
    ) -> Result<usize> {
        self.ensure_materialized_views(&stmt.sql).await?;
        let rewritten = rewrite_sql_with_params(&stmt.sql, params)?;
        let mv_names = find_mv_names(&stmt.sql);
        let has_mv = !mv_names.is_empty();
        let append_version_column = has_mv && !stmt.schema_includes_mv_version;
        let version_scalar = if append_version_column {
            determine_mv_version(&rewritten, &mv_names, self.registry.as_ref())
                .map(|value| ScalarValue::UInt64(Some(value)))
        } else {
            None
        };
        let base_schema = stmt
            .result_schema
            .as_ref()
            .ok_or_else(|| anyhow!("statement missing result schema after planning"))?;
        let output_schema = if append_version_column {
            maybe_append_mv_version(base_schema, false)
        } else {
            Arc::clone(base_schema)
        };
        let df = self
            .ctx
            .session()
            .sql(&rewritten)
            .await
            .context("plan SQL for Execute")?;
        let mut stream = df.execute_stream().await.context("execute query stream")?;
        self.write_buf.clear();
        push_row_description(&mut self.write_buf, &output_schema);
        self.stream
            .write_all(&self.write_buf)
            .await
            .context("send RowDescription before Execute")?;
        self.write_buf.clear();
        let mut total_rows = 0usize;
        let mut buffered_rows = 0usize;
        let flush_limit = self.config.data_row_flush_limit();
        'batch_loop: while let Some(batch) = stream.next().await {
            let batch = batch.context("fetch record batch")?;
            let num_rows = batch.num_rows();
            for row_idx in 0..num_rows {
                push_data_row(
                    &mut self.write_buf,
                    &batch,
                    row_idx,
                    version_scalar.as_ref(),
                )
                .context("encode DataRow")?;
                buffered_rows += 1;
                total_rows += 1;
                if buffered_rows >= flush_limit {
                    self.flush_row_buffer(&mut buffered_rows).await?;
                }
                if let Some(limit) = row_limit {
                    if total_rows >= limit {
                        break 'batch_loop;
                    }
                }
            }
        }
        self.flush_row_buffer(&mut buffered_rows).await?;
        Ok(total_rows)
    }

    async fn execute_tail_portal(
        &mut self,
        stmt: &PreparedStmt,
        portal: &mut Portal,
        row_limit: Option<usize>,
    ) -> Result<TailStreamOutcome> {
        if !portal.params.is_empty() {
            bail!("TAIL does not accept bound parameters");
        }
        if portal.tail_state.is_none() {
            let params = parse_tail_sql(&stmt.sql)?;
            let cancel = self.shutdown.child_token();
            let stream = execute_tail(
                &self.ctx.session(),
                self.registry.as_ref(),
                params,
                cancel.clone(),
            )
            .await?;
            let schema = stream.schema();
            portal.tail_state = Some(TailPortalState {
                stream,
                schema,
                current_batch: None,
                finished: false,
                cancel,
            });
        }

        let state = portal.tail_state.as_mut().expect("tail state initialized");
        self.write_buf.clear();
        push_row_description(&mut self.write_buf, &state.schema);
        self.stream
            .write_all(&self.write_buf)
            .await
            .context("send tail RowDescription")?;
        self.write_buf.clear();

        self.stream_tail_rows(state, row_limit).await
    }

    async fn stream_tail_rows(
        &mut self,
        state: &mut TailPortalState,
        row_limit: Option<usize>,
    ) -> Result<TailStreamOutcome> {
        let mut rows_sent = 0usize;
        let mut buffered_rows = 0usize;
        loop {
            if let Some(limit) = row_limit {
                if rows_sent >= limit {
                    self.flush_row_buffer(&mut buffered_rows).await?;
                    return Ok(TailStreamOutcome::Suspended { rows: rows_sent });
                }
            }
            if state.finished && state.current_batch.is_none() {
                self.flush_row_buffer(&mut buffered_rows).await?;
                return Ok(TailStreamOutcome::Completed { rows: rows_sent });
            }
            if state.current_batch.is_none() {
                match state.stream.next().await {
                    Some(Ok(batch)) => {
                        state.current_batch = Some(TailBatchCursor { batch, next_row: 0 });
                    }
                    Some(Err(err)) => {
                        self.flush_row_buffer(&mut buffered_rows).await?;
                        if is_tail_canceled_error(&err) {
                            return Ok(TailStreamOutcome::Canceled { rows: rows_sent });
                        }
                        return Err(err);
                    }
                    None => {
                        state.finished = true;
                        continue;
                    }
                }
            }
            let cursor = state
                .current_batch
                .as_mut()
                .expect("tail batch cursor must exist");
            let total_rows = cursor.batch.batch.num_rows();
            if cursor.next_row >= total_rows {
                state.current_batch = None;
                continue;
            }
            let available = total_rows - cursor.next_row;
            let allowed = match row_limit {
                Some(limit) => limit.saturating_sub(rows_sent).min(available),
                None => available,
            };
            if allowed == 0 {
                self.flush_row_buffer(&mut buffered_rows).await?;
                return Ok(TailStreamOutcome::Suspended { rows: rows_sent });
            }
            let slice = cursor.batch.batch.slice(cursor.next_row, allowed);
            encode_tail_batch(&mut self.write_buf, &slice, cursor.batch.version)?;
            buffered_rows += allowed;
            rows_sent += allowed;
            cursor.next_row += allowed;
            if cursor.next_row >= total_rows {
                state.current_batch = None;
            }
            if buffered_rows >= self.config.data_row_flush_limit() {
                self.flush_row_buffer(&mut buffered_rows).await?;
            }
        }
    }

    async fn ensure_statement_schema(&mut self, stmt: &mut PreparedStmt) -> Result<()> {
        if stmt.result_schema.is_some() {
            return Ok(());
        }
        let schema = self.plan_statement_schema(&stmt.sql).await?;
        stmt.schema_includes_mv_version = schema
            .fields()
            .iter()
            .any(|field| field.name() == MV_VERSION_COLUMN);
        stmt.result_schema = Some(schema);
        Ok(())
    }

    async fn plan_statement_schema(&self, sql: &str) -> Result<SchemaRef> {
        self.ensure_materialized_views(sql).await?;
        let df = self
            .ctx
            .session()
            .sql(sql)
            .await
            .context("plan SQL for Describe")?;
        Ok(Arc::clone(df.schema().inner()))
    }

    async fn ensure_materialized_views(&self, sql: &str) -> Result<()> {
        let mv_names = find_mv_names(sql);
        if mv_names.is_empty() {
            return Ok(());
        }
        let session = self.ctx.session();
        let mut bridge = self.bridge.lock().await;
        for mv in mv_names {
            load_or_register_mv(&session, Arc::clone(&self.registry), &mut bridge, &mv)
                .await
                .with_context(|| format!("ensure materialized view '{mv}' is registered"))?;
            info!(%self.peer_addr, mv = %mv, "materialized view registered for pgwire session");
        }
        Ok(())
    }
}

fn push_authentication_ok(buf: &mut BytesMut) {
    buf.put_u8(b'R');
    buf.put_i32(8);
    buf.put_i32(0);
}

fn push_parameter_status(buf: &mut BytesMut, name: &str, value: &str) {
    buf.put_u8(b'S');
    let len = 4 + name.len() + 1 + value.len() + 1;
    let len = i32::try_from(len).expect("parameter status too large");
    buf.put_i32(len);
    put_cstr(buf, name);
    put_cstr(buf, value);
}

fn push_backend_key_data(buf: &mut BytesMut, process_id: i32, secret_key: i32) {
    buf.put_u8(b'K');
    buf.put_i32(12);
    buf.put_i32(process_id);
    buf.put_i32(secret_key);
}

fn push_ready_for_query(buf: &mut BytesMut, status: u8) {
    buf.put_u8(READY_FOR_QUERY_TAG);
    buf.put_i32(5);
    buf.put_u8(status);
}

fn push_bind_complete(buf: &mut BytesMut) {
    buf.put_u8(b'2');
    buf.put_i32(4);
}

fn push_close_complete(buf: &mut BytesMut) {
    buf.put_u8(b'3');
    buf.put_i32(4);
}

fn push_command_complete(buf: &mut BytesMut, rows: usize) {
    buf.put_u8(b'C');
    let tag = format!("SELECT {rows}");
    let len = 4 + tag.len() + 1;
    buf.put_i32(len as i32);
    put_cstr(buf, &tag);
}

fn is_disconnect_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_err| {
                matches!(
                    io_err.kind(),
                    ErrorKind::BrokenPipe
                        | ErrorKind::ConnectionReset
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::NotConnected
                        | ErrorKind::UnexpectedEof
                )
            })
    })
}

fn push_parse_complete(buf: &mut BytesMut) {
    buf.put_u8(b'1');
    buf.put_i32(4);
}

fn push_parameter_description(buf: &mut BytesMut, param_types: &[u32]) {
    buf.put_u8(b't');
    let len = 4 + 2 + (param_types.len() * 4);
    buf.put_i32(len as i32);
    buf.put_i16(i16::try_from(param_types.len()).expect("parameter count overflow"));
    for oid in param_types {
        let oid = if *oid == 0 { TEXT_OID } else { *oid };
        buf.put_u32(oid);
    }
}

fn maybe_append_mv_version(schema: &SchemaRef, schema_includes_mv: bool) -> SchemaRef {
    if schema_includes_mv {
        return Arc::clone(schema);
    }
    let mut fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| (**field).clone())
        .collect();
    fields.push(Field::new(MV_VERSION_COLUMN, DataType::UInt64, false));
    Arc::new(Schema::new(fields))
}

fn push_row_description(buf: &mut BytesMut, schema: &SchemaRef) {
    let len_pos = start_message(buf, b'T');
    buf.put_i16(i16::try_from(schema.fields().len()).expect("row description field overflow"));
    for field in schema.fields() {
        put_cstr(buf, field.name());
        buf.put_u32(0);
        buf.put_i16(0);
        let (type_oid, type_len) = pg_type_from_arrow(field.data_type());
        buf.put_u32(type_oid);
        buf.put_i16(type_len);
        buf.put_i32(-1);
        buf.put_i16(0);
    }
    finish_message(buf, len_pos);
}

fn push_no_data(buf: &mut BytesMut) {
    buf.put_u8(b'n');
    buf.put_i32(4);
}

fn push_portal_suspended(buf: &mut BytesMut) {
    buf.put_u8(b's');
    buf.put_i32(4);
}

fn push_data_row(
    buf: &mut BytesMut,
    batch: &RecordBatch,
    row: usize,
    extra_value: Option<&ScalarValue>,
) -> Result<()> {
    let len_pos = start_message(buf, b'D');
    let column_count = batch.num_columns() + extra_value.is_some() as usize;
    buf.put_i16(i16::try_from(column_count).expect("data row column count overflow"));
    for column in batch.columns() {
        if column.is_null(row) {
            buf.put_i32(-1);
            continue;
        }
        let value =
            array_value_to_string(column.as_ref(), row).context("format arrow value for output")?;
        let bytes = value.as_bytes();
        buf.put_i32(i32::try_from(bytes.len()).expect("value too large to encode"));
        buf.extend_from_slice(bytes);
    }
    if let Some(extra) = extra_value {
        if extra.is_null() {
            buf.put_i32(-1);
        } else {
            let text = scalar_value_to_text(extra)?;
            let bytes = text.as_bytes();
            buf.put_i32(i32::try_from(bytes.len()).expect("value too large to encode"));
            buf.extend_from_slice(bytes);
        }
    }
    finish_message(buf, len_pos);
    Ok(())
}

fn scalar_value_to_text(value: &ScalarValue) -> Result<String> {
    match value {
        ScalarValue::UInt64(Some(v)) => Ok(v.to_string()),
        ScalarValue::UInt64(None) => Ok(String::new()),
        _ => bail!("unsupported extra scalar value type for pgwire output: {value:?}"),
    }
}

fn count_placeholders(sql: &str) -> usize {
    let bytes = sql.as_bytes();
    let mut max_idx = 0usize;
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j > i + 1 {
                if let Ok(idx) = sql[i + 1..j].parse::<usize>() {
                    max_idx = max_idx.max(idx);
                }
            }
            i = j;
        } else {
            i += 1;
        }
    }
    max_idx
}

fn determine_mv_version(
    sql: &str,
    mv_names: &[String],
    registry: &MaterializedViewRegistry,
) -> Option<u64> {
    if mv_names.len() != 1 {
        return None;
    }
    if let Some(value) = parse_mv_version_literal(sql) {
        return Some(value);
    }
    registry
        .get(&mv_names[0])
        .and_then(|handle| handle.dbsp_state())
        .map(|state| state.version())
}

fn parse_mv_version_literal(sql: &str) -> Option<u64> {
    let normalized = sql.replace("\"__mv_version\"", MV_VERSION_COLUMN);
    let lower = normalized.to_ascii_lowercase();
    let target = MV_VERSION_COLUMN;
    let bytes = lower.as_bytes();
    let mut offset = 0;
    while offset < lower.len() {
        let slice = &lower[offset..];
        let Some(pos) = slice.find(target) else {
            break;
        };
        let mut idx = offset + pos + target.len();
        while idx < lower.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        if idx >= lower.len() || bytes[idx] != b'=' {
            offset = idx;
            continue;
        }
        idx += 1;
        while idx < lower.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        let start = idx;
        while idx < lower.len() && bytes[idx].is_ascii_digit() {
            idx += 1;
        }
        if start == idx {
            offset = idx;
            continue;
        }
        let literal = &lower[start..idx];
        if let Ok(value) = literal.parse::<u64>() {
            return Some(value);
        }
        offset = idx;
    }
    None
}

fn push_error_response(
    buf: &mut BytesMut,
    severity: &str,
    code: &str,
    message: &str,
    detail: &str,
) {
    let len_pos = start_message(buf, b'E');
    put_error_field(buf, b'S', severity);
    put_error_field(buf, b'C', code);
    put_error_field(buf, b'M', message);
    if !detail.is_empty() && detail != message {
        put_error_field(buf, b'D', detail);
    }
    buf.put_u8(0);
    finish_message(buf, len_pos);
}

fn put_cstr(buf: &mut BytesMut, value: &str) {
    buf.extend_from_slice(value.as_bytes());
    buf.put_u8(0);
}

fn put_error_field(buf: &mut BytesMut, code: u8, value: &str) {
    buf.put_u8(code);
    put_cstr(buf, value);
}

fn parse_startup_parameters(bytes: &[u8]) -> Result<HashMap<String, String>> {
    let mut params = HashMap::new();
    let mut parts = bytes.split(|b| *b == 0);
    loop {
        let Some(key_buf) = parts.next() else {
            break;
        };
        if key_buf.is_empty() {
            break;
        }
        let value_buf = parts.next().unwrap_or(&[]);
        let key = str::from_utf8(key_buf)
            .context("startup parameter key is not valid UTF-8")?
            .to_string();
        let value = str::from_utf8(value_buf)
            .context("startup parameter value is not valid UTF-8")?
            .to_string();
        params.insert(key, value);
    }
    Ok(params)
}

fn generate_backend_key() -> (i32, i32) {
    let process_id = (PROCESS_ID_ALLOC.fetch_add(1, Ordering::Relaxed) & 0x7FFF_FFFF) as i32;
    let secret_key = (SECRET_KEY_ALLOC.fetch_add(1, Ordering::Relaxed) & 0x7FFF_FFFF) as i32;
    (process_id, secret_key)
}

fn format_code_for_index(formats: &[i16], idx: usize) -> i16 {
    if formats.is_empty() {
        0
    } else if formats.len() == 1 {
        formats[0]
    } else {
        *formats.get(idx).or_else(|| formats.last()).unwrap_or(&0)
    }
}

fn convert_text_param(value: &str, type_oid: u32) -> Result<(BoundParam, u32)> {
    if type_oid == 0 {
        return Ok(infer_bound_param(value));
    }
    let trimmed = value.trim();
    let param = match type_oid {
        oid if matches!(oid, _ if oid == Type::INT8.oid() || oid == Type::INT4.oid() || oid == Type::INT2.oid()) =>
        {
            let parsed = trimmed
                .parse::<i64>()
                .with_context(|| format!("parameter '{value}' is not a valid integer"))?;
            BoundParam::Int8(parsed)
        }
        oid if matches!(oid, _ if oid == Type::FLOAT8.oid() || oid == Type::FLOAT4.oid()) => {
            let parsed = trimmed
                .parse::<f64>()
                .with_context(|| format!("parameter '{value}' is not a valid float"))?;
            BoundParam::Float8(parsed)
        }
        oid if oid == Type::BOOL.oid() => {
            let parsed = parse_bool_literal(trimmed)
                .ok_or_else(|| anyhow!("parameter '{value}' is not a valid boolean"))?;
            BoundParam::Bool(parsed)
        }
        oid if oid == Type::TIMESTAMPTZ.oid() || oid == Type::TIMESTAMP.oid() => {
            let millis = parse_timestamp_millis(trimmed)
                .ok_or_else(|| anyhow!("parameter '{value}' is not a valid timestamp"))?;
            BoundParam::Timestamp(millis)
        }
        oid if oid == Type::NUMERIC.oid() => {
            let parsed = trimmed
                .parse::<f64>()
                .with_context(|| format!("parameter '{value}' is not a valid numeric"))?;
            BoundParam::Float8(parsed)
        }
        oid if oid == Type::VARCHAR.oid()
            || oid == Type::BPCHAR.oid()
            || oid == Type::TEXT.oid()
            || oid == Type::NAME.oid()
            || oid == Type::UNKNOWN.oid() =>
        {
            BoundParam::Text(value.to_string())
        }
        _ => BoundParam::Text(value.to_string()),
    };
    Ok((param, type_oid))
}

fn infer_bound_param(value: &str) -> (BoundParam, u32) {
    let trimmed = value.trim();
    if is_integer_literal(trimmed) {
        if let Ok(parsed) = trimmed.parse::<i64>() {
            return (BoundParam::Int8(parsed), Type::INT8.oid());
        }
    }
    if is_float_literal(trimmed) {
        if let Ok(parsed) = trimmed.parse::<f64>() {
            return (BoundParam::Float8(parsed), Type::FLOAT8.oid());
        }
    }
    if let Some(parsed) = parse_bool_literal(trimmed) {
        return (BoundParam::Bool(parsed), Type::BOOL.oid());
    }
    if let Some(millis) = parse_timestamp_millis(trimmed) {
        return (BoundParam::Timestamp(millis), Type::TIMESTAMPTZ.oid());
    }
    (BoundParam::Text(value.to_string()), Type::TEXT.oid())
}

fn is_integer_literal(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }
    let bytes = value.as_bytes();
    let mut idx = 0;
    if matches!(bytes[0], b'+' | b'-') {
        if bytes.len() == 1 {
            return false;
        }
        idx += 1;
    }
    bytes[idx..].iter().all(|b| b.is_ascii_digit())
}

fn is_float_literal(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }
    if !(value.contains('.') || value.contains('e') || value.contains('E')) {
        return false;
    }
    value.parse::<f64>().is_ok()
}

fn parse_bool_literal(value: &str) -> Option<bool> {
    if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("t") {
        Some(true)
    } else if value.eq_ignore_ascii_case("false") || value.eq_ignore_ascii_case("f") {
        Some(false)
    } else {
        None
    }
}

fn parse_timestamp_millis(value: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt: DateTime<FixedOffset>| dt.timestamp_millis())
}

fn rewrite_sql_with_params(sql: &str, params: &[BoundParam]) -> Result<String> {
    if params.is_empty() {
        return Ok(sql.to_string());
    }
    let mut output = String::with_capacity(sql.len() + params.len() * 8);
    let bytes = sql.as_bytes();
    let mut i = 0;
    let len = bytes.len();
    let mut in_single_quote = false;
    while i < len {
        let b = bytes[i];
        if b == b'\'' {
            output.push('\'');
            if in_single_quote {
                if i + 1 < len && bytes[i + 1] == b'\'' {
                    output.push('\'');
                    i += 2;
                    continue;
                }
                in_single_quote = false;
            } else {
                in_single_quote = true;
            }
            i += 1;
            continue;
        }
        if in_single_quote {
            output.push(b as char);
            i += 1;
            continue;
        }
        if b == b'$' {
            let mut j = i + 1;
            while j < len && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j == i + 1 {
                output.push('$');
                i += 1;
                continue;
            }
            let idx: usize = sql[i + 1..j]
                .parse()
                .context("invalid parameter placeholder")?;
            if idx == 0 {
                bail!("parameter placeholders start at $1");
            }
            let param = params
                .get(idx - 1)
                .ok_or_else(|| anyhow!("parameter ${idx} missing"))?;
            output.push_str(&encode_literal(param));
            i = j;
            continue;
        }
        output.push(b as char);
        i += 1;
    }
    Ok(output)
}

fn encode_literal(param: &BoundParam) -> String {
    match param {
        BoundParam::Null => "NULL".to_string(),
        BoundParam::Int8(v) => v.to_string(),
        BoundParam::Float8(v) => v.to_string(),
        BoundParam::Text(text) => format!("'{}'", escape_sql_literal(text)),
        BoundParam::Bool(true) => "TRUE".to_string(),
        BoundParam::Bool(false) => "FALSE".to_string(),
        BoundParam::Timestamp(ms) => format!("TO_TIMESTAMP({}/1000.0)", ms),
    }
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn pg_type_from_arrow(data_type: &DataType) -> (u32, i16) {
    match data_type {
        DataType::Int64 => (INT8_OID, 8),
        DataType::Int32 => (INT8_OID, 8),
        DataType::Float64 => (FLOAT8_OID, 8),
        DataType::Boolean => (BOOL_OID, 1),
        DataType::Timestamp(_, _) => (TIMESTAMPTZ_OID, 8),
        DataType::Utf8 | DataType::LargeUtf8 => (TEXT_OID, -1),
        _ => (TEXT_OID, -1),
    }
}

pub(super) fn start_message(buf: &mut BytesMut, tag: u8) -> usize {
    buf.put_u8(tag);
    let len_pos = buf.len();
    buf.put_i32(0);
    len_pos
}

pub(super) fn finish_message(buf: &mut BytesMut, len_pos: usize) {
    let len = (buf.len() - len_pos) as i32;
    buf[len_pos..len_pos + 4].copy_from_slice(&len.to_be_bytes());
}

struct MessageCursor<'a> {
    buf: &'a [u8],
    idx: usize,
}

impl<'a> MessageCursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, idx: 0 }
    }

    fn read_u8(&mut self) -> Result<u8> {
        if self.idx >= self.buf.len() {
            bail!("unexpected end of message");
        }
        let value = self.buf[self.idx];
        self.idx += 1;
        Ok(value)
    }

    fn read_i16(&mut self) -> Result<i16> {
        let bytes = self.read_bytes(2)?;
        Ok(i16::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn read_i32(&mut self) -> Result<i32> {
        let bytes = self.read_bytes(4)?;
        Ok(i32::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn read_cstr_owned(&mut self) -> Result<String> {
        let remaining = &self.buf[self.idx..];
        if let Some(pos) = remaining.iter().position(|b| *b == 0) {
            let start = self.idx;
            let end = start + pos;
            self.idx = end + 1;
            let slice = &self.buf[start..end];
            Ok(str::from_utf8(slice)
                .context("string is not valid UTF-8")?
                .to_string())
        } else {
            bail!("unterminated string in message");
        }
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        if self.idx + len > self.buf.len() {
            bail!("unexpected end of message");
        }
        let slice = &self.buf[self.idx..self.idx + len];
        self.idx += len;
        Ok(slice)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use bytes::BytesMut;
    use datafusion::arrow::array::{ArrayRef, Int64Array};
    use postgres_protocol::message::backend::READY_FOR_QUERY_TAG;

    #[test]
    fn detects_mv_names() {
        let sql = r#"SELECT * FROM mv_orders JOIN "mv_Sales" ON mv_orders.id = "mv_Sales".id"#;
        let mut names = find_mv_names(sql);
        names.sort_by(|a, b| a.to_ascii_lowercase().cmp(&b.to_ascii_lowercase()));
        assert_eq!(names, vec!["mv_orders", "mv_Sales"]);
    }

    #[test]
    fn query_result_wraps_batches() {
        let schema = SchemaRef::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        )
        .expect("batch");
        let result = to_query_result(vec![batch]);
        assert_eq!(result.schema.fields().len(), 1);
        assert_eq!(result.batches.len(), 1);
    }

    #[test]
    fn ready_for_query_encoding_matches_protocol() {
        let mut buf = BytesMut::new();
        push_ready_for_query(&mut buf, b'I');
        assert_eq!(buf.len(), 6);
        assert_eq!(buf[0], READY_FOR_QUERY_TAG);
        assert_eq!(&buf[1..5], &5i32.to_be_bytes());
        assert_eq!(buf[5], b'I');
    }

    #[test]
    fn parses_startup_parameters() {
        let params =
            parse_startup_parameters(b"user\0floe\0database\0postgres\0\0").expect("params");
        assert_eq!(params.get("user").unwrap(), "floe");
        assert_eq!(params.get("database").unwrap(), "postgres");
    }

    #[test]
    fn parameter_description_defaults_to_text() {
        let mut buf = BytesMut::new();
        push_parameter_description(&mut buf, &[0, Type::INT4.oid()]);
        assert_eq!(buf[0], b't');
        let count = i16::from_be_bytes([buf[5], buf[6]]);
        assert_eq!(count, 2);
        let first = u32::from_be_bytes([buf[7], buf[8], buf[9], buf[10]]);
        let second = u32::from_be_bytes([buf[11], buf[12], buf[13], buf[14]]);
        assert_eq!(first, Type::TEXT.oid());
        assert_eq!(second, Type::INT4.oid());
    }

    #[test]
    fn arrow_types_map_to_pg() {
        assert_eq!(pg_type_from_arrow(&DataType::Int64), (Type::INT8.oid(), 8));
        assert_eq!(pg_type_from_arrow(&DataType::Utf8), (Type::TEXT.oid(), -1));
        let ts = DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None);
        assert_eq!(pg_type_from_arrow(&ts), (Type::TIMESTAMPTZ.oid(), 8));
    }

    #[test]
    fn infers_integer_param_from_text() {
        let (param, oid) = infer_bound_param("42");
        assert_eq!(oid, Type::INT8.oid());
        assert_eq!(param, BoundParam::Int8(42));
    }

    #[test]
    fn infers_bool_param_from_text() {
        let (param, oid) = infer_bound_param("TRUE");
        assert_eq!(oid, Type::BOOL.oid());
        assert_eq!(param, BoundParam::Bool(true));
    }

    #[test]
    fn infers_timestamp_param_from_text() {
        let literal = "2024-05-20T12:34:56Z";
        let (param, oid) = infer_bound_param(literal);
        assert_eq!(oid, Type::TIMESTAMPTZ.oid());
        match param {
            BoundParam::Timestamp(ms) => {
                assert_eq!(Some(ms), parse_timestamp_millis(literal));
            }
            other => panic!("expected timestamp, got {other:?}"),
        }
    }

    #[test]
    fn convert_text_param_respects_explicit_type() {
        let (param, oid) = convert_text_param("15", Type::INT8.oid()).expect("param");
        assert_eq!(oid, Type::INT8.oid());
        assert_eq!(param, BoundParam::Int8(15));
    }

    #[test]
    fn rewrite_sql_with_parameters() {
        let sql = "SELECT * FROM mv_orders WHERE id = $1 AND status = $2";
        let params = vec![BoundParam::Int8(42), BoundParam::Text("ready".into())];
        let rewritten = rewrite_sql_with_params(sql, &params).expect("rewrite");
        assert_eq!(
            rewritten,
            "SELECT * FROM mv_orders WHERE id = 42 AND status = 'ready'"
        );
    }

    #[test]
    fn rewrite_sql_escapes_text_literals() {
        let sql = "SELECT $1";
        let params = vec![BoundParam::Text("O'Reilly".into())];
        let rewritten = rewrite_sql_with_params(sql, &params).expect("rewrite");
        assert_eq!(rewritten, "SELECT 'O''Reilly'");
    }
}
