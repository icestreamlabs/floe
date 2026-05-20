use anyhow::{Context, Result, bail, ensure};
use pgwire_replication::PgWireError;
use pgwire_replication::auth::ScramClient;
use pgwire_replication::protocol::framing::{
    read_backend_message, write_password_message, write_query, write_startup_message,
};
use pgwire_replication::protocol::{parse_auth_request, parse_error_response};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use crate::config::PostgresCdcConfig;
use crate::lsn::PostgresLsn;

#[derive(Debug)]
pub struct PostgresExportedSlotSnapshot {
    slot_name: String,
    consistent_lsn: PostgresLsn,
    snapshot_name: String,
    output_plugin: String,
    _stream: TcpStream,
}

impl PostgresExportedSlotSnapshot {
    pub fn slot_name(&self) -> &str {
        &self.slot_name
    }

    pub fn consistent_lsn(&self) -> PostgresLsn {
        self.consistent_lsn
    }

    pub fn snapshot_name(&self) -> &str {
        &self.snapshot_name
    }

    pub fn output_plugin(&self) -> &str {
        &self.output_plugin
    }
}

pub async fn create_pgoutput_slot_with_exported_snapshot(
    config: &PostgresCdcConfig,
) -> Result<PostgresExportedSlotSnapshot> {
    validate_replication_slot_name(config.slot())?;
    let mut stream = TcpStream::connect((config.host(), config.port()))
        .await
        .with_context(|| {
            format!(
                "connect Postgres replication control plane at {}:{}",
                config.host(),
                config.port()
            )
        })?;
    stream
        .set_nodelay(true)
        .context("configure Postgres replication control TCP_NODELAY")?;

    let startup_params = [
        ("user", config.user()),
        ("database", config.database()),
        ("replication", "database"),
        ("client_encoding", "UTF8"),
        ("application_name", "floe-cdc-snapshot"),
    ];
    write_startup_message(&mut stream, 196608, &startup_params)
        .await
        .context("send Postgres replication startup message")?;
    authenticate_replication_control_stream(&mut stream, config)
        .await
        .context("authenticate Postgres replication control connection")?;

    let command = format!(
        "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput EXPORT_SNAPSHOT",
        config.slot()
    );
    write_query(&mut stream, &command)
        .await
        .with_context(|| format!("create Postgres pgoutput slot '{}'", config.slot()))?;

    let mut data_row = None;
    loop {
        let message = read_backend_message(&mut stream).await.with_context(|| {
            format!(
                "read Postgres CREATE_REPLICATION_SLOT response for '{}'",
                config.slot()
            )
        })?;
        match message.tag {
            b'D' => data_row = Some(parse_simple_data_row(&message.payload)?),
            b'E' => bail!(
                "Postgres failed to create pgoutput slot '{}': {}",
                config.slot(),
                parse_error_response(&message.payload)
            ),
            b'C' | b'T' | b'N' | b'S' | b'K' => {}
            b'Z' => break,
            _ => {}
        }
    }

    let values = data_row.ok_or_else(|| {
        anyhow::anyhow!(
            "Postgres CREATE_REPLICATION_SLOT for '{}' returned no data row",
            config.slot()
        )
    })?;
    ensure!(
        values.len() >= 4,
        "Postgres CREATE_REPLICATION_SLOT returned {} columns, expected at least 4",
        values.len()
    );
    let slot_name = required_data_row_value(&values, 0, "slot_name")?;
    let consistent_lsn = required_data_row_value(&values, 1, "consistent_point")?;
    let snapshot_name = required_data_row_value(&values, 2, "snapshot_name")?;
    let output_plugin = required_data_row_value(&values, 3, "output_plugin")?;
    ensure!(
        slot_name == config.slot(),
        "Postgres created logical slot '{slot_name}', expected '{}'",
        config.slot()
    );
    ensure!(
        output_plugin == "pgoutput",
        "Postgres created logical slot '{}' with output plugin '{output_plugin}', expected pgoutput",
        config.slot()
    );
    let consistent_lsn = PostgresLsn::parse(&consistent_lsn)?;

    Ok(PostgresExportedSlotSnapshot {
        slot_name,
        consistent_lsn,
        snapshot_name,
        output_plugin,
        _stream: stream,
    })
}

pub(crate) fn validate_replication_slot_name(slot: &str) -> Result<()> {
    ensure!(!slot.is_empty(), "Postgres CDC slot cannot be empty");
    ensure!(
        slot.bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
        "Postgres CDC slot '{}' can only contain lowercase ASCII letters, digits, and underscores",
        slot
    );
    Ok(())
}

async fn authenticate_replication_control_stream<S>(
    stream: &mut S,
    config: &PostgresCdcConfig,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let message = read_backend_message(stream).await?;
        match message.tag {
            b'R' => {
                let (code, data) = parse_auth_request(&message.payload)?;
                handle_replication_control_auth_request(stream, config, code, data).await?;
            }
            b'E' => bail!(
                "Postgres replication authentication failed: {}",
                parse_error_response(&message.payload)
            ),
            b'S' | b'K' => {}
            b'Z' => return Ok(()),
            _ => {}
        }
    }
}

async fn handle_replication_control_auth_request<S>(
    stream: &mut S,
    config: &PostgresCdcConfig,
    code: i32,
    data: &[u8],
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match code {
        0 => Ok(()),
        3 => {
            let mut payload = Vec::from(config.password().as_bytes());
            payload.push(0);
            write_password_message(stream, &payload).await?;
            Ok(())
        }
        10 => authenticate_replication_control_scram(stream, config, data).await,
        _ => Err(PgWireError::Auth(format!("unsupported auth method code: {code}")).into()),
    }
}

async fn authenticate_replication_control_scram<S>(
    stream: &mut S,
    config: &PostgresCdcConfig,
    mechanisms_data: &[u8],
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mechanisms = parse_sasl_mechanisms(mechanisms_data);
    ensure!(
        mechanisms
            .iter()
            .any(|mechanism| mechanism == "SCRAM-SHA-256"),
        "Postgres server does not offer SCRAM-SHA-256 authentication; available mechanisms: {:?}",
        mechanisms
    );

    let scram = ScramClient::new(config.user());
    let mut initial_response = Vec::new();
    initial_response.extend_from_slice(b"SCRAM-SHA-256\0");
    initial_response.extend_from_slice(&(scram.client_first.len() as i32).to_be_bytes());
    initial_response.extend_from_slice(scram.client_first.as_bytes());
    write_password_message(stream, &initial_response).await?;

    let server_first = read_auth_data(stream, 11).await?;
    let server_first = String::from_utf8_lossy(&server_first);
    let (client_final, auth_message, salted_password) =
        scram.client_final(config.password(), &server_first)?;
    write_password_message(stream, client_final.as_bytes()).await?;

    let server_final = read_auth_data(stream, 12).await?;
    let server_final = String::from_utf8_lossy(&server_final);
    ScramClient::verify_server_final(&server_final, &salted_password, &auth_message)?;
    Ok(())
}

async fn read_auth_data<S>(stream: &mut S, expected_code: i32) -> Result<Vec<u8>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let message = read_backend_message(stream).await?;
        match message.tag {
            b'R' => {
                let (code, data) = parse_auth_request(&message.payload)?;
                ensure!(
                    code == expected_code,
                    "unexpected Postgres authentication code {code}, expected {expected_code}"
                );
                return Ok(data.to_vec());
            }
            b'E' => bail!(
                "Postgres authentication failed: {}",
                parse_error_response(&message.payload)
            ),
            _ => {}
        }
    }
}

fn parse_sasl_mechanisms(data: &[u8]) -> Vec<String> {
    let mut mechanisms = Vec::new();
    let mut remaining = data;
    while !remaining.is_empty() {
        let Some(pos) = remaining.iter().position(|&byte| byte == 0) else {
            break;
        };
        if pos == 0 {
            break;
        }
        mechanisms.push(String::from_utf8_lossy(&remaining[..pos]).to_string());
        remaining = &remaining[pos + 1..];
    }
    mechanisms
}

pub(crate) fn parse_simple_data_row(payload: &[u8]) -> Result<Vec<Option<String>>> {
    let mut remaining = payload;
    let column_count = take_i16(&mut remaining)? as usize;
    let mut values = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let len = take_i32(&mut remaining)?;
        if len == -1 {
            values.push(None);
            continue;
        }
        ensure!(len >= 0, "Postgres data row field length cannot be {len}");
        let len = len as usize;
        ensure!(
            remaining.len() >= len,
            "Postgres data row field is truncated: need {len} bytes, have {}",
            remaining.len()
        );
        let value = std::str::from_utf8(&remaining[..len])
            .context("decode Postgres data row field as UTF-8")?
            .to_string();
        remaining = &remaining[len..];
        values.push(Some(value));
    }
    ensure!(
        remaining.is_empty(),
        "Postgres data row has {} trailing bytes",
        remaining.len()
    );
    Ok(values)
}

fn required_data_row_value(values: &[Option<String>], idx: usize, name: &str) -> Result<String> {
    values
        .get(idx)
        .and_then(Clone::clone)
        .ok_or_else(|| anyhow::anyhow!("Postgres CREATE_REPLICATION_SLOT returned NULL {name}"))
}

fn take_i16(input: &mut &[u8]) -> Result<i16> {
    ensure!(
        input.len() >= 2,
        "Postgres data row is truncated while reading i16"
    );
    let value = i16::from_be_bytes([input[0], input[1]]);
    *input = &input[2..];
    Ok(value)
}

fn take_i32(input: &mut &[u8]) -> Result<i32> {
    ensure!(
        input.len() >= 4,
        "Postgres data row is truncated while reading i32"
    );
    let value = i32::from_be_bytes([input[0], input[1], input[2], input[3]]);
    *input = &input[4..];
    Ok(value)
}
