use std::io::{Read, Write};
use std::net::TcpStream;

use anyhow::Context;

#[derive(Debug, Clone)]
pub struct TailConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: String,
    pub sql: String,
    pub max_rows: Option<usize>,
    pub no_header: bool,
}

pub fn build_tail_sql(mv: &str, with_snapshot: bool, as_of: Option<i64>) -> String {
    let mut sql = format!("TAIL {mv}");
    if with_snapshot {
        sql.push_str(" WITH SNAPSHOT");
    }
    if let Some(as_of) = as_of {
        sql.push_str(&format!(" AS OF {as_of}"));
    }
    sql
}

pub fn run(config: TailConfig) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect((config.host.as_str(), config.port))
        .with_context(|| format!("connect to {}:{}", config.host.as_str(), config.port))?;
    stream.set_nodelay(true)?;

    send_startup(&mut stream, &config.user, &config.database)?;
    wait_for_ready(&mut stream)?;

    send_query(&mut stream, &config.sql)?;
    let mut printed_header = config.no_header;
    let mut row_count = 0usize;

    loop {
        let Some((msg_type, payload)) = read_message(&mut stream)? else {
            break;
        };
        match msg_type {
            b'T' => {
                if !printed_header {
                    let columns = parse_row_description(&payload)?;
                    println!("{}", columns.join("\t"));
                    printed_header = true;
                }
            }
            b'D' => {
                let row = parse_data_row(&payload)?;
                println!("{}", row.join("\t"));
                row_count += 1;
                if let Some(max_rows) = config.max_rows
                    && row_count >= max_rows
                {
                    break;
                }
            }
            b'E' => {
                let message = parse_error_message(&payload);
                anyhow::bail!("TAIL error: {message}");
            }
            b'N' => {}
            b'C' => {}
            b'Z' => break,
            _ => {}
        }
    }

    send_terminate(&mut stream)?;
    Ok(())
}

fn send_startup(stream: &mut TcpStream, user: &str, database: &str) -> anyhow::Result<()> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&196_608u32.to_be_bytes());
    payload.extend_from_slice(b"user\0");
    payload.extend_from_slice(user.as_bytes());
    payload.push(0);
    payload.extend_from_slice(b"database\0");
    payload.extend_from_slice(database.as_bytes());
    payload.push(0);
    payload.extend_from_slice(b"client_encoding\0UTF8\0");
    payload.push(0);

    let len = (payload.len() + 4) as u32;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(&payload)?;
    stream.flush()?;
    Ok(())
}

fn wait_for_ready(stream: &mut TcpStream) -> anyhow::Result<()> {
    loop {
        let Some((msg_type, payload)) = read_message(stream)? else {
            anyhow::bail!("server closed connection during startup");
        };
        match msg_type {
            b'R' => {
                if payload.len() < 4 {
                    anyhow::bail!("invalid auth response");
                }
                let code = u32::from_be_bytes(payload[0..4].try_into()?);
                if code != 0 {
                    anyhow::bail!("unsupported authentication method {code}");
                }
            }
            b'E' => {
                let message = parse_error_message(&payload);
                anyhow::bail!("startup error: {message}");
            }
            b'Z' => return Ok(()),
            _ => {}
        }
    }
}

fn send_query(stream: &mut TcpStream, sql: &str) -> anyhow::Result<()> {
    let mut payload = Vec::with_capacity(sql.len() + 1);
    payload.extend_from_slice(sql.as_bytes());
    payload.push(0);
    let len = (payload.len() + 4) as u32;
    stream.write_all(b"Q")?;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(&payload)?;
    stream.flush()?;
    Ok(())
}

fn send_terminate(stream: &mut TcpStream) -> anyhow::Result<()> {
    stream.write_all(b"X")?;
    stream.write_all(&4u32.to_be_bytes())?;
    stream.flush()?;
    Ok(())
}

fn read_message(stream: &mut TcpStream) -> anyhow::Result<Option<(u8, Vec<u8>)>> {
    let mut msg_type = [0u8; 1];
    if stream.read_exact(&mut msg_type).is_err() {
        return Ok(None);
    }
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len < 4 {
        anyhow::bail!("invalid message length {len}");
    }
    let mut payload = vec![0u8; len - 4];
    stream.read_exact(&mut payload)?;
    Ok(Some((msg_type[0], payload)))
}

fn parse_row_description(payload: &[u8]) -> anyhow::Result<Vec<String>> {
    if payload.len() < 2 {
        anyhow::bail!("row description too short");
    }
    let count = u16::from_be_bytes(payload[0..2].try_into()?) as usize;
    let mut idx = 2;
    let mut names = Vec::with_capacity(count);
    for _ in 0..count {
        let (name, next) = read_cstring(payload, idx)?;
        names.push(name);
        idx = next + 18;
        if idx > payload.len() {
            anyhow::bail!("row description truncated");
        }
    }
    Ok(names)
}

fn parse_data_row(payload: &[u8]) -> anyhow::Result<Vec<String>> {
    if payload.len() < 2 {
        anyhow::bail!("data row too short");
    }
    let count = u16::from_be_bytes(payload[0..2].try_into()?) as usize;
    let mut idx = 2;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        if idx + 4 > payload.len() {
            anyhow::bail!("data row truncated");
        }
        let len = i32::from_be_bytes(payload[idx..idx + 4].try_into()?);
        idx += 4;
        if len < 0 {
            values.push("NULL".to_string());
            continue;
        }
        let end = idx + len as usize;
        if end > payload.len() {
            anyhow::bail!("data row truncated");
        }
        let text = String::from_utf8_lossy(&payload[idx..end]).to_string();
        values.push(text);
        idx = end;
    }
    Ok(values)
}

fn parse_error_message(payload: &[u8]) -> String {
    let mut idx = 0;
    let mut message = None;
    while idx < payload.len() {
        let field = payload[idx];
        if field == 0 {
            break;
        }
        idx += 1;
        if let Ok((value, next)) = read_cstring(payload, idx) {
            if field == b'M' {
                message = Some(value);
                break;
            }
            idx = next;
        } else {
            break;
        }
    }
    message.unwrap_or_else(|| "unknown error".to_string())
}

fn read_cstring(payload: &[u8], start: usize) -> anyhow::Result<(String, usize)> {
    let mut idx = start;
    while idx < payload.len() && payload[idx] != 0 {
        idx += 1;
    }
    if idx >= payload.len() {
        anyhow::bail!("cstring not terminated");
    }
    let value = String::from_utf8_lossy(&payload[start..idx]).to_string();
    Ok((value, idx + 1))
}
