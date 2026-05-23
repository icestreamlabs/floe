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
    build_stream_sql("TAIL", mv, with_snapshot, as_of)
}

pub fn build_subscribe_sql(mv: &str, with_snapshot: bool, as_of: Option<i64>) -> String {
    build_stream_sql("SUBSCRIBE", mv, with_snapshot, as_of)
}

fn build_stream_sql(keyword: &str, mv: &str, with_snapshot: bool, as_of: Option<i64>) -> String {
    let mut sql = format!("{keyword} {mv}");
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::thread;

    fn write_message(stream: &mut TcpStream, msg_type: u8, payload: &[u8]) {
        stream.write_all(&[msg_type]).expect("write type");
        let len = (payload.len() + 4) as u32;
        stream
            .write_all(&len.to_be_bytes())
            .expect("write message length");
        stream.write_all(payload).expect("write payload");
        stream.flush().expect("flush message");
    }

    fn row_description_payload(columns: &[&str]) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(columns.len() as u16).to_be_bytes());
        for column in columns {
            payload.extend_from_slice(column.as_bytes());
            payload.push(0);
            payload.extend_from_slice(&0u32.to_be_bytes()); // table oid
            payload.extend_from_slice(&0u16.to_be_bytes()); // attr num
            payload.extend_from_slice(&25u32.to_be_bytes()); // text type oid
            payload.extend_from_slice(&(-1i16).to_be_bytes()); // type size
            payload.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
            payload.extend_from_slice(&0u16.to_be_bytes()); // text format
        }
        payload
    }

    fn data_row_payload(values: &[Option<&str>]) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(values.len() as u16).to_be_bytes());
        for value in values {
            match value {
                Some(text) => {
                    payload.extend_from_slice(&(text.len() as i32).to_be_bytes());
                    payload.extend_from_slice(text.as_bytes());
                }
                None => payload.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        payload
    }

    #[test]
    fn build_tail_sql_formats_options() {
        assert_eq!(build_tail_sql("mv_q1", false, None), "TAIL mv_q1");
        assert_eq!(
            build_tail_sql("mv_q1", true, Some(42)),
            "TAIL mv_q1 WITH SNAPSHOT AS OF 42"
        );
        assert_eq!(
            build_subscribe_sql("mv_q1", true, Some(42)),
            "SUBSCRIBE mv_q1 WITH SNAPSHOT AS OF 42"
        );
    }

    #[test]
    fn parse_row_description_and_data_row() {
        let description = row_description_payload(&["auction", "price"]);
        let columns = parse_row_description(&description).expect("parse row description");
        assert_eq!(columns, vec!["auction", "price"]);

        let row = parse_data_row(&data_row_payload(&[Some("10"), None, Some("alice")]))
            .expect("parse data row");
        assert_eq!(row, vec!["10", "NULL", "alice"]);
    }

    #[test]
    fn parse_error_message_prefers_message_field() {
        let mut payload = Vec::new();
        payload.push(b'S');
        payload.extend_from_slice(b"ERROR\0");
        payload.push(b'M');
        payload.extend_from_slice(b"boom\0");
        payload.push(0);
        assert_eq!(parse_error_message(&payload), "boom");
    }

    #[test]
    fn run_executes_simple_tail_exchange() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept client");

            // Startup packet: [len][payload].
            let mut len = [0u8; 4];
            stream.read_exact(&mut len).expect("startup len");
            let startup_len = u32::from_be_bytes(len) as usize;
            let mut startup = vec![0u8; startup_len.saturating_sub(4)];
            stream.read_exact(&mut startup).expect("startup payload");
            assert!(
                startup.windows(b"user\0".len()).any(|w| w == b"user\0"),
                "startup packet should include user key"
            );

            write_message(&mut stream, b'R', &0u32.to_be_bytes());
            write_message(&mut stream, b'Z', b"I");

            // Query packet: [Q][len][sql\0].
            let mut typ = [0u8; 1];
            stream.read_exact(&mut typ).expect("query type");
            assert_eq!(typ[0], b'Q');
            let mut q_len = [0u8; 4];
            stream.read_exact(&mut q_len).expect("query len");
            let q_len = u32::from_be_bytes(q_len) as usize;
            let mut query = vec![0u8; q_len.saturating_sub(4)];
            stream.read_exact(&mut query).expect("query payload");
            assert!(
                query.starts_with(b"TAIL mv_q1"),
                "expected tail query in startup exchange"
            );

            write_message(&mut stream, b'T', &row_description_payload(&["k", "v"]));
            write_message(
                &mut stream,
                b'D',
                &data_row_payload(&[Some("1"), Some("ok")]),
            );
            write_message(&mut stream, b'C', b"TAIL 1\0");
            write_message(&mut stream, b'Z', b"I");

            let mut terminate_type = [0u8; 1];
            stream
                .read_exact(&mut terminate_type)
                .expect("terminate type");
            assert_eq!(terminate_type[0], b'X');
            let mut terminate_len = [0u8; 4];
            stream
                .read_exact(&mut terminate_len)
                .expect("terminate len");
            assert_eq!(u32::from_be_bytes(terminate_len), 4);
        });

        let result = run(TailConfig {
            host: "127.0.0.1".to_string(),
            port: addr.port(),
            user: "postgres".to_string(),
            database: "postgres".to_string(),
            sql: build_tail_sql("mv_q1", false, None),
            max_rows: Some(1),
            no_header: true,
        });
        assert!(result.is_ok(), "tail client should complete successfully");
        server.join().expect("server thread");
    }
}
