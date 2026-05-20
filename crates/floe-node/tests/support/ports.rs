use std::net::TcpListener;

use anyhow::{Context, Result};

pub(crate) fn find_unused_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("bind ephemeral port")?;
    Ok(listener.local_addr().context("read ephemeral port")?.port())
}
