#![allow(dead_code)]

use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, ensure};

pub fn repo_root() -> Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| anyhow!("cannot derive repo root from CARGO_MANIFEST_DIR"))
}

pub fn env_string(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

pub fn env_nonempty(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

pub fn env_path(name: &str) -> Option<PathBuf> {
    env_nonempty(name).map(PathBuf::from)
}

pub fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.as_str(),
                "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
            )
        })
        .unwrap_or(default)
}

pub fn env_parse<T>(name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match env::var(name) {
        Ok(value) => value
            .parse::<T>()
            .map_err(|err| anyhow!("parse {name}={value}: {err}")),
        Err(_) => Ok(default),
    }
}

pub fn current_millis() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time before UNIX_EPOCH")?
        .as_millis())
}

pub fn current_nanos() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time before UNIX_EPOCH")?
        .as_nanos())
}

pub fn command<I, S>(program: impl AsRef<OsStr>, args: I, cwd: Option<&Path>) -> Command
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = Command::new(program);
    command.args(args);
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    command
}

pub fn command_success<I, S>(
    program: impl AsRef<OsStr>,
    args: I,
    cwd: Option<&Path>,
) -> Result<bool>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let status = command(program, args, cwd)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("run command")?;
    Ok(status.success())
}

pub fn run_status<I, S>(program: impl AsRef<OsStr>, args: I, cwd: Option<&Path>) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let status = command(program, args, cwd)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("run command")?;
    ensure_status(status)
}

pub fn run_capture<I, S>(program: impl AsRef<OsStr>, args: I, cwd: Option<&Path>) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = command(program, args, cwd)
        .output()
        .context("run command")?;
    ensure!(
        output.status.success(),
        "command failed with {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

pub fn run_capture_stdin<I, S>(
    program: impl AsRef<OsStr>,
    args: I,
    stdin: &[u8],
    cwd: Option<&Path>,
) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut child = command(program, args, cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawn command")?;
    child
        .stdin
        .as_mut()
        .context("open command stdin")?
        .write_all(stdin)
        .context("write command stdin")?;
    let output = child.wait_with_output().context("wait for command")?;
    ensure!(
        output.status.success(),
        "command failed with {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

pub fn ensure_status(status: ExitStatus) -> Result<()> {
    ensure!(status.success(), "command failed with {status}");
    Ok(())
}

pub fn write_file(path: impl AsRef<Path>, content: impl AsRef<[u8]>) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create parent directory {}", parent.display()))?;
    }
    fs::write(path, content).with_context(|| format!("write {}", path.display()))
}

pub fn append_file(path: impl AsRef<Path>, content: impl AsRef<str>) -> Result<()> {
    use std::io::Write;

    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create parent directory {}", parent.display()))?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open {}", path.display()))?;
    file.write_all(content.as_ref().as_bytes())
        .with_context(|| format!("append {}", path.display()))
}

pub fn wait_until<F>(timeout: Duration, interval: Duration, mut check: F) -> Result<bool>
where
    F: FnMut() -> Result<bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if check()? {
            return Ok(true);
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return Ok(false);
        };
        if remaining.is_zero() {
            return Ok(false);
        }
        thread::park_timeout(interval.min(remaining));
    }
}

pub fn seconds(ms: u128) -> String {
    format!("{:.3}", ms as f64 / 1000.0)
}

pub fn normalize_flag(value: &str) -> bool {
    matches!(value, "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON")
}

pub fn print_tail(path: impl AsRef<Path>, lines: usize) {
    if let Ok(content) = fs::read_to_string(path) {
        let tail = content.lines().rev().take(lines).collect::<Vec<_>>();
        for line in tail.into_iter().rev() {
            eprintln!("{line}");
        }
    }
}

use std::io::Write as _;
