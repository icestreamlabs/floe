use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, ensure};

#[cfg(unix)]
unsafe extern "C" {
    fn getpgid(pid: i32) -> i32;
}

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

pub fn configure_process_group(command: &mut Command) {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
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

pub fn run_status_vec(
    program: impl AsRef<OsStr>,
    args: &[String],
    cwd: Option<&Path>,
) -> Result<()> {
    run_status(program, args, cwd)
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

pub fn wait_before_retry(deadline: Instant, interval: Duration) -> bool {
    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
        return false;
    };
    if remaining.is_zero() {
        return false;
    }
    thread::park_timeout(interval.min(remaining));
    deadline > Instant::now()
}

pub fn validate_identifier(identifier: &str) -> Result<()> {
    let mut chars = identifier.chars();
    let Some(first) = chars.next() else {
        anyhow::bail!("empty SQL identifier");
    };
    ensure!(
        first == '_' || first.is_ascii_alphabetic(),
        "invalid SQL identifier '{identifier}'"
    );
    ensure!(
        chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()),
        "invalid SQL identifier '{identifier}'"
    );
    Ok(())
}

pub fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

pub fn seconds_cell(ms: Option<u128>) -> String {
    ms.map(|ms| format!("{:.3}", ms as f64 / 1000.0))
        .unwrap_or_else(|| "n/a".to_string())
}

pub fn terminate_child_process_group(child: &mut Child, graceful_timeout: Duration) {
    if matches!(child.try_wait(), Ok(Some(_))) {
        return;
    }

    signal_child_process_group(child, "INT");
    if wait_for_child_exit(child, graceful_timeout) {
        return;
    }

    signal_child_process_group(child, "TERM");
    if wait_for_child_exit(child, Duration::from_secs(2)) {
        return;
    }

    let _ = child.kill();
    let _ = child.wait();
}

pub fn terminate_stale_floe_nodes_on_pgwire_port(port: u16, graceful_timeout: Duration) {
    #[cfg(target_os = "linux")]
    {
        for pid in floe_node_pids_on_pgwire_port(port) {
            terminate_process_pid(pid, graceful_timeout);
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = (port, graceful_timeout);
    }
}

fn wait_for_child_exit(child: &mut Child, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if matches!(child.try_wait(), Ok(Some(_))) {
            return true;
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return false;
        };
        if remaining.is_zero() {
            return false;
        }
        thread::park_timeout(Duration::from_millis(100).min(remaining));
    }
}

#[cfg(target_os = "linux")]
fn floe_node_pids_on_pgwire_port(port: u16) -> Vec<u32> {
    let Ok(entries) = fs::read_dir("/proc") else {
        return Vec::new();
    };
    entries
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let pid = entry.file_name().to_string_lossy().parse::<u32>().ok()?;
            let cmdline = fs::read(entry.path().join("cmdline")).ok()?;
            let args = cmdline
                .split(|byte| *byte == 0)
                .filter(|arg| !arg.is_empty())
                .map(|arg| String::from_utf8_lossy(arg).to_string())
                .collect::<Vec<_>>();
            floe_node_cmdline_matches_pgwire_port(&args, port).then_some(pid)
        })
        .collect()
}

#[cfg(target_os = "linux")]
fn floe_node_cmdline_matches_pgwire_port(args: &[String], port: u16) -> bool {
    let Some(program) = args.first() else {
        return false;
    };
    if Path::new(program).file_name().and_then(OsStr::to_str) != Some("floe-node") {
        return false;
    }
    if args.get(1).map(String::as_str) != Some("run") {
        return false;
    }

    let pgwire_addr = format!("127.0.0.1:{port}");
    args.windows(2)
        .any(|pair| pair[0] == "--pgwire-addr" && pair[1] == pgwire_addr)
        || args
            .iter()
            .any(|arg| arg == &format!("--pgwire-addr={pgwire_addr}"))
}

#[cfg(target_os = "linux")]
fn terminate_process_pid(pid: u32, graceful_timeout: Duration) {
    signal_pid(pid, "INT");
    if wait_for_pid_exit(pid, graceful_timeout) {
        return;
    }
    signal_pid(pid, "TERM");
    if wait_for_pid_exit(pid, Duration::from_secs(2)) {
        return;
    }
    signal_pid(pid, "KILL");
    let _ = wait_for_pid_exit(pid, Duration::from_secs(2));
}

#[cfg(target_os = "linux")]
fn signal_pid(pid: u32, signal: &str) {
    let _ = Command::new("kill")
        .args([format!("-{signal}"), pid.to_string()])
        .status();
}

#[cfg(target_os = "linux")]
fn wait_for_pid_exit(pid: u32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let proc_path = PathBuf::from("/proc").join(pid.to_string());
    loop {
        if !proc_path.exists() {
            return true;
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return false;
        };
        if remaining.is_zero() {
            return false;
        }
        thread::park_timeout(Duration::from_millis(100).min(remaining));
    }
}

fn signal_child_process_group(child: &Child, signal: &str) {
    #[cfg(unix)]
    {
        let pid = child.id();
        let Some(target) = signal_target_for_child(pid, child_owns_process_group(pid)) else {
            return;
        };
        let _ = Command::new("kill")
            .arg(format!("-{signal}"))
            .arg("--")
            .arg(target)
            .status();
    }

    #[cfg(not(unix))]
    {
        let _ = (child, signal);
    }
}

#[cfg(unix)]
fn signal_target_for_child(pid: u32, owns_process_group: bool) -> Option<String> {
    if pid == 0 {
        return None;
    }
    if owns_process_group {
        Some(format!("-{pid}"))
    } else {
        Some(pid.to_string())
    }
}

#[cfg(unix)]
fn child_owns_process_group(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    let pgid = unsafe { getpgid(pid) };
    pgid > 0 && pgid == pid
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

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    use super::floe_node_cmdline_matches_pgwire_port;
    #[cfg(unix)]
    use super::{
        child_owns_process_group, configure_process_group, signal_target_for_child,
        terminate_child_process_group,
    };
    #[cfg(unix)]
    use std::process::Command;
    #[cfg(unix)]
    use std::time::Duration;

    #[cfg(unix)]
    #[test]
    fn signal_target_uses_process_group_only_for_group_leader() {
        assert_eq!(
            signal_target_for_child(12_345, true).as_deref(),
            Some("-12345")
        );
        assert_eq!(
            signal_target_for_child(12_345, false).as_deref(),
            Some("12345")
        );
        assert_eq!(signal_target_for_child(0, true), None);
    }

    #[cfg(unix)]
    #[test]
    fn inherited_process_group_uses_child_pid_target() {
        let mut child = Command::new("sleep")
            .arg("60")
            .spawn()
            .expect("spawn sleep");
        let pid = child.id();
        let owns_process_group = child_owns_process_group(pid);
        let signal_target = signal_target_for_child(pid, owns_process_group);
        let _ = child.kill();
        let _ = child.wait();

        assert!(!owns_process_group);
        assert_eq!(signal_target, Some(pid.to_string()));
    }

    #[cfg(unix)]
    #[test]
    fn configured_process_group_uses_group_target() {
        let mut command = Command::new("sleep");
        configure_process_group(&mut command);
        let mut child = command.arg("60").spawn().expect("spawn sleep");
        let pid = child.id();
        let owns_process_group = child_owns_process_group(pid);
        let signal_target = signal_target_for_child(pid, owns_process_group);
        terminate_child_process_group(&mut child, Duration::from_millis(100));

        assert!(owns_process_group);
        assert_eq!(signal_target, Some(format!("-{pid}")));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn stale_floe_matcher_requires_floe_run_on_exact_pgwire_port() {
        let args = vec![
            "/repo/target/release/floe-node".to_string(),
            "run".to_string(),
            "--pgwire-addr".to_string(),
            "127.0.0.1:15432".to_string(),
            "--admin-port".to_string(),
            "0".to_string(),
        ];
        assert!(floe_node_cmdline_matches_pgwire_port(&args, 15432));
        assert!(!floe_node_cmdline_matches_pgwire_port(&args, 15433));

        let mut non_floe = args.clone();
        non_floe[0] = "/usr/bin/gnome-shell".to_string();
        assert!(!floe_node_cmdline_matches_pgwire_port(&non_floe, 15432));
    }
}
