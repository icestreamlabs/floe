use super::*;

pub(super) fn command_success<I, S>(
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

pub(super) fn run_status<I, S>(
    program: impl AsRef<OsStr>,
    args: I,
    cwd: Option<&Path>,
) -> Result<()>
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

pub(super) fn run_status_vec(
    program: impl AsRef<OsStr>,
    args: &[String],
    cwd: Option<&Path>,
) -> Result<()> {
    let status = command(program, args, cwd)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("run command")?;
    ensure_status(status)
}

pub(super) fn run_capture<I, S>(
    program: impl AsRef<OsStr>,
    args: I,
    cwd: Option<&Path>,
) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = command(program, args, cwd)
        .output()
        .context("run command")?;
    if !output.status.success() {
        bail!("command failed with {}", output.status);
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

pub(super) fn command<I, S>(program: impl AsRef<OsStr>, args: I, cwd: Option<&Path>) -> Command
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

pub(super) fn ensure_status(status: ExitStatus) -> Result<()> {
    ensure!(status.success(), "command failed with {status}");
    Ok(())
}

pub(super) fn env_string(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

pub(super) fn env_nonempty(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

pub(super) fn env_path(name: &str) -> Option<PathBuf> {
    env_nonempty(name).map(PathBuf::from)
}

pub(super) fn env_bool(name: &str, default: bool) -> bool {
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

pub(super) fn env_parse<T>(name: &str, default: T) -> Result<T>
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

pub(super) fn repo_root() -> Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| anyhow!("cannot derive repo root from CARGO_MANIFEST_DIR"))
}

pub(super) fn current_millis() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time before UNIX_EPOCH")?
        .as_millis())
}

pub(super) fn source_labels(sources: &[Source]) -> String {
    sources
        .iter()
        .map(|source| source.label())
        .collect::<Vec<_>>()
        .join(" ")
}

pub(super) fn validate_identifier(identifier: &str) -> Result<()> {
    let mut chars = identifier.chars();
    let Some(first) = chars.next() else {
        bail!("empty SQL identifier");
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

pub(super) fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub(super) fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

pub(super) fn seconds_cell(ms: Option<u128>) -> String {
    ms.map(|ms| format!("{:.3}", ms as f64 / 1000.0))
        .unwrap_or_else(|| "n/a".to_string())
}

pub(super) fn log(message: impl AsRef<str>) {
    println!("[nexmark-cross-engine] {}", message.as_ref());
}

pub(super) fn token_value(line: &str, prefix: &str) -> Option<String> {
    for token in line.split_whitespace() {
        if let Some(value) = token.strip_prefix(prefix) {
            return Some(
                value
                    .trim_matches(|ch: char| {
                        !(ch.is_ascii_alphanumeric()
                            || ch == '_'
                            || ch == '.'
                            || ch == ':'
                            || ch == '-')
                    })
                    .to_string(),
            );
        }
    }
    None
}

pub(super) fn print_tail(path: PathBuf, lines: usize) {
    if let Ok(content) = fs::read_to_string(path) {
        let tail = content.lines().rev().take(lines).collect::<Vec<_>>();
        for line in tail.into_iter().rev() {
            eprintln!("{line}");
        }
    }
}

pub(super) fn print_usage() {
    println!(
        "Usage: nexmark_cross_engine_compare [floe|materialize|risingwave|feldera|all] [all|nexmark_all|q0..q22]"
    );
}
