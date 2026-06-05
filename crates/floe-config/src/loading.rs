use super::*;

pub fn load_config(path: impl AsRef<Path>) -> Result<NodeConfig> {
    let path = path.as_ref();
    let contents =
        std::fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
    let ext = path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    let config = match ext.as_str() {
        "toml" => parse_toml_config(&contents),
        "yaml" | "yml" => serde_yaml::from_str(&contents).context("parse yaml config"),
        "json" => serde_json::from_str(&contents).context("parse json config"),
        _ => bail!(
            "unsupported config extension '{}' for {}; expected .toml, .yaml, .yml, or .json",
            ext,
            path.display()
        ),
    }?;
    validate_node_config(&config).context("validate node config")?;
    Ok(config)
}

pub fn load_toml_config(path: impl AsRef<Path>) -> Result<NodeConfig> {
    let path = path.as_ref();
    let contents =
        std::fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
    parse_toml_config(&contents)
        .and_then(|config| {
            validate_node_config(&config).context("validate node config")?;
            Ok(config)
        })
        .with_context(|| format!("load TOML config {}", path.display()))
}

pub fn parse_toml_config(contents: &str) -> Result<NodeConfig> {
    toml::from_str(contents).context("parse toml config")
}
