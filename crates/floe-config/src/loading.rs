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
        _ => parse_config_fallback(&contents),
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

fn parse_config_fallback(contents: &str) -> Result<NodeConfig> {
    if let Ok(config) = toml::from_str(contents) {
        return Ok(config);
    }
    if let Ok(config) = serde_json::from_str(contents) {
        return Ok(config);
    }
    if let Ok(config) = serde_yaml::from_str(contents) {
        return Ok(config);
    }
    toml::from_str(contents).context("parse config (tried toml, json, yaml)")
}
