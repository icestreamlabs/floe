use super::*;
use clap::{ArgMatches, CommandFactory, FromArgMatches, parser::ValueSource};
use std::collections::HashSet;

#[derive(Debug, Default)]
pub(super) struct RunArgOverrides {
    ids: HashSet<String>,
}

impl RunArgOverrides {
    pub(super) fn contains(&self, id: &str) -> bool {
        self.ids.contains(id)
    }

    #[cfg(test)]
    pub(super) fn from_ids(ids: impl IntoIterator<Item = &'static str>) -> Self {
        Self {
            ids: ids.into_iter().map(str::to_string).collect(),
        }
    }
}

pub(super) fn parse_run_command() -> anyhow::Result<Option<(cli::RunArgs, RunArgOverrides)>> {
    let matches = cli::Cli::command().get_matches();
    let overrides = run_arg_overrides(&matches);
    let cli = cli::Cli::from_arg_matches(&matches)?;
    match cli.command {
        cli::Command::Run(args) => Ok(Some((args, overrides))),
    }
}

fn run_arg_overrides(matches: &ArgMatches) -> RunArgOverrides {
    matches
        .subcommand_matches("run")
        .map(|run| RunArgOverrides {
            ids: run
                .ids()
                .filter(|id| run.value_source(id.as_str()) == Some(ValueSource::CommandLine))
                .map(|id| id.as_str().to_string())
                .collect(),
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_explicit_values_even_when_they_equal_cli_defaults() {
        let matches = cli::Cli::command()
            .try_get_matches_from([
                "floe-node",
                "run",
                "--events-per-second",
                "10",
                "--ingest-batch-size",
                "256",
            ])
            .expect("parse CLI");
        let overrides = run_arg_overrides(&matches);

        assert!(overrides.contains("events_per_second"));
        assert!(overrides.contains("ingest_batch_size"));
        assert!(!overrides.contains("kafka_poll_ms"));
    }
}
