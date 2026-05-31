use super::*;

pub(super) fn parse_run_command() -> anyhow::Result<Option<cli::RunArgs>> {
    let cli = cli::Cli::parse();
    match cli.command {
        cli::Command::Run(args) => Ok(Some(args)),
    }
}
