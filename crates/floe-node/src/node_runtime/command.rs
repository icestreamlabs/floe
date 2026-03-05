use super::*;

pub(super) fn parse_run_command() -> anyhow::Result<Option<cli::RunArgs>> {
    let cli = cli::Cli::parse();
    match cli.command {
        cli::Command::Run(args) => Ok(Some(args)),
        cli::Command::Tail(args) => {
            let config = args.to_config()?;
            tail_client::run(config)?;
            Ok(None)
        }
    }
}
