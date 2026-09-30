use clap::{Parser, Subcommand};
use flow_journal::maintenance;
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "Offline v2 journal inspection and maintenance")]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Verify {
        data_dir: PathBuf,
    },
    RebuildIndex {
        data_dir: PathBuf,
    },
    Backup {
        data_dir: PathBuf,
        destination: PathBuf,
    },
    /// Prints the recoverable prefix by default. --confirm writes a NEW directory only.
    Repair {
        data_dir: PathBuf,
        destination: PathBuf,
        #[arg(long)]
        confirm: bool,
    },
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let report = match args.command {
        Command::Verify { data_dir } => {
            let _lock = maintenance::offline_lock(&data_dir)?;
            maintenance::verify(&data_dir)?
        }
        Command::RebuildIndex { data_dir } => {
            let _lock = maintenance::offline_lock(&data_dir)?;
            maintenance::rebuild_index(&data_dir, u64::MAX)?
        }
        Command::Backup {
            data_dir,
            destination,
        } => {
            let _lock = maintenance::offline_lock(&data_dir)?;
            maintenance::backup(&data_dir, &destination, u64::MAX)?
        }
        Command::Repair {
            data_dir,
            destination,
            confirm,
        } => {
            if !confirm {
                let _lock = maintenance::offline_lock(&data_dir)?;
                let report = flow_journal::scan(&data_dir, |_, _| Ok(()))?;
                println!("{}", serde_json::to_string_pretty(&report)?);
                return Err("repair requires --confirm; candidate suffix may include previously acknowledged data; original evidence will be preserved".into());
            }
            maintenance::repair(&data_dir, &destination, true)?
        }
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    if report.fault.is_some() {
        return Err("journal verification found corruption; source is unchanged".into());
    }
    Ok(())
}
