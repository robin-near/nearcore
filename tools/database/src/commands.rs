use crate::adjust_database::ChangeDbKindCommand;
use crate::analyse_data_size_distribution::AnalyseDataSizeDistributionCommand;
use crate::compact::RunCompactionCommand;
use crate::make_snapshot::MakeSnapshotCommand;
use crate::run_migrations::RunMigrationsCommand;
use crate::state_perf::StatePerfCommand;
use crate::trim_database::TrimDatabaseCommand;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
pub struct DatabaseCommand {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Parser)]
#[clap(subcommand_required = true, arg_required_else_help = true)]
enum SubCommand {
    /// Analyse data size distribution in RocksDB
    AnalyseDataSizeDistribution(AnalyseDataSizeDistributionCommand),

    /// Change DbKind of hot or cold db.
    ChangeDbKind(ChangeDbKindCommand),

    /// Run SST file compaction on database
    CompactDatabase(RunCompactionCommand),

    /// Make snapshot of the database
    MakeSnapshot(MakeSnapshotCommand),

    /// Run migrations,
    RunMigrations(RunMigrationsCommand),

    /// Run performance test for State column reads.
    /// Uses RocksDB data specified via --home argument.
    StatePerf(StatePerfCommand),

    /// Trim the database by removing not-strictly-necessary columns.
    /// DO NOT USE FOR ANYTHING EXCEPT PERFORMANCE DEBUGGING.
    /// DO NOT USE FOR PRODUCTION NODES, IT IS UNSUPPORTED AND WILL
    /// BREAK YOUR NODE.
    TrimDatabase(TrimDatabaseCommand),
}

impl DatabaseCommand {
    pub fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        match &self.subcmd {
            SubCommand::AnalyseDataSizeDistribution(cmd) => cmd.run(home),
            SubCommand::ChangeDbKind(cmd) => cmd.run(home),
            SubCommand::CompactDatabase(cmd) => cmd.run(home),
            SubCommand::MakeSnapshot(cmd) => {
                let near_config = nearcore::config::load_config(
                    &home,
                    near_chain_configs::GenesisValidationMode::UnsafeFast,
                )
                .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));
                cmd.run(home, near_config.config.archive, &near_config.config.store)
            }
            SubCommand::RunMigrations(cmd) => cmd.run(home),
            SubCommand::StatePerf(cmd) => cmd.run(home),
            SubCommand::TrimDatabase(cmd) => cmd.run(home),
        }
    }
}
