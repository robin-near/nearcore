use std::path::Path;
use std::sync::Arc;

use clap::Parser;
use near_store::db::Database;

use crate::utils::{open_rocksdb, resolve_column};

#[derive(Parser)]
pub(crate) struct ColumnStatsCommand {
    #[arg(short, long)]
    column: String,
}

impl ColumnStatsCommand {
    pub(crate) fn run(&self, home: &Path) -> anyhow::Result<()> {
        let col = resolve_column(&self.column).unwrap();
        let rocksdb = Arc::new(open_rocksdb(home, near_store::Mode::ReadOnly)?);
        let mut tot_keys_size = 0;
        let mut tot_values_size = 0;
        for res in rocksdb.iter(col) {
            let (key, value) = res.unwrap();
            tot_keys_size += key.len();
            tot_values_size += value.len();
        }
        println!(
            "total size: keys {}GB, values {}GB",
            tot_keys_size as f64 / 1000_000_000.0,
            tot_values_size as f64 / 1000_000_000.0
        );
        let stats = rocksdb.get_store_statistics().unwrap();
        for (prop, col_values) in stats.data {
            if let Some(value) = col_values
                .iter()
                .flat_map(|stats| match stats {
                    near_store::db::StatsValue::ColumnValue(c, val) => {
                        if c == &col {
                            Some(*val)
                        } else {
                            None
                        }
                    }
                    _ => None,
                })
                .next()
            {
                println!("{prop}={value}");
            }
        }
        Ok(())
    }
}
