use crate::utils::open_rocksdb;
use clap::Parser;
use near_store::db::Database;
use near_store::DBCol;
use std::path::PathBuf;
use strum::IntoEnumIterator;

#[derive(Parser)]
pub(crate) struct RunCompactionCommand {
    /// If specified only this column will be compacted
    #[arg(short, long)]
    column: Option<String>,
}

impl RunCompactionCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        let db = open_rocksdb(home, near_store::Mode::ReadWrite)?;
        if let Some(col) = self.column.as_ref() {
            let col = DBCol::iter()
                .filter(|db_col| <&str>::from(db_col) == col)
                .next()
                .expect("column does not exist");
            db.compact_column(col)?;
        } else {
            db.compact()?;
        }
        eprintln!("Compaction is finished!");
        Ok(())
    }
}
