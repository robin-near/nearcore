use std::{
    fs::File,
    io::{Seek, SeekFrom, Write},
};

use near_primitives::{
    hash::CryptoHash,
    types::{BlockHeight, ShardId},
};

#[derive(serde::Serialize)]
pub struct StateWitnessStat {
    pub block_height: BlockHeight,
    pub block_hash: CryptoHash,
    pub shard_id: ShardId,
    pub num_outcomes: u64,
    pub gas_burnt: u64,
    pub num_trie_nodes: u64,
    pub total_trie_size: u64,
}

pub struct StateWitnessStatWriter {
    writer: Option<File>,
}

impl StateWitnessStatWriter {
    pub fn new() -> Self {
        Self { writer: None }
    }

    pub fn write(&mut self, stat: StateWitnessStat) {
        let writer = if self.writer.is_none() {
            std::fs::create_dir_all("state_witness_stats").unwrap();
            let filename = format!("state_witness_stats/from_{}.json", stat.block_height);
            self.writer = Some(File::create(filename).unwrap());
            self.writer.as_mut().unwrap().write_all(b"[").unwrap();
            self.writer.as_mut().unwrap()
        } else {
            self.writer.as_mut().unwrap().write_all(b",").unwrap();
            self.writer.as_mut().unwrap()
        };
        writer.write_all(&serde_json::to_vec(&stat).unwrap()).unwrap();
        if writer.seek(SeekFrom::Current(0)).unwrap() > 100000000 {
            writer.write_all(b"]").unwrap();
            self.writer = None;
        }
    }
}

impl Drop for StateWitnessStatWriter {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.as_mut() {
            writer.write_all(b"]").unwrap();
        }
        self.writer = None;
    }
}
