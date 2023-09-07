use std::path::Path;

use borsh::BorshSerialize;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{get_block_shard_uid, ShardLayout};
use near_store::flat::store_helper;
use near_store::{DBCol, ShardUId, Store};
use strum::IntoEnumIterator;

pub(crate) fn open_rocksdb(
    home: &Path,
    mode: near_store::Mode,
) -> anyhow::Result<near_store::db::RocksDB> {
    let config = nearcore::config::Config::from_file_skip_validation(
        &home.join(nearcore::config::CONFIG_FILENAME),
    )?;
    let store_config = &config.store;
    let db_path = store_config.path.as_ref().cloned().unwrap_or_else(|| home.join("data"));
    let rocksdb =
        near_store::db::RocksDB::open(&db_path, store_config, mode, near_store::Temperature::Hot)?;
    Ok(rocksdb)
}

pub fn sweat_shard() -> ShardUId {
    ShardLayout::get_simple_nightshade_layout().get_shard_uids()[3]
}

pub fn flat_head_state_root(store: &Store, shard_uid: &ShardUId) -> CryptoHash {
    let chunk: near_primitives::types::chunk_extra::ChunkExtra = store
        .get_ser(DBCol::ChunkExtra, &get_block_shard_uid(&flat_head(store, shard_uid), shard_uid))
        .unwrap()
        .unwrap();
    chunk.state_root().clone()
}

pub fn resolve_column(col: &str) -> Option<DBCol> {
    DBCol::iter().filter(|db_col| <&str>::from(db_col) == col).next()
}

pub fn flat_head(store: &Store, shard_uid: &ShardUId) -> CryptoHash {
    match store_helper::get_flat_storage_status(store, shard_uid).unwrap() {
        near_store::flat::FlatStorageStatus::Ready(status) => status.flat_head.hash,
        other => panic!("invalid flat storage status {other:?}"),
    }
}

pub fn flush_disk_cache() {
    eprintln!("Flush disk page cache");
    let mut cmd = std::process::Command::new("sudo")
        .arg("tee")
        .arg("/proc/sys/vm/drop_caches")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    use std::io::Write;
    cmd.stdin.as_mut().unwrap().write_all("3".as_bytes()).unwrap();
    cmd.wait().unwrap();
}
