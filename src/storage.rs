use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use serde::Serialize;

pub fn save_json_records<T: Serialize>(prefix: &str, records: &[T]) -> Result<Option<PathBuf>> {
    if records.is_empty() {
        return Ok(None);
    }
    let dir = PathBuf::from("accounts");
    fs::create_dir_all(&dir)?;
    let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let file_path = dir.join(format!("{prefix}-{}-{ts}.json", records.len()));
    let data = serde_json::to_vec_pretty(records)?;
    fs::write(&file_path, data)?;
    Ok(Some(file_path))
}
