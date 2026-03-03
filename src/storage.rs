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
    // 性能优先：使用紧凑 JSON，减少序列化 CPU 与写盘体积
    let data = serde_json::to_vec(records)?;
    fs::write(&file_path, data)?;
    Ok(Some(file_path))
}
