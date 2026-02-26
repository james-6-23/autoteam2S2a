use std::time::Duration;

use anyhow::{Result, bail};
use serde::Deserialize;

use crate::config::D1CleanupConfig;

const D1_API_BASE: &str = "https://api.cloudflare.com/client/v4/accounts";

/// D1 查询响应
#[derive(Debug, Deserialize)]
struct D1Response {
    success: bool,
    result: Option<Vec<D1ResultItem>>,
    errors: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct D1ResultItem {
    results: Option<Vec<serde_json::Value>>,
    meta: Option<D1Meta>,
}

#[derive(Debug, Deserialize)]
struct D1Meta {
    changes: Option<u64>,
}

struct D1Client {
    http: rquest::Client,
    account_id: String,
    api_key: String,
}

impl D1Client {
    fn new(account_id: &str, api_key: &str) -> Result<Self> {
        let http = rquest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self {
            http,
            account_id: account_id.to_string(),
            api_key: api_key.to_string(),
        })
    }

    /// 执行 D1 SQL 查询
    async fn query(&self, db_id: &str, sql: &str) -> Result<D1Response> {
        let url = format!(
            "{}/{}/d1/database/{}/query",
            D1_API_BASE, self.account_id, db_id
        );
        let resp = self
            .http
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({ "sql": sql }))
            .send()
            .await?;

        let data: D1Response = resp.json().await?;
        if !data.success {
            bail!(
                "D1 query error: {}",
                data.errors
                    .as_ref()
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            );
        }
        Ok(data)
    }

    /// 获取表行数
    async fn get_count(&self, db_id: &str, table: &str) -> Result<u64> {
        let data = self
            .query(db_id, &format!("SELECT COUNT(*) as cnt FROM {table};"))
            .await?;
        if let Some(results) = data.result.and_then(|r| r.into_iter().next()) {
            if let Some(rows) = results.results {
                if let Some(row) = rows.first() {
                    if let Some(cnt) = row.get("cnt").and_then(|v| v.as_u64()) {
                        return Ok(cnt);
                    }
                }
            }
        }
        Ok(0)
    }

    /// 获取表的列信息
    async fn get_columns(&self, db_id: &str, table: &str) -> Result<Vec<String>> {
        let data = self
            .query(db_id, &format!("PRAGMA table_info({table});"))
            .await?;
        let mut cols = Vec::new();
        if let Some(results) = data.result.and_then(|r| r.into_iter().next()) {
            if let Some(rows) = results.results {
                for row in rows {
                    if let Some(name) = row.get("name").and_then(|v| v.as_str()) {
                        cols.push(name.to_string());
                    }
                }
            }
        }
        Ok(cols)
    }

    /// 批量删除最老的数据，保留 keep_percent 比例的最新数据
    async fn clean_table(
        &self,
        db_id: &str,
        db_name: &str,
        table: &str,
        keep_percent: f64,
        batch_size: usize,
    ) -> Result<()> {
        let total = self.get_count(db_id, table).await?;
        if total == 0 {
            println!("  [{table}] 已为空，跳过清理");
            return Ok(());
        }

        let keep_count = ((total as f64 * keep_percent) as u64).max(1);
        let delete_target = total.saturating_sub(keep_count);

        if delete_target == 0 {
            println!("  [{table}] {total} 行，数量过少无需清理");
            return Ok(());
        }

        println!(
            "  [{table}] 总计: {total}, 删除: {delete_target} ({:.0}%), 保留: {keep_count} ({:.0}%)",
            (1.0 - keep_percent) * 100.0,
            keep_percent * 100.0
        );

        // 获取主键列名
        let columns = self.get_columns(db_id, table).await?;
        let pk_col = columns.first().map(|s| s.as_str()).unwrap_or("rowid");

        let mut deleted = 0u64;
        let mut batch = 0u32;
        let mut remaining = delete_target;

        while remaining > 0 {
            batch += 1;
            let current_batch = (batch_size as u64).min(remaining);
            let sql = format!(
                "DELETE FROM {table} WHERE {pk_col} IN (SELECT {pk_col} FROM {table} ORDER BY {pk_col} ASC LIMIT {current_batch});"
            );

            match self.query(db_id, &sql).await {
                Ok(data) => {
                    let changes = data
                        .result
                        .and_then(|r| r.into_iter().next())
                        .and_then(|item| item.meta)
                        .and_then(|m| m.changes)
                        .unwrap_or(0);

                    deleted += changes;
                    remaining = remaining.saturating_sub(changes);
                    println!("    batch {batch}: 删除 {changes} (剩余待删: {remaining})");
                    if changes == 0 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
                Err(e) => {
                    println!("    batch {batch}: 失败 - {e}");
                    break;
                }
            }
        }

        let final_count = self.get_count(db_id, table).await?;
        println!("  [{table}] 完成: 删除 {deleted}, 剩余 {final_count}");
        let _ = db_name; // 保留参数用于日志
        Ok(())
    }
}

/// 主入口：遍历所有配置的 D1 数据库，执行清理和索引创建
pub async fn run_cleanup(config: &D1CleanupConfig) -> Result<()> {
    let account_id = config.account_id.as_deref().unwrap_or_default();
    let api_key = config.api_key.as_deref().unwrap_or_default();

    if account_id.is_empty() || api_key.is_empty() {
        bail!("D1 清理配置缺少 account_id 或 api_key");
    }

    let databases = config.databases.as_deref().unwrap_or_default();
    if databases.is_empty() {
        println!("  D1 清理: 未配置目标数据库，跳过");
        return Ok(());
    }

    let keep_percent = config.keep_percent.unwrap_or(0.1).clamp(0.01, 0.99);
    let batch_size = config.batch_size.unwrap_or(5000).max(100);

    let client = D1Client::new(account_id, api_key)?;

    // 清理阶段
    println!("\n--- D1 清理 ---");
    for db in databases {
        println!("\n[{}]", db.name);
        if let Err(e) = client
            .clean_table(&db.id, &db.name, "email", keep_percent, batch_size)
            .await
        {
            println!("  清理 {} 失败: {e}", db.name);
        }
    }

    println!("\nD1 清理完成!");
    Ok(())
}
