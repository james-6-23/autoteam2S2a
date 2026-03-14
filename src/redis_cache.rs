use anyhow::{Context, Result};
use rand::Rng;
use redis::AsyncCommands;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::config::RedisRuntimeConfig;

#[derive(Clone)]
pub struct RedisCache {
    conn: redis::aio::ConnectionManager,
    runtime: RedisRuntimeConfig,
}

impl RedisCache {
    pub async fn new(runtime: RedisRuntimeConfig) -> Result<Self> {
        let client = redis::Client::open(runtime.url.clone())
            .with_context(|| format!("初始化 Redis 客户端失败: {}", runtime.url))?;
        let conn = redis::aio::ConnectionManager::new(client)
            .await
            .context("创建 Redis ConnectionManager 失败")?;
        Ok(Self { conn, runtime })
    }

    pub async fn ping(&self) -> Result<()> {
        let mut conn = self.conn.clone();
        let _: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .context("Redis PING 失败")?;
        Ok(())
    }

    pub fn default_ttl_secs(&self) -> u64 {
        self.runtime.default_ttl_secs
    }

    pub fn batch_progress_ttl_secs(&self) -> u64 {
        self.runtime.batch_progress_ttl_secs
    }

    #[allow(dead_code)]
    pub fn lock_ttl_secs(&self) -> u64 {
        self.runtime.lock_ttl_secs
    }

    pub fn owner_health_key(&self, account_id: &str) -> String {
        self.key(["owner", "health", account_id])
    }

    pub fn owner_members_key(&self, account_id: &str) -> String {
        self.key(["owner", "members", account_id])
    }

    pub fn batch_job_key(&self, job_id: &str) -> String {
        self.key(["batch", "job", job_id])
    }

    pub fn health_lock_key(&self, account_id: &str) -> String {
        self.key(["lock", "health", account_id])
    }

    pub async fn get_json<T>(&self, key: &str) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let mut conn = self.conn.clone();
        let payload: Option<String> = conn
            .get(key)
            .await
            .with_context(|| format!("Redis GET 失败: {key}"))?;
        match payload {
            Some(payload) => {
                let value = serde_json::from_str::<T>(&payload)
                    .with_context(|| format!("Redis JSON 反序列化失败: {key}"))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// 批量读取多个 key，使用 MGET 一次网络往返
    pub async fn mget_json<T>(&self, keys: &[String]) -> Result<Vec<Option<T>>>
    where
        T: DeserializeOwned,
    {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut conn = self.conn.clone();
        let payloads: Vec<Option<String>> = redis::cmd("MGET")
            .arg(keys)
            .query_async(&mut conn)
            .await
            .context("Redis MGET 失败")?;
        let results = payloads
            .into_iter()
            .enumerate()
            .map(|(i, payload)| match payload {
                Some(s) => match serde_json::from_str::<T>(&s) {
                    Ok(v) => Some(v),
                    Err(e) => {
                        tracing::warn!(
                            "Redis MGET 反序列化失败: key={}, error={}",
                            keys[i],
                            e
                        );
                        None
                    }
                },
                None => None,
            })
            .collect();
        Ok(results)
    }

    pub async fn set_json<T>(&self, key: &str, value: &T, ttl_secs: u64) -> Result<()>
    where
        T: Serialize,
    {
        let payload = serde_json::to_string(value)
            .with_context(|| format!("Redis JSON 序列化失败: {key}"))?;
        let mut conn = self.conn.clone();
        let _: () = conn
            .set_ex(key, payload, ttl_secs)
            .await
            .with_context(|| format!("Redis SETEX 失败: {key}"))?;
        Ok(())
    }

    /// 批量写入，使用 Pipeline 一次网络往返
    #[allow(dead_code)]
    pub async fn pipeline_set_json<T>(&self, entries: &[(String, &T, u64)]) -> Result<()>
    where
        T: Serialize,
    {
        if entries.is_empty() {
            return Ok(());
        }
        let mut pipe = redis::pipe();
        for (key, value, ttl) in entries {
            let payload = serde_json::to_string(value)
                .with_context(|| format!("Redis Pipeline 序列化失败: {key}"))?;
            pipe.cmd("SETEX").arg(key).arg(*ttl).arg(payload);
        }
        let mut conn = self.conn.clone();
        pipe.query_async::<()>(&mut conn)
            .await
            .context("Redis Pipeline 执行失败")?;
        Ok(())
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        let _: usize = conn
            .del(key)
            .await
            .with_context(|| format!("Redis DEL 失败: {key}"))?;
        Ok(())
    }

    /// 获取分布式锁，锁值存储 owner 标识用于安全释放
    pub async fn try_acquire_lock(&self, key: &str, owner: &str) -> Result<bool> {
        let mut conn = self.conn.clone();
        let result: Option<String> = redis::cmd("SET")
            .arg(key)
            .arg(owner)
            .arg("NX")
            .arg("EX")
            .arg(self.runtime.lock_ttl_secs)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("Redis SET NX EX 失败: {key}"))?;
        Ok(result.is_some())
    }

    /// 使用 Lua CAS 脚本安全释放锁 — 只释放自己持有的锁
    pub async fn release_lock(&self, key: &str, owner: &str) -> Result<bool> {
        // Lua 脚本：若当前值 == owner 则 DEL，否则不操作
        let script = redis::Script::new(
            r#"
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("DEL", KEYS[1])
            else
                return 0
            end
            "#,
        );
        let mut conn = self.conn.clone();
        let deleted: i32 = script
            .key(key)
            .arg(owner)
            .invoke_async(&mut conn)
            .await
            .with_context(|| format!("Redis 释放锁失败: {key}"))?;
        Ok(deleted == 1)
    }

    /// 在 base TTL 基础上 ±10% 随机浮动，避免缓存雪崩
    #[allow(dead_code)]
    pub fn jittered_ttl(base: u64) -> u64 {
        if base == 0 {
            return 0;
        }
        let jitter_range = (base as f64 * 0.1).max(1.0) as u64;
        let mut rng = rand::rng();
        let offset = rng.random_range(0..=jitter_range * 2);
        base.saturating_sub(jitter_range).saturating_add(offset)
    }

    fn key<'a>(&self, segments: impl IntoIterator<Item = &'a str>) -> String {
        let mut key = self.runtime.key_prefix.clone();
        for segment in segments {
            key.push(':');
            key.push_str(segment);
        }
        key
    }
}
