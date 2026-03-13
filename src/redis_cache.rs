use anyhow::{Context, Result};
use redis::AsyncCommands;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::config::RedisRuntimeConfig;

#[derive(Clone)]
pub struct RedisCache {
    client: redis::Client,
    runtime: RedisRuntimeConfig,
}

impl RedisCache {
    pub fn new(runtime: RedisRuntimeConfig) -> Result<Self> {
        let client = redis::Client::open(runtime.url.clone())
            .with_context(|| format!("初始化 Redis 客户端失败: {}", runtime.url))?;
        Ok(Self { client, runtime })
    }

    pub async fn ping(&self) -> Result<()> {
        let mut conn = self.connection().await?;
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
        let mut conn = self.connection().await?;
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

    pub async fn set_json<T>(&self, key: &str, value: &T, ttl_secs: u64) -> Result<()>
    where
        T: Serialize,
    {
        let payload = serde_json::to_string(value)
            .with_context(|| format!("Redis JSON 序列化失败: {key}"))?;
        let mut conn = self.connection().await?;
        let _: () = conn
            .set_ex(key, payload, ttl_secs)
            .await
            .with_context(|| format!("Redis SETEX 失败: {key}"))?;
        Ok(())
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let mut conn = self.connection().await?;
        let _: usize = conn
            .del(key)
            .await
            .with_context(|| format!("Redis DEL 失败: {key}"))?;
        Ok(())
    }

    pub async fn try_acquire_lock(&self, key: &str) -> Result<bool> {
        let mut conn = self.connection().await?;
        let result: Option<String> = redis::cmd("SET")
            .arg(key)
            .arg("1")
            .arg("NX")
            .arg("EX")
            .arg(self.runtime.lock_ttl_secs)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("Redis SET NX EX 失败: {key}"))?;
        Ok(result.is_some())
    }

    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .context("获取 Redis 连接失败")
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
