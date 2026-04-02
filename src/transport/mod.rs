//! 任务传输层
//!
//! Monitor 侧通过 [`TaskPublisher`] 发布 [`TaskEnvelope`]，
//! Executor 侧通过 [`TaskSubscriber`] 接收。
//!
//! 内置三种传输实现：
//!   - [`local`]  — tokio mpsc channel（同进程）
//!   - [`tcp`]    — TCP 长连接 + length-prefix framing
//!   - [`quic`]   — QUIC 多路复用（推荐分布式部署）

pub mod local;
pub mod quic;
pub mod tcp;

use std::collections::HashMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

// ── TaskEnvelope ──────────────────────────────────────────────────────────────

/// 决策节点产出、操作节点消费的任务信封
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEnvelope {
    /// 全局递增任务 ID
    pub task_id: u64,
    /// 策略名（操作节点用它查找本地 .trade 文件）
    pub strategy_name: String,
    /// Monitor 产出的上下文数据，key = protocol name, value = 序列化后的 bytes
    pub contexts: HashMap<String, Vec<u8>>,
    /// 发布时的 Unix 纳秒时间戳（用于计算分发耗时）
    pub sent_at_ns: u64,
}

// ── TaskResult ────────────────────────────────────────────────────────────────

/// 操作节点执行完成后回报的结果（可选）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    pub task_id: u64,
    pub executor_id: String,
    pub success: bool,
    pub tx_signature: Option<String>,
    pub error: Option<String>,
}

// ── Transport traits ──────────────────────────────────────────────────────────

/// Monitor 侧：发布任务
#[async_trait]
pub trait TaskPublisher: Send + Sync {
    async fn publish(
        &self,
        envelope: &TaskEnvelope,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// Executor 侧：接收任务
#[async_trait]
pub trait TaskSubscriber: Send + Sync {
    /// 阻塞等待下一个任务（连接断开时返回 None）
    async fn recv(&mut self) -> Option<TaskEnvelope>;
}

// ── Context serialization ─────────────────────────────────────────────────────

/// 上下文序列化器：将 `Arc<dyn Any>` 转为 bytes
///
/// 由 impl crate 对每个 protocol 注册，MonitorSide 在 publish 前调用。
/// 使用 Arc 而非 Box，以便在 LocalRuntime 等场景中 clone 共享。
pub type ContextSerializer = std::sync::Arc<
    dyn Fn(&std::sync::Arc<dyn std::any::Any + Send + Sync>) -> Option<Vec<u8>> + Send + Sync,
>;

/// 上下文反序列化器：将 bytes 还原为 `Arc<dyn Any>`
///
/// 由 impl crate 对每个 protocol 注册，ExecutorSide 在 recv 后调用。
pub type ContextDeserializer = std::sync::Arc<
    dyn Fn(&[u8]) -> Option<std::sync::Arc<dyn std::any::Any + Send + Sync>> + Send + Sync,
>;

// ── 序列化辅助 ────────────────────────────────────────────────────────────────

/// 将 envelope 序列化为 length-prefix + bincode bytes (网络传输用)
pub fn encode_envelope(
    envelope: &TaskEnvelope,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let payload = bincode::serialize(envelope)?;
    let len = (payload.len() as u32).to_be_bytes();
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&len);
    buf.extend_from_slice(&payload);
    Ok(buf)
}

/// 从 reader 中读取一个 length-prefix + bincode 的 envelope
pub async fn decode_envelope<R: tokio::io::AsyncReadExt + Unpin>(
    reader: &mut R,
) -> Option<TaskEnvelope> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await.ok()?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 16 * 1024 * 1024 {
        // 16MB 上限，防止恶意/损坏数据
        return None;
    }
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await.ok()?;
    bincode::deserialize(&payload).ok()
}
