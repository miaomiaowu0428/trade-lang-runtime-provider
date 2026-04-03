//! Trade-Lang Runtime Provider
//!
//! 提供策略运行器（RuntimeProvider）的多种部署模式实现。
//!
//! 核心抽象：
//! - [`MonitorSide`] — 运行 Monitor → 序列化上下文 → 发布 TaskEnvelope
//! - [`ExecutorSide`] — 接收 TaskEnvelope → 反序列化 → spawn TradePipeline
//!
//! 部署模式（基于核心抽象组合）：
//!
//! | 模块 | 说明 | 组合方式 |
//! |------|------|---------|
//! | [`local`] | 同进程单机 | MonitorSide + ExecutorSide + LocalTransport |
//! | [`decision`] | 分布式决策节点 | MonitorSide + TCP/QUIC Publisher |
//! | [`executor`] | 分布式操作节点 | ExecutorSide + TCP/QUIC Subscriber |
//!
//! ```text
//! 单机模式 (LocalRuntime):
//!   MonitorSide ──(mpsc channel)──→ ExecutorSide → TradePipeline
//!
//! 分布式模式:
//!   DecisionRuntime                         ExecutorRuntime (×N)
//!     MonitorSide ──→ TCP/QUIC ──→ ExecutorSide → TradePipeline
//! ```

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

// ── Re-export core ────────────────────────────────────────────────────────────
pub use trade_lang_core;
pub use trade_lang_core::{
    CancellationToken as _CT, ConditionHandler,
    DataItemHandler, ExecutorHandler, MonitorHandler, MonitorMessage, RuntimeRegistry,
    TradeTaskContext, monitor_mpsc,
};

// ── 核心模块 ──────────────────────────────────────────────────────────────────

pub mod pipeline;
pub mod transport;

pub mod executor_side;
pub mod monitor_side;

// ── 部署模式 ──────────────────────────────────────────────────────────────────

pub mod decision;
pub mod executor;
pub mod local;

pub mod builtins;

// ── Re-exports ────────────────────────────────────────────────────────────────

pub use builtins::register_builtins;
pub use decision::{DecisionRuntime, DecisionRuntimeBuilder};
pub use executor::{ExecutorRuntime, ExecutorRuntimeBuilder};
pub use executor_side::ExecutorSide;
pub use local::{LocalRuntime, LocalRuntimeBuilder, StrategyRunner};
pub use monitor_side::MonitorSide;
pub use transport::{
    ContextDeserializer, ContextSerializer, TaskEnvelope, TaskPublisher, TaskResult, TaskSubscriber,
};

// bincode re-export for use inside register_context! macro
#[doc(hidden)]
pub use bincode as _bincode;

/// 一键注册 context 类型的正反序列化器。
///
/// # 用法
/// ```ignore
/// let mut ser = HashMap::new();
/// let mut de  = HashMap::new();
/// trade_lang_runtime_provider::register_context!(ser, de, "my_protocol", MyCtxType);
/// ```
///
/// 要求 `MyCtxType` 实现 `serde::Serialize + serde::DeserializeOwned`（bincode 1.x 兼容）。
#[macro_export]
macro_rules! register_context {
    ($ser_map:expr, $de_map:expr, $key:literal, $ty:ty) => {
        $ser_map.insert(
            $key.to_string(),
            ::std::sync::Arc::new(
                |value: &::std::sync::Arc<dyn ::std::any::Any + Send + Sync>| {
                    value
                        .downcast_ref::<$ty>()
                        .and_then(|v| $crate::_bincode::serialize(v).ok())
                },
            ) as $crate::ContextSerializer,
        );
        $de_map.insert(
            $key.to_string(),
            ::std::sync::Arc::new(|bytes: &[u8]| {
                let v: $ty = $crate::_bincode::deserialize(bytes).ok()?;
                Some(
                    ::std::sync::Arc::new(v)
                        as ::std::sync::Arc<dyn ::std::any::Any + Send + Sync>,
                )
            }) as $crate::ContextDeserializer,
        );
    };
}

// ── RuntimeProvider trait ─────────────────────────────────────────────────────

/// 统一运行入口 — 业务代码通过该 trait 启动策略
///
/// 不同 RuntimeProvider 实现对应不同部署模式：
///   - `LocalRuntime`     — 单机（Monitor + Executor 同进程）
///   - `DecisionRuntime`  — 决策节点（只 Monitor → 网络广播）
///   - `ExecutorRuntime`  — 操作节点（网络接收 → Executor pipeline）
#[async_trait]
pub trait RuntimeProvider: Send + Sync {
    async fn run(
        &self,
        strategy_source: &str,
        cancel: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}
