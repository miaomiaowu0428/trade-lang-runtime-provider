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
    AllCallHandler, ControlFlowHandler, CancellationToken as _CT, ConditionHandler,
    DataItemHandler, ExecutorHandler, MonitorHandler, MonitorMessage, PipelineOps, RuntimeRegistry,
    TradeTaskContext, monitor_mpsc,
};

// ── 核心模块 ──────────────────────────────────────────────────────────────────

pub mod pipeline;
pub mod transport;

pub mod control_flows;
pub mod conditions;

pub mod executor_side;
pub mod monitor_side;

// ── 部署模式 ──────────────────────────────────────────────────────────────────

pub mod decision;
pub mod executor;
pub mod local;

pub mod builtins;

// ── Re-exports ────────────────────────────────────────────────────────────────

pub use builtins::register_builtins;
pub use conditions::TimeoutCondition;
pub use control_flows::{AllHandler, OneOfHandler, SpawnHandler};
pub use decision::{DecisionRuntime, DecisionRuntimeBuilder};
pub use executor::{ExecutorRuntime, ExecutorRuntimeBuilder};
pub use executor_side::ExecutorSide;
pub use local::{LocalRuntime, LocalRuntimeBuilder, StrategyRunner};
pub use monitor_side::MonitorSide;
pub use transport::{
    ContextDeserializer, ContextSerializer, TaskEnvelope, TaskPublisher, TaskResult, TaskSubscriber,
};

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
