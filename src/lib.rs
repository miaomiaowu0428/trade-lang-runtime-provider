//! Trade-Lang Runtime Provider
//!
//! 提供策略运行器的单机模式实现：Monitor 触发后直接在同进程 spawn TradePipeline，
//! 不经序列化，不经网络传输。

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

// ── Re-export core ────────────────────────────────────────────────────────────
pub use trade_lang_core;
pub use trade_lang_core::{
    CancellationToken as _CT, ConditionHandler, DataItemHandler, ExecutorHandler, MonitorHandler,
    MonitorMessage, RuntimeRegistry, TradeTaskContext, monitor_mpsc,
};

// ── 核心模块 ──────────────────────────────────────────────────────────────────

pub mod builtins;
pub mod local;
pub mod pipeline;

// ── Re-exports ────────────────────────────────────────────────────────────────

pub use builtins::register_builtins;
pub use local::{LocalRuntime, LocalRuntimeBuilder, StrategyRunner};

// ── RuntimeProvider trait ─────────────────────────────────────────────────────

/// 统一运行入口 — 业务代码通过该 trait 启动策略
#[async_trait]
pub trait RuntimeProvider: Send + Sync {
    async fn run(
        &self,
        strategy_source: &str,
        cancel: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}
