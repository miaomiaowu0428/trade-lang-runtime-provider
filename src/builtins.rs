//! 语言内置 handler 注册
//!
//! 注册策略 DSL 必需的最小 handler 集合：
//!   - Done  — no-op executor stub（pipeline 按名称识别）
//!   - Spawn — 后台派生控制流
//!   - OneOf — 并发竞争控制流
//!   - All   — 全满足控制流
//!
//! 通用条件（Timeout 等）作为 plugin 由 impl crate 按需注册，不在此处自动注册。
//! 下游 impl crate 可覆盖 Spawn/OneOf 的注册（如带取消信号的 SolanaSpawnHandler）。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use trade_lang_core::{ExecutorHandler, RuntimeRegistry, TradeTaskContext};
use trade_meta_compiler::{RuntimeValue, TypeSpec};

use crate::control_flows::{AllHandler, OneOfHandler, SpawnHandler};

// ── Done (no-op stub) ─────────────────────────────────────────────────────────

/// No-op executor stub — Done 由 pipeline 按名称识别
struct BuiltinNoopExecutor;

#[async_trait]
impl ExecutorHandler for BuiltinNoopExecutor {
    fn declared_return_type(&self) -> Option<TypeSpec> {
        None
    }
    async fn execute(
        &self,
        _args: &HashMap<String, RuntimeValue>,
        _ctx: &Arc<TradeTaskContext>,
    ) -> Option<RuntimeValue> {
        None
    }
}

// ── 注册 ──────────────────────────────────────────────────────────────────────

/// 注册语言内置 handler（Done + 基础控制流）
pub fn register_builtins(registry: &mut RuntimeRegistry) {
    registry
        .executors
        .insert("Done".to_string(), Arc::new(BuiltinNoopExecutor));

    registry.register_control_flow("Spawn", Arc::new(SpawnHandler));
    registry.register_control_flow("OneOf", Arc::new(OneOfHandler));
    registry.register_all_call("All", Arc::new(AllHandler));
}
