//! 语言内置 handler 注册
//!
//! 注册策略 DSL 必需的最小 handler 集合：
//!   - Done  — no-op executor stub（pipeline 按名称识别）
//!   - Spawn — 接收 PreparedSpawnTask 并在 tokio 上 spawn 后台任务

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use trade_lang_core::{ExecutorHandler, RuntimeRegistry, TradeTaskContext};
use trade_meta_compiler::{RuntimeValue, TaskValue, TypeSpec};

use crate::pipeline::PreparedSpawnTask;

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

// ── Spawn executor ────────────────────────────────────────────────────────────

/// 内置 Spawn executor：接收 pipeline 组装好的 `PreparedSpawnTask`，
/// 在 tokio 上 spawn 后台任务。
///
/// pipeline 在处理 `Statement::Spawn` 时把 `PreparedSpawnTask` 包装为
/// `RuntimeValue::Task`，通过 args["task"] 传入此处。
struct BuiltinSpawnExecutor;

#[async_trait]
impl ExecutorHandler for BuiltinSpawnExecutor {
    fn declared_return_type(&self) -> Option<TypeSpec> {
        None
    }
    async fn execute(
        &self,
        args: &HashMap<String, RuntimeValue>,
        _ctx: &Arc<TradeTaskContext>,
    ) -> Option<RuntimeValue> {
        if let Some(RuntimeValue::Task(TaskValue(any))) = args.get("task") {
            if let Some(task) = any.clone().downcast::<PreparedSpawnTask>().ok() {
                tokio::spawn(async move { task.run().await });
            }
        }
        None
    }
}

// ── 注册 ──────────────────────────────────────────────────────────────────────

/// 注册语言内置 handler（Done、Spawn）
pub fn register_builtins(registry: &mut RuntimeRegistry) {
    registry
        .executors
        .insert("Done".to_string(), Arc::new(BuiltinNoopExecutor));
    registry
        .executors
        .insert("Spawn".to_string(), Arc::new(BuiltinSpawnExecutor));
}
