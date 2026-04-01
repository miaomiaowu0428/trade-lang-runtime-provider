//! 语言内置 handler 实现
//!
//! 提供 DSL 控制流语句的默认实现，可按需注册：
//!   - SpawnHandler  — 后台派生（fire-and-forget）
//!   - OneOfHandler  — 并发竞争（first-wins）
//!   - AllHandler    — 并发全满足
//!   - Timeout       — 等待指定时长
//!
//! 下游 impl crate 可以替换这些实现（如带超时的 Spawn、加权 OneOf 等）。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{FuturesUnordered, StreamExt};
use log::info;

use trade_lang_core::{
    AllCallHandler, ControlFlowHandler, PipelineOps, RuntimeRegistry, TradeTaskContext
};
use trade_meta_compiler::RuntimeValue;
use trade_meta_compiler::ast::{Condition, ExecutorItem};

// ── Spawn ─────────────────────────────────────────────────────────────────────

/// 后台派生：顺序匹配分支，首个满足条件的分支执行后 signal_done
pub struct SpawnHandler;

#[async_trait]
impl ControlFlowHandler for SpawnHandler {
    async fn execute(
        &self,
        branches: &[(Condition, Vec<ExecutorItem>)],
        ops: Arc<dyn PipelineOps>,
    ) -> bool {
        let branches_owned = branches.to_vec();
        tokio::spawn(async move {
            for (cond, execs) in &branches_owned {
                if ops.is_done() {
                    break;
                }
                if ops.eval_condition(cond).await {
                    if ops.exec_executor_items(execs).await {
                        ops.signal_done();
                        break;
                    }
                }
            }
        });
        false
    }
}

// ── OneOf ─────────────────────────────────────────────────────────────────────

/// 并发竞争：所有分支并发评估条件，首个满足的执行
pub struct OneOfHandler;

#[async_trait]
impl ControlFlowHandler for OneOfHandler {
    async fn execute(
        &self,
        branches: &[(Condition, Vec<ExecutorItem>)],
        ops: Arc<dyn PipelineOps>,
    ) -> bool {
        let branches_owned = branches.to_vec();
        let mut futs: FuturesUnordered<_> = branches_owned
            .iter()
            .enumerate()
            .map(|(i, (cond, _))| {
                let ops = Arc::clone(&ops);
                let cond = cond.clone();
                async move { (i, ops.eval_condition(&cond).await) }
            })
            .collect();

        let mut winner: Option<usize> = None;
        while let Some((i, ok)) = futs.next().await {
            if ok {
                winner = Some(i);
                break;
            }
        }
        drop(futs);

        if let Some(idx) = winner {
            ops.exec_executor_items(&branches_owned[idx].1).await
        } else {
            false
        }
    }
}

// ── All ───────────────────────────────────────────────────────────────────────

/// 并发全满足：join_all 评估所有条件，全部 true 后执行
pub struct AllHandler;

#[async_trait]
impl AllCallHandler for AllHandler {
    async fn execute(
        &self,
        conditions: &[Condition],
        executors: &[ExecutorItem],
        ops: Arc<dyn PipelineOps>,
    ) -> bool {
        let futs: Vec<_> = conditions
            .iter()
            .map(|cond| {
                let ops = Arc::clone(&ops);
                let cond = cond.clone();
                async move { ops.eval_condition(&cond).await }
            })
            .collect();

        let results = futures::future::join_all(futs).await;
        if results.iter().all(|&r| r) {
            ops.exec_executor_items(executors).await
        } else {
            false
        }
    }
}

// ── Done (no-op stub) ─────────────────────────────────────────────────────────

/// No-op executor stub — Done 由 pipeline 按名称识别
struct BuiltinNoopExecutor;

#[async_trait]
impl trade_lang_core::ExecutorHandler for BuiltinNoopExecutor {
    fn declared_return_type(&self) -> Option<trade_meta_compiler::TypeSpec> {
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

// ── Timeout ───────────────────────────────────────────────────────────────────

/// Timeout condition — 等待指定时长后返回 true
struct TimeoutCondition;

#[async_trait]
impl trade_lang_core::ConditionHandler for TimeoutCondition {
    async fn evaluate(
        &self,
        args: &HashMap<String, RuntimeValue>,
        ctx: &Arc<TradeTaskContext>,
    ) -> bool {
        let secs = args.get("duration").map(|v| v.as_f64()).unwrap_or(0.0);

        if secs <= 0.0 {
            return true;
        }

        info!("  [Timeout] waiting {:.1}s ...", secs);

        let duration = std::time::Duration::from_secs_f64(secs);
        tokio::select! {
            _ = tokio::time::sleep(duration) => {
                info!("  [Timeout] {:.1}s elapsed", secs);
                true
            }
            _ = ctx.done_future() => {
                false
            }
        }
    }
}

// ── 注册 ──────────────────────────────────────────────────────────────────────

/// 注册语言内置 handler（Done, Spawn, OneOf, All, Timeout）
pub fn register_builtins(registry: &mut RuntimeRegistry) {
    // Done — no-op executor stub
    registry
        .executors
        .insert("Done".to_string(), Arc::new(BuiltinNoopExecutor));

    // 控制流 handler
    registry.register_control_flow("Spawn", Arc::new(SpawnHandler));
    registry.register_control_flow("OneOf", Arc::new(OneOfHandler));
    registry.register_all_call("All", Arc::new(AllHandler));

    // Timeout condition
    registry
        .conditions
        .insert("Timeout".to_string(), Arc::new(TimeoutCondition));
}
