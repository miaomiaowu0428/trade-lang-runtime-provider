//! 策略运行器（单机模式）
//!
//! `StrategyRunner` 管理 Monitor → 交易流程的生命周期：
//!
//! 1. 从 AST 提取 Monitor 名称和参数
//! 2. 启动 Monitor（独立于交易流程运行）
//! 3. 每次 Monitor 触发 → 创建新的 `TradeTaskContext` → spawn 交易流程
//!
//! ```text
//! StrategyRunner
//!   ├── monitor loop (长期运行)
//!   │     ├── 触发 → spawn TradePipeline #1
//!   │     ├── 触发 → spawn TradePipeline #2
//!   │     └── ...
//!   └── cancel: 外部可随时停止
//! ```

use std::sync::Arc;

use log::{debug, error, info};
use tokio_util::sync::CancellationToken;

use trade_meta_compiler::ast::*;

use trade_lang_core::{RuntimeRegistry, TradeTaskContext};

use crate::pipeline::{TradePipeline, eval_named_args_static, init_vars};

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// StrategyRunner
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// 策略运行器 — 单机模式下管理 Monitor → Pipeline 的完整生命周期
pub struct StrategyRunner {
    runtime: Arc<RuntimeRegistry>,
    cancel: CancellationToken,
}

impl StrategyRunner {
    pub fn new(runtime: RuntimeRegistry) -> Self {
        Self {
            runtime: Arc::new(runtime),
            cancel: CancellationToken::new(),
        }
    }

    pub fn new_with_cancel(runtime: &RuntimeRegistry, cancel: CancellationToken) -> Self {
        Self {
            runtime: Arc::new(RuntimeRegistry {
                data_items: runtime.data_items.clone(),
                executors: runtime.executors.clone(),
                conditions: runtime.conditions.clone(),
                monitors: runtime.monitors.clone(),
            }),
            cancel,
        }
    }

    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// 启动策略：进入 Monitor 循环，每次触发 → spawn TradePipeline
    pub async fn run(&self, strategy: &Strategy) {
        info!("");
        info!("══════════════════════════════════════════");
        info!("  Strategy {:?} started", strategy.name);
        info!("══════════════════════════════════════════");

        let monitor_name = &strategy.monitor.monitor_call.name.name;
        let monitor_args = eval_named_args_static(&strategy.monitor.monitor_call.args);

        let monitor = match self.runtime.monitors.get(monitor_name) {
            Some(m) => Arc::clone(m),
            None => {
                error!(
                    "  [FATAL] Monitor '{}' not registered in runtime",
                    monitor_name
                );
                return;
            }
        };

        info!(
            "  Monitor '{}' starting, waiting for triggers...",
            monitor_name
        );

        let mut rx = monitor.start(&monitor_args, self.cancel.clone()).await;
        let mut task_id: u64 = 0;

        // 热路优化：on_trigger 包含 4 个 Vec<BlockItem>，每次 trigger 深拷贝是不必要的。
        // 预先 Arc 包装，后续只做 Arc::clone（原子计数）。
        let on_trigger = Arc::new(strategy.monitor.on_trigger.clone());

        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Some(msg) => {
                            task_id += 1;

                            let mut ctx_inner =
                                TradeTaskContext::with_parent_cancel(&self.cancel);
                            // 把触发信号时刻透传给 ctx，pipeline 用于自动打点
                            ctx_inner.sig_time = msg.sig_time;
                            init_vars(&ctx_inner, &strategy.vars);
                            // Fix4: 直接赋值替换空 Vec，避免 write-lock + 循环 push
                            *ctx_inner.contexts.write() = msg.contexts;
                            let ctx = Arc::new(ctx_inner);

                            let pipeline = TradePipeline::new(
                                Arc::clone(&self.runtime),
                                ctx,
                            );
                            let on_trigger = Arc::clone(&on_trigger);
                            let tid = task_id;
                            // 热路径：先 spawn，再把日志放进 spawn 内部，避免
                            // 主 select! 循环被 format!/Display 同步阻塞，
                            // 影响下一次 rx.recv() 的 poll 时机。
                            let monitor_name_owned = monitor_name.clone();
                            tokio::spawn(async move {
                                info!("");
                                info!(
                                    "  ★ Monitor '{}' triggered! Spawning trade task #{}",
                                    monitor_name_owned, tid
                                );
                                pipeline.run(tid, &on_trigger).await;
                            });
                        }
                        None => {
                            info!("  [StrategyRunner] Monitor channel closed, exiting");
                            break;
                        }
                    }
                }
                _ = self.cancel.cancelled() => {
                    info!("  [StrategyRunner] Received cancel signal, stopping");
                    break;
                }
            }
        }

        info!("");
        info!("══════════════════════════════════════════");
        info!("  Strategy {:?} stopped", strategy.name);
        info!("══════════════════════════════════════════");
    }
}
