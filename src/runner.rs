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

use log::{error, info};
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
                control_flows: runtime.control_flows.clone(),
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

        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Some(msg) => {
                            task_id += 1;
                            info!("");
                            info!(
                                "  ★ Monitor '{}' triggered! Spawning trade task #{}",
                                monitor_name, task_id
                            );

                            let ctx = Arc::new(TradeTaskContext::with_parent_cancel(&self.cancel));
                            init_vars(&ctx, &strategy.vars).await;

                            {
                                let mut contexts = ctx.contexts.write().await;
                                for (protocol, value) in msg.contexts {
                                    contexts.insert(protocol.to_string(), value);
                                }
                            }

                            let pipeline = TradePipeline::new(
                                Arc::clone(&self.runtime),
                                ctx,
                            );
                            let on_trigger = strategy.monitor.on_trigger.clone();
                            let tid = task_id;
                            tokio::spawn(async move {
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
