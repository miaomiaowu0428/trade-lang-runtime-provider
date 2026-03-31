//! Executor 侧核心逻辑
//!
//! 通过 [`TaskSubscriber`] 接收 [`TaskEnvelope`]，查找本地策略，
//! 反序列化上下文后 spawn TradePipeline 执行 buy → sell → finally。
//!
//! 不关心传输方式，只依赖 `TaskSubscriber` trait。

use std::collections::HashMap;
use std::sync::Arc;

use log::{error, info, warn};
use tokio_util::sync::CancellationToken;

use trade_meta_compiler::ast::Strategy;

use trade_lang_core::{RuntimeRegistry, TradeTaskContext};

use crate::pipeline::{TradePipeline, init_vars};
use crate::transport::{ContextDeserializer, TaskEnvelope, TaskSubscriber};

/// Executor 侧运行器
pub struct ExecutorSide {
    registry: Arc<RuntimeRegistry>,
    strategies: HashMap<String, Strategy>,
    context_deserializers: HashMap<String, ContextDeserializer>,
}

impl ExecutorSide {
    pub fn new(
        registry: Arc<RuntimeRegistry>,
        strategies: HashMap<String, Strategy>,
        context_deserializers: HashMap<String, ContextDeserializer>,
    ) -> Self {
        Self {
            registry,
            strategies,
            context_deserializers,
        }
    }

    /// 运行 Executor 循环：接收 envelope → 查找策略 → spawn pipeline
    pub async fn run(
        &self,
        subscriber: &mut dyn TaskSubscriber,
        cancel: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "[ExecutorSide] Started, {} strategies loaded, waiting for tasks...",
            self.strategies.len()
        );

        loop {
            tokio::select! {
                envelope = subscriber.recv() => {
                    match envelope {
                        Some(envelope) => {
                            self.handle_task(envelope, &cancel).await;
                        }
                        None => {
                            warn!("[ExecutorSide] Transport disconnected");
                            break;
                        }
                    }
                }
                _ = cancel.cancelled() => {
                    info!("[ExecutorSide] Received cancel signal, stopping");
                    break;
                }
            }
        }

        info!("[ExecutorSide] Stopped");
        Ok(())
    }

    async fn handle_task(&self, envelope: TaskEnvelope, cancel: &CancellationToken) {
        let task_id = envelope.task_id;
        let strategy_name = &envelope.strategy_name;

        let strategy = match self.strategies.get(strategy_name) {
            Some(s) => s,
            None => {
                error!(
                    "[ExecutorSide] Task #{}: unknown strategy '{}', skipping",
                    task_id, strategy_name
                );
                return;
            }
        };

        info!(
            "[ExecutorSide] ★ Task #{} received for strategy '{}'",
            task_id, strategy_name
        );

        let ctx = Arc::new(TradeTaskContext::with_parent_cancel(cancel));
        init_vars(&ctx, &strategy.vars).await;

        // 反序列化并注入上下文
        {
            let mut contexts = ctx.contexts.write().await;
            for (protocol, bytes) in &envelope.contexts {
                if let Some(deserializer) = self.context_deserializers.get(protocol) {
                    if let Some(value) = deserializer(bytes) {
                        contexts.insert(protocol.clone(), value);
                    } else {
                        warn!(
                            "[ExecutorSide] Task #{}: failed to deserialize context '{}'",
                            task_id, protocol
                        );
                    }
                } else {
                    warn!(
                        "[ExecutorSide] Task #{}: no deserializer for protocol '{}'",
                        task_id, protocol
                    );
                }
            }
        }

        let pipeline = TradePipeline::new(Arc::clone(&self.registry), ctx);
        let on_trigger = strategy.monitor.on_trigger.clone();
        let tid = task_id;

        tokio::spawn(async move {
            pipeline.run(tid, &on_trigger).await;
        });
    }
}
