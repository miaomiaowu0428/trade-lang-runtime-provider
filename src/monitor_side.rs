//! Monitor 侧核心逻辑
//!
//! 运行 Monitor，每次触发 → 序列化上下文 → 通过 [`TaskPublisher`] 发送 [`TaskEnvelope`]。
//!
//! 不关心传输方式，只依赖 `TaskPublisher` trait。
//! 由上层（LocalRuntime / 独立决策 binary）组装具体的 publisher 实现。

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use log::{error, info, warn};
use tokio_util::sync::CancellationToken;

use trade_meta_compiler::ast::Strategy;

use trade_lang_core::RuntimeRegistry;

use crate::pipeline::eval_named_args_static;
use crate::transport::{ContextSerializer, TaskEnvelope, TaskPublisher};

/// Monitor 侧运行器
pub struct MonitorSide {
    registry: Arc<RuntimeRegistry>,
    publisher: Arc<dyn TaskPublisher>,
    context_serializers: HashMap<String, ContextSerializer>,
    task_counter: AtomicU64,
}

impl MonitorSide {
    pub fn new(
        registry: Arc<RuntimeRegistry>,
        publisher: Arc<dyn TaskPublisher>,
        context_serializers: HashMap<String, ContextSerializer>,
    ) -> Self {
        Self {
            registry,
            publisher,
            context_serializers,
            task_counter: AtomicU64::new(0),
        }
    }

    /// 运行 Monitor 循环：触发 → 序列化 → 发布 envelope
    pub async fn run(
        &self,
        strategy: &Strategy,
        cancel: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let strategy_name = &strategy.name;
        let monitor_name = &strategy.monitor.monitor_call.name.name;
        let monitor_args = eval_named_args_static(&strategy.monitor.monitor_call.args);

        let monitor = self
            .registry
            .monitors
            .get(monitor_name)
            .ok_or_else(|| format!("Monitor '{}' not registered", monitor_name))?;
        let monitor = Arc::clone(monitor);

        info!(
            "[MonitorSide] Strategy {:?}, Monitor '{}' starting...",
            strategy_name, monitor_name
        );

        let mut rx = monitor.start(&monitor_args, cancel.clone()).await;

        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Some(msg) => {
                            let task_id = self.task_counter.fetch_add(1, Ordering::Relaxed) + 1;
                            info!(
                                "[MonitorSide] ★ Monitor '{}' triggered! Publishing task #{}",
                                monitor_name, task_id
                            );

                            let mut contexts = HashMap::new();
                            for (protocol, value) in &msg.contexts {
                                if let Some(serializer) = self.context_serializers.get(*protocol) {
                                    if let Some(bytes) = serializer(value) {
                                        contexts.insert(protocol.to_string(), bytes);
                                    } else {
                                        warn!(
                                            "[MonitorSide] Task #{}: failed to serialize context '{}'",
                                            task_id, protocol
                                        );
                                    }
                                } else {
                                    warn!(
                                        "[MonitorSide] Task #{}: no serializer for protocol '{}'",
                                        task_id, protocol
                                    );
                                }
                            }

                            let envelope = TaskEnvelope {
                                task_id,
                                strategy_name: strategy_name.clone(),
                                contexts,
                                sent_at_ns: 0, // 由 publisher.publish() 写入实际时间戳
                            };

                            if let Err(e) = self.publisher.publish(&envelope).await {
                                error!(
                                    "[MonitorSide] Failed to publish task #{}: {}",
                                    task_id, e
                                );
                            }
                        }
                        None => {
                            info!("[MonitorSide] Monitor channel closed, exiting");
                            break;
                        }
                    }
                }
                _ = cancel.cancelled() => {
                    info!("[MonitorSide] Received cancel signal, stopping");
                    break;
                }
            }
        }

        info!("[MonitorSide] Strategy {:?} stopped", strategy_name);
        Ok(())
    }
}
