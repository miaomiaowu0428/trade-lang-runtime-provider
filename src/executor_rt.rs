//! 分布式操作节点
//!
//! 不运行 Monitor，只负责执行交易。通过 [`TaskSubscriber`] 接收
//! 决策节点推送的 [`TaskEnvelope`]，解析本地 .trade 文件，
//! 反序列化上下文后执行 buy → sell → finally 管线。
//!
//! ```text
//! ExecutorRuntime
//!   ├── 连接决策节点（TCP 长连接，断线自动重连）
//!   ├── 本地 .trade 文件仓库
//!   └── 收到 TaskEnvelope → 查找策略 → 注入上下文 → spawn TradePipeline
//! ```
//!
//! 操作节点持有私钥和 nonce 账户，独立发送交易。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use log::{error, info, warn};
use tokio_util::sync::CancellationToken;

use trade_meta_compiler::SymbolRegistry;
use trade_meta_compiler::ast::Strategy;

use trade_lang_core::{RuntimeRegistry, TradeTaskContext};

use crate::RuntimeProvider;
use crate::builtins::register_builtins;
use crate::pipeline::{TradePipeline, init_vars};
use crate::transport::{TaskEnvelope, TaskSubscriber};

/// 上下文反序列化器：将 bytes 还原为 `Arc<dyn Any + Send + Sync>`
///
/// 由 impl crate 提供，每个 protocol 注册一个反序列化函数。
pub type ContextDeserializer =
    Box<dyn Fn(&[u8]) -> Option<Arc<dyn std::any::Any + Send + Sync>> + Send + Sync>;

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// ExecutorRuntime
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// 分布式操作节点：接收任务，执行交易管线
pub struct ExecutorRuntime {
    registry: RuntimeRegistry,
    #[allow(dead_code)] // 后续扩展需要
    symbol_registry: SymbolRegistry,
    subscriber: tokio::sync::Mutex<Box<dyn TaskSubscriber>>,
    /// 本地策略仓库：策略名 → 预解析的 AST
    strategies: HashMap<String, Strategy>,
    /// protocol → 反序列化器
    context_deserializers: HashMap<String, ContextDeserializer>,
}

impl ExecutorRuntime {
    pub fn builder(subscriber: Box<dyn TaskSubscriber>) -> ExecutorRuntimeBuilder {
        ExecutorRuntimeBuilder::new(subscriber)
    }
}

pub struct ExecutorRuntimeBuilder {
    symbol_registry: SymbolRegistry,
    registry: RuntimeRegistry,
    subscriber: Box<dyn TaskSubscriber>,
    strategy_sources: Vec<(String, String)>,
    context_deserializers: HashMap<String, ContextDeserializer>,
}

impl ExecutorRuntimeBuilder {
    pub fn new(subscriber: Box<dyn TaskSubscriber>) -> Self {
        let mut symbol_registry = trade_meta_compiler::builtin_symbol_registry();
        symbol_registry.collect_from_inventory();
        let mut registry = RuntimeRegistry::new();
        register_builtins(&mut registry);
        Self {
            symbol_registry,
            registry,
            subscriber,
            strategy_sources: Vec::new(),
            context_deserializers: HashMap::new(),
        }
    }

    /// 注入 Executor handler（操作节点只需要 Executor，不需要 Monitor）
    pub fn with_registry(mut self, other: RuntimeRegistry) -> Self {
        for (name, handler) in other.executors {
            self.registry.executors.insert(name, handler);
        }
        for (name, handler) in other.conditions {
            self.registry.conditions.insert(name, handler);
        }
        for (name, handler) in other.data_items {
            self.registry.data_items.insert(name, handler);
        }
        self
    }

    pub fn with_symbols(mut self, symbols: SymbolRegistry) -> Self {
        self.symbol_registry.merge(symbols);
        self
    }

    /// 添加本地策略文件（策略名, DSL 源码）
    pub fn with_strategy(mut self, name: impl Into<String>, source: impl Into<String>) -> Self {
        self.strategy_sources.push((name.into(), source.into()));
        self
    }

    /// 注册上下文反序列化器
    pub fn with_context_deserializer(
        mut self,
        protocol: impl Into<String>,
        deserializer: ContextDeserializer,
    ) -> Self {
        self.context_deserializers
            .insert(protocol.into(), deserializer);
        self
    }

    pub fn build(self) -> Result<ExecutorRuntime, Box<dyn std::error::Error + Send + Sync>> {
        use trade_meta_compiler::{Checker, StrategyParser};

        let parser = StrategyParser::new();
        let mut strategies = HashMap::new();

        for (name, source) in &self.strategy_sources {
            let ast = parser
                .parse(source)
                .map_err(|e| format!("Parse error in strategy '{}': {}", name, e))?;

            let mut checker = Checker::new(self.symbol_registry.clone());
            checker
                .check(&ast)
                .map_err(|e| format!("Semantic check failed for '{}': {:?}", name, e))?;

            strategies.insert(name.clone(), ast);
        }

        Ok(ExecutorRuntime {
            registry: self.registry,
            symbol_registry: self.symbol_registry,
            subscriber: tokio::sync::Mutex::new(self.subscriber),
            strategies,
            context_deserializers: self.context_deserializers,
        })
    }
}

#[async_trait]
impl RuntimeProvider for ExecutorRuntime {
    async fn run(
        &self,
        _strategy_source: &str,
        cancel: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "[Executor] Started, {} strategies loaded, waiting for tasks...",
            self.strategies.len()
        );

        let mut subscriber = self.subscriber.lock().await;

        loop {
            tokio::select! {
                envelope = subscriber.recv() => {
                    match envelope {
                        Some(envelope) => {
                            self.handle_task(envelope, &cancel).await;
                        }
                        None => {
                            warn!("[Executor] Connection lost, will reconnect...");
                            // TODO: 重连逻辑 — 当前暂时退出，由外部重启
                            break;
                        }
                    }
                }
                _ = cancel.cancelled() => {
                    info!("[Executor] Received cancel signal, stopping");
                    break;
                }
            }
        }

        info!("[Executor] Stopped");
        Ok(())
    }
}

impl ExecutorRuntime {
    async fn handle_task(&self, envelope: TaskEnvelope, cancel: &CancellationToken) {
        let task_id = envelope.task_id;
        let strategy_name = &envelope.strategy_name;

        let strategy = match self.strategies.get(strategy_name) {
            Some(s) => s,
            None => {
                error!(
                    "[Executor] Task #{}: unknown strategy '{}', skipping",
                    task_id, strategy_name
                );
                return;
            }
        };

        info!(
            "[Executor] ★ Task #{} received for strategy '{}'",
            task_id, strategy_name
        );

        // 创建 TradeTaskContext 并注入反序列化后的上下文
        let ctx = Arc::new(TradeTaskContext::with_parent_cancel(cancel));
        init_vars(&ctx, &strategy.vars).await;

        {
            let mut contexts = ctx.contexts.write().await;
            for (protocol, bytes) in &envelope.contexts {
                if let Some(deserializer) = self.context_deserializers.get(protocol) {
                    if let Some(value) = deserializer(bytes) {
                        contexts.insert(protocol.clone(), value);
                    } else {
                        warn!(
                            "[Executor] Task #{}: failed to deserialize context '{}'",
                            task_id, protocol
                        );
                    }
                } else {
                    warn!(
                        "[Executor] Task #{}: no deserializer for protocol '{}'",
                        task_id, protocol
                    );
                }
            }
        }

        // Spawn pipeline
        let runtime = Arc::new(RuntimeRegistry {
            data_items: self.registry.data_items.clone(),
            executors: self.registry.executors.clone(),
            conditions: self.registry.conditions.clone(),
            monitors: self.registry.monitors.clone(),
            control_flows: self.registry.control_flows.clone(),
            all_calls: self.registry.all_calls.clone(),
        });

        let pipeline = TradePipeline::new(runtime, ctx);
        let on_trigger = strategy.monitor.on_trigger.clone();
        let tid = task_id;

        tokio::spawn(async move {
            pipeline.run(tid, &on_trigger).await;
        });
    }
}
