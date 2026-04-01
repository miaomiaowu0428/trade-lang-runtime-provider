//! 分布式操作节点
//!
//! 不运行 Monitor，只负责执行交易。通过 [`TaskSubscriber`] 接收
//! 决策节点推送的 [`TaskEnvelope`]，反序列化上下文后执行 buy → sell → finally 管线。
//!
//! 内部委托给 [`ExecutorSide`] 执行核心逻辑。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use log::info;
use tokio_util::sync::CancellationToken;

use trade_meta_compiler::SymbolRegistry;
use trade_meta_compiler::ast::Strategy;

use trade_lang_core::RuntimeRegistry;

use crate::RuntimeProvider;
use crate::builtins::register_builtins;
use crate::executor_side::ExecutorSide;
use crate::transport::{ContextDeserializer, TaskSubscriber};

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// ExecutorRuntime
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// 分布式操作节点：接收任务，执行交易管线
pub struct ExecutorRuntime {
    registry: Arc<RuntimeRegistry>,
    #[allow(dead_code)]
    symbol_registry: SymbolRegistry,
    subscriber: tokio::sync::Mutex<Box<dyn TaskSubscriber>>,
    strategies: HashMap<String, Strategy>,
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
        for (name, handler) in other.control_flows {
            self.registry.control_flows.insert(name, handler);
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
            registry: Arc::new(self.registry),
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
            "[Executor] Started, {} strategies loaded",
            self.strategies.len()
        );

        // 委托给 ExecutorSide 执行核心循环
        let executor_side = ExecutorSide::new(
            Arc::clone(&self.registry),
            self.strategies.clone(),
            self.context_deserializers.clone(),
        );

        let mut subscriber = self.subscriber.lock().await;
        executor_side.run(&mut **subscriber, cancel).await
    }
}
