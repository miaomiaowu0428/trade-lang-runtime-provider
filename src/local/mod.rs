//! 单机运行模式
//!
//! Monitor 和 Executor 在同一进程内，通信方式取决于是否注册了上下文编解码器：
//!
//! - **未注册编解码器**（默认）：走 `StrategyRunner` 直连模式（零序列化开销）
//! - **已注册编解码器**：走 `MonitorSide → LocalTransport → ExecutorSide` 通道模式
//!
//! ```ignore
//! let runtime = LocalRuntime::builder()
//!     .with_registry(trade_solana_impl::create_registry())
//!     .build();
//! runtime.run(&dsl, cancel).await?;
//! ```

mod runner;
pub use runner::StrategyRunner;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use log::info;
use tokio_util::sync::CancellationToken;

use trade_meta_compiler::SymbolRegistry;

use trade_lang_core::RuntimeRegistry;

use crate::RuntimeProvider;
use crate::builtins::register_builtins;
use crate::executor_side::ExecutorSide;
use crate::monitor_side::MonitorSide;
use crate::transport::local::local_transport;
use crate::transport::{ContextDeserializer, ContextSerializer};

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// LocalRuntime
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// 单机运行器：Monitor 和 Executor 在同一进程内
pub struct LocalRuntime {
    registry: Arc<RuntimeRegistry>,
    symbol_registry: SymbolRegistry,
    context_serializers: HashMap<String, ContextSerializer>,
    context_deserializers: HashMap<String, ContextDeserializer>,
}

impl LocalRuntime {
    pub fn builder() -> LocalRuntimeBuilder {
        LocalRuntimeBuilder::new()
    }
}

pub struct LocalRuntimeBuilder {
    symbol_registry: SymbolRegistry,
    registry: RuntimeRegistry,
    context_serializers: HashMap<String, ContextSerializer>,
    context_deserializers: HashMap<String, ContextDeserializer>,
}

impl LocalRuntimeBuilder {
    pub fn new() -> Self {
        let mut symbol_registry = trade_meta_compiler::builtin_symbol_registry();
        symbol_registry.collect_from_inventory();
        let mut registry = RuntimeRegistry::new();
        register_builtins(&mut registry);
        Self {
            symbol_registry,
            registry,
            context_serializers: HashMap::new(),
            context_deserializers: HashMap::new(),
        }
    }

    pub fn with_symbols(mut self, symbols: SymbolRegistry) -> Self {
        self.symbol_registry.merge(symbols);
        self
    }

    pub fn with_registry(mut self, other: RuntimeRegistry) -> Self {
        for (name, handler) in other.data_items {
            self.registry.data_items.insert(name, handler);
        }
        for (name, handler) in other.executors {
            self.registry.executors.insert(name, handler);
        }
        for (name, handler) in other.conditions {
            self.registry.conditions.insert(name, handler);
        }
        for (name, handler) in other.monitors {
            self.registry.monitors.insert(name, handler);
        }
        self
    }

    /// 注册上下文序列化/反序列化器对
    ///
    /// 注册后 local 模式走通道路径（MonitorSide → LocalChannel → ExecutorSide），
    /// 未注册则走直连路径（StrategyRunner，零序列化）。
    pub fn with_context_codec(
        mut self,
        protocol: impl Into<String>,
        serializer: ContextSerializer,
        deserializer: ContextDeserializer,
    ) -> Self {
        let protocol = protocol.into();
        self.context_serializers
            .insert(protocol.clone(), serializer);
        self.context_deserializers.insert(protocol, deserializer);
        self
    }

    pub fn build(self) -> LocalRuntime {
        LocalRuntime {
            registry: Arc::new(self.registry),
            symbol_registry: self.symbol_registry,
            context_serializers: self.context_serializers,
            context_deserializers: self.context_deserializers,
        }
    }
}

#[async_trait]
impl RuntimeProvider for LocalRuntime {
    async fn run(
        &self,
        strategy_source: &str,
        cancel: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use trade_meta_compiler::{Checker, StrategyParser};

        let ast = StrategyParser::new()
            .parse(strategy_source)
            .map_err(|e| format!("Parse error: {}", e))?;
        info!("[Local] Parsed strategy: {:?}", ast.name);

        let mut checker = Checker::new(self.symbol_registry.clone());
        checker
            .check(&ast)
            .map_err(|e| format!("Semantic check failed: {:?}", e))?;
        info!("[Local] Semantic check passed");

        if let Err(errors) = self.registry.validate_against(&self.symbol_registry) {
            for e in &errors {
                log::warn!("  Runtime validation: {}", e);
            }
        }

        if self.context_serializers.is_empty() {
            // 直连模式：零序列化开销，直接 spawn pipeline
            info!("[Local] Running in direct mode (no context codecs)");
            let runner = StrategyRunner::new_with_cancel(&self.registry, cancel);
            runner.run(&ast).await;
        } else {
            // 通道模式：MonitorSide → LocalTransport → ExecutorSide
            info!("[Local] Running in transport mode (MonitorSide→Channel→ExecutorSide)");

            let (publisher, mut subscriber) = local_transport(64);

            // ContextSerializer/ContextDeserializer 是 Arc<dyn Fn>，可以 clone
            let serializers = self.context_serializers.clone();
            let deserializers = self.context_deserializers.clone();

            let mut strategies = HashMap::new();
            strategies.insert(ast.name.clone(), ast.clone());

            let executor_side =
                ExecutorSide::new(Arc::clone(&self.registry), strategies, deserializers);

            let publisher: Arc<dyn crate::transport::TaskPublisher> = Arc::new(publisher);
            let monitor_side = MonitorSide::new(Arc::clone(&self.registry), publisher, serializers);

            let exec_cancel = cancel.clone();
            let exec_task = tokio::spawn(async move {
                let subscriber: &mut dyn crate::transport::TaskSubscriber = &mut subscriber;
                let _ = executor_side.run(subscriber, exec_cancel).await;
            });

            monitor_side.run(&ast, cancel).await?;
            let _ = exec_task.await;
        }

        Ok(())
    }
}
