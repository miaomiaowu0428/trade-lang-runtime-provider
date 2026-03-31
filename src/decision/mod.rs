//! 分布式决策节点
//!
//! 只运行 Monitor，不执行任何交易。当 Monitor 触发时，将上下文数据序列化为
//! [`TaskEnvelope`] 并通过 [`TaskPublisher`] 广播给所有操作节点。
//!
//! 内部委托给 [`MonitorSide`] 执行核心逻辑。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use log::info;
use tokio_util::sync::CancellationToken;

use trade_meta_compiler::SymbolRegistry;

use trade_lang_core::RuntimeRegistry;

use crate::RuntimeProvider;
use crate::builtins::register_builtins;
use crate::monitor_side::MonitorSide;
use crate::transport::{ContextSerializer, TaskPublisher};

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// DecisionRuntime
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// 分布式决策节点：只跑 Monitor，触发后通过网络推送任务
pub struct DecisionRuntime {
    registry: Arc<RuntimeRegistry>,
    symbol_registry: SymbolRegistry,
    publisher: Arc<dyn TaskPublisher>,
    context_serializers: HashMap<String, ContextSerializer>,
}

impl DecisionRuntime {
    pub fn builder(publisher: Arc<dyn TaskPublisher>) -> DecisionRuntimeBuilder {
        DecisionRuntimeBuilder::new(publisher)
    }
}

pub struct DecisionRuntimeBuilder {
    symbol_registry: SymbolRegistry,
    registry: RuntimeRegistry,
    publisher: Arc<dyn TaskPublisher>,
    context_serializers: HashMap<String, ContextSerializer>,
}

impl DecisionRuntimeBuilder {
    pub fn new(publisher: Arc<dyn TaskPublisher>) -> Self {
        let mut symbol_registry = trade_meta_compiler::builtin_symbol_registry();
        symbol_registry.collect_from_inventory();
        let mut registry = RuntimeRegistry::new();
        register_builtins(&mut registry);
        Self {
            symbol_registry,
            registry,
            publisher,
            context_serializers: HashMap::new(),
        }
    }

    /// 注入 Monitor handler（决策节点只需要 Monitor，不需要 Executor）
    pub fn with_registry(mut self, other: RuntimeRegistry) -> Self {
        for (name, handler) in other.monitors {
            self.registry.monitors.insert(name, handler);
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

    /// 注册上下文序列化器（Monitor 产出的上下文 → bytes）
    pub fn with_context_serializer(
        mut self,
        protocol: impl Into<String>,
        serializer: ContextSerializer,
    ) -> Self {
        self.context_serializers.insert(protocol.into(), serializer);
        self
    }

    pub fn build(self) -> DecisionRuntime {
        DecisionRuntime {
            registry: Arc::new(self.registry),
            symbol_registry: self.symbol_registry,
            publisher: self.publisher,
            context_serializers: self.context_serializers,
        }
    }
}

#[async_trait]
impl RuntimeProvider for DecisionRuntime {
    async fn run(
        &self,
        strategy_source: &str,
        cancel: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use trade_meta_compiler::{Checker, StrategyParser};

        let ast = StrategyParser::new()
            .parse(strategy_source)
            .map_err(|e| format!("Parse error: {}", e))?;
        info!("[Decision] Parsed strategy: {:?}", ast.name);

        let mut checker = Checker::new(self.symbol_registry.clone());
        checker
            .check(&ast)
            .map_err(|e| format!("Semantic check failed: {:?}", e))?;
        info!("[Decision] Semantic check passed");

        // 委托给 MonitorSide 执行核心循环
        let monitor_side = MonitorSide::new(
            Arc::clone(&self.registry),
            Arc::clone(&self.publisher),
            self.context_serializers.clone(),
        );

        monitor_side.run(&ast, cancel).await
    }
}
