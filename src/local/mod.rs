//! 单机运行模式
//!
//! Monitor 和 Executor 在同一进程内，通过 tokio mpsc channel 直接传递
//! Arc<dyn Any>，零序列化开销。

mod runner;
pub use runner::StrategyRunner;

use std::sync::Arc;

use async_trait::async_trait;
use log::info;
use tokio_util::sync::CancellationToken;

use trade_meta_compiler::SymbolRegistry;

use trade_lang_core::RuntimeRegistry;

use crate::RuntimeProvider;
use crate::builtins::register_builtins;

// ════════════════════════════════════════════════════════════════════════════════
// LocalRuntime
// ════════════════════════════════════════════════════════════════════════════════

/// 单机运行器：Monitor 和 Executor 在同一进程内，直接传递 Arc<dyn Any>
pub struct LocalRuntime {
    registry: Arc<RuntimeRegistry>,
    symbol_registry: SymbolRegistry,
}

impl LocalRuntime {
    pub fn builder() -> LocalRuntimeBuilder {
        LocalRuntimeBuilder::new()
    }
}

pub struct LocalRuntimeBuilder {
    symbol_registry: SymbolRegistry,
    registry: RuntimeRegistry,
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

    pub fn build(self) -> LocalRuntime {
        LocalRuntime {
            registry: Arc::new(self.registry),
            symbol_registry: self.symbol_registry,
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
        checker.check(&ast).map_err(|e| format!("Semantic check failed: {:?}", e))?;
        info!("[Local] Semantic check passed");

        if let Err(errors) = self.registry.validate_against(&self.symbol_registry) {
            for e in &errors {
                log::warn!("  Runtime validation: {}", e);
            }
        }

        let runner = StrategyRunner::new_with_cancel(&self.registry, cancel);
        runner.run(&ast).await;
        Ok(())
    }
}
