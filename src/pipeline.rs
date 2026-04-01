//! 交易执行管线
//!
//! `TradePipeline` 负责单次交易流程的执行（buy → sell → finally），
//! 由各种 Runtime 在收到触发信号后创建并 spawn。
//!
//! 该模块被所有 Runtime 模式共享：
//!   - LocalRuntime：monitor 触发后直接创建 pipeline
//!   - ExecutorRuntime：收到 TaskEnvelope 后创建 pipeline

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use log::{info, warn};

use trade_meta_compiler::RuntimeValue;
use trade_meta_compiler::ast::*;

use trade_lang_core::{RuntimeRegistry, TradeTaskContext};

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// TradePipeline — 每次触发后的交易执行流程
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// 单次交易流程执行器（buy → sell → finally）
///
/// 每个 `TradePipeline` 拥有独立的 `TradeTaskContext`，包含 Monitor 产出的上下文。
#[derive(Clone)]
pub struct TradePipeline {
    pub runtime: Arc<RuntimeRegistry>,
    pub ctx: Arc<TradeTaskContext>,
}

impl TradePipeline {
    pub fn new(runtime: Arc<RuntimeRegistry>, ctx: Arc<TradeTaskContext>) -> Self {
        Self { runtime, ctx }
    }

    /// 执行完整的交易流程
    pub async fn run(&self, task_id: u64, trigger: &TriggerBody) {
        info!("  [Task#{}] ─── buy ───", task_id);
        let buy_ok = self.exec_buy(&trigger.buy).await;

        if buy_ok {
            info!("  [Task#{}] ─── sell ───", task_id);
            for stmt in &trigger.sell {
                if self.ctx.is_done() {
                    info!("  [Task#{}] Done signal, exiting sell", task_id);
                    break;
                }
                if self.exec_statement(stmt).await {
                    break;
                }
            }
        } else {
            warn!("  [Task#{}] buy failed, skipping sell", task_id);
        }

        if !trigger.sell_finally.is_empty() {
            info!("  [Task#{}] ─── finally ───", task_id);
            self.exec_executor_items(&trigger.sell_finally).await;
        }

        info!("  [Task#{}] Trade pipeline finished", task_id);
        self.ctx.signal_done();
    }

    // ── Buy spec ──────────────────────────────────────────────────────────────

    async fn exec_buy(&self, buy: &BuySpec) -> bool {
        match buy {
            BuySpec::Direct(call) => match self.exec_call(call).await {
                Some(v) => {
                    info!("    buy result: {:?}", v);
                    true
                }
                None => false,
            },
            BuySpec::Destructure { targets, value } => match value {
                DataExpr::Call(call) => match self.exec_call(call).await {
                    Some(rv) => {
                        self.destructure(targets, rv).await;
                        true
                    }
                    None => false,
                },
                _ => {
                    let rv = self.eval_expr(value).await;
                    self.destructure(targets, rv).await;
                    true
                }
            },
            BuySpec::List(items) => {
                let mut any_ok = false;
                for item in items {
                    match item {
                        BuyItem::Direct(call) => match self.exec_call(call).await {
                            Some(v) => {
                                info!("    buy item result: {:?}", v);
                                any_ok = true;
                            }
                            None => {
                                warn!("    buy item '{}' failed", call.name.name);
                            }
                        },
                        BuyItem::Destructure { targets, value } => match value {
                            DataExpr::Call(call) => match self.exec_call(call).await {
                                Some(rv) => {
                                    self.destructure(targets, rv).await;
                                    any_ok = true;
                                }
                                None => {
                                    warn!(
                                        "    buy item '{}' failed, vars set to uninit",
                                        call.name.name
                                    );
                                    self.destructure_uninit(targets).await;
                                }
                            },
                            _ => {
                                let rv = self.eval_expr(value).await;
                                self.destructure(targets, rv).await;
                                any_ok = true;
                            }
                        },
                    }
                }
                any_ok
            }
        }
    }

    // ── Statement execution ───────────────────────────────────────────────────

    fn exec_statement<'a>(
        &'a self,
        stmt: &'a Statement,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        Box::pin(async move {
            match stmt {
                Statement::LetAssign { var_name, value } => {
                    let v = self.eval_expr(value).await;
                    self.ctx.set_var(var_name, v).await;
                    false
                }
                Statement::LetDestructure { targets, value } => {
                    let rv = self.eval_expr(value).await;
                    self.destructure(targets, rv).await;
                    false
                }
                Statement::Executor { call } => {
                    self.exec_call(call).await;
                    false
                }
                Statement::ConditionExec {
                    condition,
                    executors,
                } => {
                    if self.eval_condition(condition).await {
                        self.exec_executor_items(executors).await
                    } else {
                        false
                    }
                }
                Statement::Spawn { items } => {
                    let items = items.clone();
                    let child = self.clone();
                    tokio::spawn(async move {
                        child.exec_executor_items(&items).await;
                    });
                    false
                }
            }
        })
    }

    // ── ExecutorItem 序列 ─────────────────────────────────────────────────────

    async fn exec_executor_items(&self, items: &[ExecutorItem]) -> bool {
        for item in items {
            if self.ctx.is_done() {
                return true;
            }
            match item {
                ExecutorItem::LetAssign { var_name, value } => {
                    let v = self.eval_expr(value).await;
                    self.ctx.set_var(var_name, v).await;
                }
                ExecutorItem::LetDestructure { targets, value } => {
                    let rv = self.eval_expr(value).await;
                    self.destructure(targets, rv).await;
                }
                ExecutorItem::Executor(ec) => {
                    let name = ec.executor.name.as_str();
                    if name == "Done" {
                        self.ctx.signal_done();
                        return true;
                    }
                    self.exec_executor_call(ec).await;
                }
            }
        }
        false
    }

    // ── Call 分派 ─────────────────────────────────────────────────────────────

    async fn exec_call(&self, call: &CallExpr) -> Option<RuntimeValue> {
        let name = &call.name.name;
        let args = self.eval_named_args(&call.args).await;

        if let Some(handler) = self.runtime.executors.get(name.as_str()) {
            handler.execute(&args, &self.ctx).await
        } else if let Some(handler) = self.runtime.conditions.get(name.as_str()) {
            let ok = handler.evaluate(&args, &self.ctx).await;
            Some(RuntimeValue::Bool(ok))
        } else if let Some(handler) = self.runtime.data_items.get(name.as_str()) {
            Some(handler.get(&args, &self.ctx).await)
        } else {
            warn!("    [warn] '{}' not found in runtime", name);
            None
        }
    }

    async fn exec_executor_call(&self, ec: &ExecutorCall) -> Option<RuntimeValue> {
        let name = ec.executor.name.as_str();
        let args = self.eval_hashmap_args(&ec.args).await;
        if let Some(handler) = self.runtime.executors.get(name) {
            handler.execute(&args, &self.ctx).await
        } else if let Some(handler) = self.runtime.data_items.get(name) {
            Some(handler.get(&args, &self.ctx).await)
        } else {
            warn!("    [warn] executor/data '{}' not registered", name);
            None
        }
    }

    // ── Args evaluation ───────────────────────────────────────────────────────

    async fn eval_named_args(&self, args: &[NamedArg]) -> HashMap<String, RuntimeValue> {
        let mut map = HashMap::new();
        for arg in args {
            let v = self.eval_expr(&arg.value).await;
            map.insert(arg.name.clone(), v);
        }
        map
    }

    async fn eval_hashmap_args(
        &self,
        args: &HashMap<String, DataExpr>,
    ) -> HashMap<String, RuntimeValue> {
        let mut map = HashMap::new();
        for (k, v) in args {
            map.insert(k.clone(), self.eval_expr(v).await);
        }
        map
    }

    // ── Condition 评估 ────────────────────────────────────────────────────────

    fn eval_condition<'a>(
        &'a self,
        cond: &'a Condition,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        Box::pin(async move {
            match cond {
                Condition::Default => true,
                Condition::Compare { left, op, right } => {
                    let l = self.eval_expr(left).await;
                    let r = self.eval_expr(right).await;
                    // Uninit 特殊处理：== uninit / != uninit
                    match (l.is_uninit(), r.is_uninit()) {
                        (true, true) => matches!(op, CompareOp::Eq | CompareOp::Le | CompareOp::Ge),
                        (true, false) | (false, true) => matches!(op, CompareOp::Ne),
                        (false, false) => apply_compare_op(*op, l.as_f64(), r.as_f64()),
                    }
                }
                Condition::Call(call) => {
                    if let Some(handler) = self.runtime.conditions.get(call.name.name.as_str()) {
                        let args = self.eval_named_args(&call.args).await;
                        handler.evaluate(&args, &self.ctx).await
                    } else {
                        false
                    }
                }
                Condition::Combinator { name, conditions } => {
                    match name.as_str() {
                        "All" => {
                            let futs: Vec<_> = conditions
                                .iter()
                                .map(|c| {
                                    let p = self.clone();
                                    let c = c.clone();
                                    async move { p.eval_condition(&c).await }
                                })
                                .collect();
                            futures::future::join_all(futs).await.iter().all(|&r| r)
                        }
                        "OneOf" => {
                            use futures::stream::{FuturesUnordered, StreamExt};
                            let mut futs: FuturesUnordered<_> = conditions
                                .iter()
                                .map(|c| {
                                    let p = self.clone();
                                    let c = c.clone();
                                    async move { p.eval_condition(&c).await }
                                })
                                .collect();
                            while let Some(ok) = futs.next().await {
                                if ok {
                                    return true;
                                }
                            }
                            false
                        }
                        other => {
                            warn!("[Pipeline] unknown combinator: {}", other);
                            false
                        }
                    }
                }
                Condition::Seq { items } => {
                    // 顺序跑完执行器序列→ true；期间 Done 信号到达→ false
                    let done_triggered = self.exec_executor_items(items).await;
                    !done_triggered
                }
            }
        })
    }

    // ── 表达式求值 ────────────────────────────────────────────────────────────

    fn eval_expr<'a>(
        &'a self,
        expr: &'a DataExpr,
    ) -> Pin<Box<dyn Future<Output = RuntimeValue> + Send + 'a>> {
        Box::pin(async move {
            match expr {
                DataExpr::Literal(v) => value_to_runtime(v),
                DataExpr::Var(name) => {
                    if let Some(v) = self.ctx.get_var(name).await {
                        return v;
                    }
                    if let Some(handler) = self.runtime.data_items.get(name.as_str()) {
                        return handler.get(&HashMap::new(), &self.ctx).await;
                    }
                    RuntimeValue::Number(0.0)
                }
                DataExpr::Symbol(sym) => {
                    if let Some(handler) = self.runtime.data_items.get(sym.name.as_str()) {
                        handler.get(&HashMap::new(), &self.ctx).await
                    } else {
                        RuntimeValue::Number(0.0)
                    }
                }
                DataExpr::BinOp { left, op, right } => {
                    let l = self.eval_expr(left).await.as_f64();
                    let r = self.eval_expr(right).await.as_f64();
                    RuntimeValue::Number(match op {
                        BinOp::Add => l + r,
                        BinOp::Sub => l - r,
                        BinOp::Mul => l * r,
                        BinOp::Div => {
                            if r.abs() > f64::EPSILON {
                                l / r
                            } else {
                                0.0
                            }
                        }
                    })
                }
                DataExpr::Call(call) => self
                    .exec_call(call)
                    .await
                    .unwrap_or(RuntimeValue::Number(0.0)),
                DataExpr::Tuple(exprs) => {
                    let mut vals = Vec::with_capacity(exprs.len());
                    for e in exprs {
                        vals.push(self.eval_expr(e).await);
                    }
                    RuntimeValue::Tuple(vals)
                }
            }
        })
    }

    // ── 辅助 ──────────────────────────────────────────────────────────────────

    async fn destructure(&self, targets: &[Option<String>], value: RuntimeValue) {
        let vals = match value {
            RuntimeValue::Tuple(v) => v,
            single => vec![single],
        };
        for (target, val) in targets.iter().zip(vals) {
            if let Some(name) = target {
                self.ctx.set_var(name, val).await;
            }
        }
    }

    /// 失败时将所有解构目标设为 Uninit
    async fn destructure_uninit(&self, targets: &[Option<String>]) {
        for target in targets {
            if let Some(name) = target {
                self.ctx.set_var(name, RuntimeValue::Uninit).await;
            }
        }
    }
}


// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 辅助函数（pub(crate) 供 runner/decision/executor 模块使用）
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// 从 DSL AST 的 NamedArg 列表静态求值为 RuntimeValue 映射
pub(crate) fn eval_named_args_static(args: &[NamedArg]) -> HashMap<String, RuntimeValue> {
    let mut map = HashMap::new();
    for arg in args {
        map.insert(arg.name.clone(), value_from_data_expr(&arg.value));
    }
    map
}

fn value_from_data_expr(expr: &DataExpr) -> RuntimeValue {
    match expr {
        DataExpr::Literal(v) => value_to_runtime(v),
        _ => RuntimeValue::Str("<dynamic>".into()),
    }
}

/// 初始化 TradeTaskContext 的变量表
pub(crate) async fn init_vars(ctx: &TradeTaskContext, vars: &VarsBlock) {
    for var in &vars.vars {
        let default = RuntimeValue::Number(0.0);
        ctx.set_var(&var.name, default).await;
    }
}

pub(crate) fn value_to_runtime(v: &Value) -> RuntimeValue {
    match v {
        Value::Number(n) => RuntimeValue::Number(*n),
        Value::Percent(p) => RuntimeValue::Percent(*p),
        Value::Duration(d) => RuntimeValue::Duration(*d as f64),
        Value::Amount(n, unit) => RuntimeValue::Amount(*n, unit.clone()),
        Value::Bool(b) => RuntimeValue::Bool(*b),
        Value::String(s) => RuntimeValue::Str(s.clone()),
        Value::List(items) => RuntimeValue::List(items.iter().map(value_to_runtime).collect()),
        Value::Map(_) => RuntimeValue::Str("<map>".into()),
        Value::Tuple(items) => RuntimeValue::Tuple(items.iter().map(value_to_runtime).collect()),
        Value::Uninit => RuntimeValue::Uninit,
    }
}

fn apply_compare_op(op: CompareOp, l: f64, r: f64) -> bool {
    match op {
        CompareOp::Ge => l >= r,
        CompareOp::Le => l <= r,
        CompareOp::Gt => l > r,
        CompareOp::Lt => l < r,
        CompareOp::Eq => (l - r).abs() < f64::EPSILON,
        CompareOp::Ne => (l - r).abs() >= f64::EPSILON,
    }
}
