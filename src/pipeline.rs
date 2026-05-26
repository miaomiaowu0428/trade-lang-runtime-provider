//! 交易执行管线
//!
//! `TradePipeline` 负责单次交易流程的执行（buy → sell → finally），
//! 由各种 Runtime 在收到触发信号后创建并 spawn。
//!
//! 该模块被所有 Runtime 模式共享：
//!   - LocalRuntime：monitor 触发后直接创建 pipeline
//!   - ExecutorRuntime：收到 TaskEnvelope 后创建 pipeline

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use log::{info, warn};

use trade_meta_compiler::ast::*;
use trade_meta_compiler::{RuntimeValue, TaskValue};

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

#[derive(Clone, Copy)]
enum ExecMode {
    /// 普通 buy/sell/condition 分支：每个 item 前检查 Done。
    Normal,
    /// Spawn 子任务：CondExec 允许先注册/等待 condition，其它 item 前检查 Done。
    Spawn,
    /// finally 块：不做入口 Done 检查，确保兜底逻辑能执行。
    Finally,
}

impl TradePipeline {
    pub fn new(runtime: Arc<RuntimeRegistry>, ctx: Arc<TradeTaskContext>) -> Self {
        Self { runtime, ctx }
    }

    /// 执行完整的交易流程
    pub async fn run(&self, task_id: u64, trigger: &TriggerBody) {
        // ── buy 阶段 ─────────────────────────────────────────────────────
        info!("  [Task#{}] ─── buy ───", task_id);
        for stmt in &trigger.buy {
            if self.ctx.is_done() {
                info!("  [Task#{}] Done signal, exiting buy", task_id);
                break;
            }
            if self.exec_block_item(stmt, ExecMode::Normal).await {
                break;
            }
        }

        // ── buy 结果处理：confirm 或 cancel 所有 handles ─────────────────
        if !self.ctx.is_done() {
            self.ctx.confirm_all_handles().await;
        } else {
            self.ctx.cancel_all_handles().await;
            // buy 失败 → 执行 else 块（若有），然后结束，不进入 sell
            if !trigger.buy_else.is_empty() {
                info!("  [Task#{}] ─── buy else ───", task_id);
                for stmt in &trigger.buy_else {
                    self.exec_block_item(stmt, ExecMode::Normal).await;
                }
            }
            info!("  [Task#{}] Trade pipeline finished (buy failed)", task_id);
            self.ctx.signal_done();
            return;
        }

        // ── sell 阶段 ─────────────────────────────────────────────────────
        if !self.ctx.is_done() {
            info!("  [Task#{}] ─── sell ───", task_id);
            for stmt in &trigger.sell {
                if self.ctx.is_done() {
                    info!("  [Task#{}] Done signal, exiting sell", task_id);
                    break;
                }
                if self.exec_block_item(stmt, ExecMode::Normal).await {
                    break;
                }
            }
        }

        if !trigger.sell_finally.is_empty() {
            info!("  [Task#{}] ─── finally ───", task_id);
            self.exec_block(&trigger.sell_finally, ExecMode::Finally)
                .await;
        }

        info!("  [Task#{}] Trade pipeline finished", task_id);
        self.ctx.signal_done();
    }

    // ── BlockItem execution ──────────────────────────────────────────────────

    fn exec_block<'a>(
        &'a self,
        items: &'a [BlockItem],
        mode: ExecMode,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        Box::pin(async move {
            for item in items {
                if self.exec_block_item(item, mode).await {
                    return true;
                }
            }
            false
        })
    }

    fn exec_block_item<'a>(
        &'a self,
        item: &'a BlockItem,
        mode: ExecMode,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        Box::pin(async move {
            let skip_for_done = match mode {
                ExecMode::Normal => self.ctx.is_done(),
                // Spawn 中 CondExec 不能被入口 done 短路，否则早注册的监听条件永远无法启动；
                // condition 自身会通过 ctx.done_future() 响应取消。
                ExecMode::Spawn => {
                    self.ctx.is_done() && !matches!(item, BlockItem::CondExec { .. })
                }
                ExecMode::Finally => false,
            };
            if skip_for_done {
                return true;
            }

            match item {
                BlockItem::LetAssign { var_name, value } => {
                    let v = self.eval_expr(value).await;
                    self.ctx.set_var_sync(var_name, v);
                    false
                }
                BlockItem::LetDestructure { targets, value } => {
                    let rv = self.eval_expr(value).await;
                    self.destructure(targets, rv);
                    false
                }
                BlockItem::Executor { call } => {
                    if call.name.name == "Done" {
                        self.ctx.signal_done();
                        return true;
                    }
                    self.exec_call(call).await;
                    false
                }
                BlockItem::CondExec { condition, body } => {
                    if self.eval_condition(condition).await {
                        if matches!(mode, ExecMode::Spawn) && self.ctx.is_done() {
                            return true;
                        }
                        self.exec_block(body, mode).await
                    } else {
                        false
                    }
                }
                BlockItem::Spawn { items } => {
                    self.spawn_items(items).await;
                    false
                }
            }
        })
    }

    /// Spawn 视为普通的 Executor Symbol 分派：pipeline 负责把 `items` 组装为一个
    /// 已就绪的 `PreparedSpawnTask`，以 `RuntimeValue::Task` 的形式传给内置 Spawn handler。
    async fn spawn_items(&self, items: &[BlockItem]) {
        let prepared: Arc<dyn Any + Send + Sync> = Arc::new(PreparedSpawnTask {
            pipeline: self.clone(),
            items: Arc::new(items.to_vec()),
        });
        let mut args: HashMap<String, RuntimeValue> = HashMap::new();
        args.insert("task".to_string(), RuntimeValue::Task(TaskValue(prepared)));
        if let Some(handler) = self.runtime.executors.get("Spawn") {
            handler.execute(&args, &self.ctx).await;
        } else {
            warn!(
                "[Pipeline] 'Spawn' executor not registered; dropping {} spawn items",
                items.len()
            );
        }
    }

    // ── Call 分派 ─────────────────────────────────────────────────────────────

    async fn exec_call(&self, call: &CallExpr) -> Option<RuntimeValue> {
        let name = &call.name.name;
        let args = self.eval_named_args(&call.args).await;

        let enter_elapsed = self.ctx.sig_time.map(|t| t.elapsed());
        let result = if let Some(handler) = self.runtime.executors.get(name.as_str()) {
            handler.execute(&args, &self.ctx).await
        } else if let Some(handler) = self.runtime.conditions.get(name.as_str()) {
            let ok = handler.evaluate(&args, &self.ctx).await.0;
            Some(RuntimeValue::Bool(ok))
        } else if let Some(handler) = self.runtime.data_items.get(name.as_str()) {
            Some(handler.get(&args, &self.ctx).await)
        } else {
            warn!("    [warn] '{}' not found in runtime", name);
            None
        };
        if let Some(enter) = enter_elapsed {
            info!(
                "  [Symbol] {} │ enter={:?} exit={:?}",
                name,
                enter,
                self.ctx.sig_time.map(|t| t.elapsed()).unwrap_or_default(),
            );
        }
        result
    }

    // ── Args evaluation ───────────────────────────────────────────────────────

    async fn eval_named_args(&self, args: &[NamedArg]) -> HashMap<String, RuntimeValue> {
        let mut map = HashMap::with_capacity(args.len());
        for arg in args {
            let v = self.eval_expr(&arg.value).await;
            map.insert(arg.name.clone(), v);
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
                        handler.evaluate(&args, &self.ctx).await.0
                    } else {
                        false
                    }
                }
                Condition::Combinator { name, conditions } => match name.as_str() {
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
                        // 预先将所有 LetBound 的目标变量初始化为 Uninit，
                        // 保证落败一侧的 targets 在 OneOf 结束后是 Uninit 状态。
                        for cond in conditions {
                            if let Condition::LetBound { targets, .. } = cond {
                                for target in targets {
                                    if let Some(name) = target {
                                        self.ctx.set_var_sync(name, RuntimeValue::Uninit);
                                    }
                                }
                            }
                        }
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
                },
                Condition::Seq { items } => {
                    // 顺序跑完执行序列→ true；期间 Done 信号到达→ false
                    let done_triggered = self.exec_block(items, ExecMode::Normal).await;
                    !done_triggered
                }
                Condition::LetBound { targets, inner } => {
                    // 运行内部条件，触发时将偏值解构绑定到 targets
                    if let Condition::Call(call) = inner.as_ref() {
                        let args = self.eval_named_args(&call.args).await;
                        if let Some(handler) = self.runtime.conditions.get(call.name.name.as_str())
                        {
                            let (triggered, side_value) = handler.evaluate(&args, &self.ctx).await;
                            if triggered {
                                if let Some(rv) = side_value {
                                    self.destructure(targets, rv);
                                }
                            }
                            return triggered;
                        }
                    }
                    false
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
                    if let Some(v) = self.ctx.get_var_sync(name) {
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
                    if *op == BinOp::Or {
                        // 懒求值：左侧非 Uninit 则直接返回，否则才求右侧
                        let l = self.eval_expr(left).await;
                        if !l.is_uninit() {
                            return l;
                        }
                        return self.eval_expr(right).await;
                    }
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
                        BinOp::Or => unreachable!(),
                    })
                }
                DataExpr::Call(call) => self
                    .exec_call(call)
                    .await
                    .unwrap_or(RuntimeValue::Number(0.0)),
                DataExpr::List(exprs) => {
                    let mut vals = Vec::with_capacity(exprs.len());
                    for e in exprs {
                        vals.push(self.eval_expr(e).await);
                    }
                    RuntimeValue::List(vals)
                }
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

    fn destructure(&self, targets: &[Option<String>], value: RuntimeValue) {
        let vals = match value {
            RuntimeValue::Tuple(v) => v,
            single => vec![single],
        };
        for (target, val) in targets.iter().zip(vals) {
            if let Some(name) = target {
                self.ctx.set_var_sync(name, val);
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
        DataExpr::List(items) => {
            RuntimeValue::List(items.iter().map(value_from_data_expr).collect())
        }
        _ => RuntimeValue::Str("<dynamic>".into()),
    }
}

/// 初始化 TradeTaskContext 的变量表
pub(crate) fn init_vars(ctx: &TradeTaskContext, vars: &VarsBlock) {
    for var in &vars.vars {
        let value = match var.var_type {
            VarType::Bool => RuntimeValue::Bool(false),
            _ => RuntimeValue::Number(0.0),
        };
        ctx.set_var_sync(&var.name, value);
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

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// PreparedSpawnTask — 组装好的后台任务
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// 由 pipeline 组装、待 `Spawn` executor handler 派发的后台任务。
///
/// - `pipeline` 为父 pipeline 的克隆，持有父 `TradeTaskContext`（用于创建子 ctx）。
/// - `items`    为 `Spawn[...]` 括号内的执行序列。
///
/// 经由 `RuntimeValue::Task(TaskValue(Arc<dyn Any>))` 传入 Spawn handler，
/// 后者 downcast 回本类型后调用 [`run`](Self::run) 在 tokio 上启动。
///
/// **隔离语义**：run() 会从父 ctx 派生出 child ctx（`spawn_child`）：
///   - 父 pipeline 取消 → 子 ctx 也取消（级联向下，任务被及时清理）
///   - 子任务内部失败调 `signal_done()` → 仅取消子 ctx，**不影响父 pipeline**
pub struct PreparedSpawnTask {
    pub pipeline: TradePipeline,
    pub items: Arc<Vec<BlockItem>>,
}

impl PreparedSpawnTask {
    /// 在当前 tokio 运行时上执行任务（调用方负责包裹 `tokio::spawn`）。
    pub async fn run(self: Arc<Self>) {
        // 派生子 ctx：共享 vars/contexts，但拥有独立的 cancel token
        let child_ctx = Arc::new(TradeTaskContext::spawn_child(&self.pipeline.ctx));
        let child_pipeline = TradePipeline {
            runtime: Arc::clone(&self.pipeline.runtime),
            ctx: child_ctx,
        };
        child_pipeline
            .exec_block(&self.items, ExecMode::Spawn)
            .await;
    }
}
