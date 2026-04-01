//! AllHandler — 全满足控制流
//!
//! `All[cond1, cond2] => [execs]`：并发评估全部条件，全部为 true 后执行共享序列。
//! 语法上与 OneOf/Spawn 略有不同（共享执行器列表而非每分支独立），
//! 因此使用独立的 AllCallHandler trait，但语义上同属控制流范畴。

use std::sync::Arc;

use async_trait::async_trait;

use trade_lang_core::{AllCallHandler, PipelineOps};
use trade_meta_compiler::ast::{Condition, ExecutorItem};

/// 并发全满足：join_all 评估所有条件，全部 true 后执行共享序列
pub struct AllHandler;

#[async_trait]
impl AllCallHandler for AllHandler {
    async fn execute(
        &self,
        conditions: &[Condition],
        executors: &[ExecutorItem],
        ops: Arc<dyn PipelineOps>,
    ) -> bool {
        let futs: Vec<_> = conditions
            .iter()
            .map(|cond| {
                let ops = Arc::clone(&ops);
                let cond = cond.clone();
                async move { ops.eval_condition(&cond).await }
            })
            .collect();

        let results = futures::future::join_all(futs).await;
        if results.iter().all(|&r| r) {
            ops.exec_executor_items(executors).await
        } else {
            false
        }
    }
}
