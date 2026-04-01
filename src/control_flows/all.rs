//! AllHandler — 全满足控制流
//!
//! `All[cond1, cond2] => [execs]`：并发评估全部条件，全部为 true 后执行。
//! 解析阶段已将共享 executor 列表复制到每个分支中，
//! handler 从各分支提取条件并发评估，全部通过后执行第一个分支的 executor 列表。

use std::sync::Arc;

use async_trait::async_trait;

use trade_lang_core::{ControlFlowHandler, PipelineOps};
use trade_meta_compiler::ast::{Condition, ExecutorItem};

/// 并发全满足：提取各分支的条件并发评估，全部 true 后执行共享序列
pub struct AllHandler;

#[async_trait]
impl ControlFlowHandler for AllHandler {
    async fn execute(
        &self,
        branches: &[(Condition, Vec<ExecutorItem>)],
        ops: Arc<dyn PipelineOps>,
    ) -> bool {
        if branches.is_empty() {
            return false;
        }

        let futs: Vec<_> = branches
            .iter()
            .map(|(cond, _)| {
                let ops = Arc::clone(&ops);
                let cond = cond.clone();
                async move { ops.eval_condition(&cond).await }
            })
            .collect();

        let results = futures::future::join_all(futs).await;
        if results.iter().all(|&r| r) {
            ops.exec_executor_items(&branches[0].1).await
        } else {
            false
        }
    }
}
