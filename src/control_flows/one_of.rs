//! OneOfHandler — 并发竞争控制流

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{FuturesUnordered, StreamExt};

use trade_lang_core::{ControlFlowHandler, PipelineOps};
use trade_meta_compiler::ast::{Condition, ExecutorItem};

/// 并发竞争：所有分支并发评估条件，首个满足的执行其对应序列
pub struct OneOfHandler;

#[async_trait]
impl ControlFlowHandler for OneOfHandler {
    async fn execute(
        &self,
        branches: &[(Condition, Vec<ExecutorItem>)],
        ops: Arc<dyn PipelineOps>,
    ) -> bool {
        let branches_owned = branches.to_vec();
        let mut futs: FuturesUnordered<_> = branches_owned
            .iter()
            .enumerate()
            .map(|(i, (cond, _))| {
                let ops = Arc::clone(&ops);
                let cond = cond.clone();
                async move { (i, ops.eval_condition(&cond).await) }
            })
            .collect();

        let mut winner: Option<usize> = None;
        while let Some((i, ok)) = futs.next().await {
            if ok {
                winner = Some(i);
                break;
            }
        }
        drop(futs);

        if let Some(idx) = winner {
            ops.exec_executor_items(&branches_owned[idx].1).await
        } else {
            false
        }
    }
}
