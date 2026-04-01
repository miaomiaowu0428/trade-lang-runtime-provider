//! SpawnHandler — 后台派生控制流

use std::sync::Arc;

use async_trait::async_trait;

use trade_lang_core::{ControlFlowHandler, PipelineOps};
use trade_meta_compiler::ast::{Condition, ExecutorItem};

/// 后台派生：依序匹配分支，首个满足条件的分支执行后 signal_done
pub struct SpawnHandler;

#[async_trait]
impl ControlFlowHandler for SpawnHandler {
    async fn execute(
        &self,
        branches: &[(Condition, Vec<ExecutorItem>)],
        ops: Arc<dyn PipelineOps>,
    ) -> bool {
        let branches_owned = branches.to_vec();
        tokio::spawn(async move {
            for (cond, execs) in &branches_owned {
                if ops.is_done() {
                    break;
                }
                if ops.eval_condition(cond).await {
                    if ops.exec_executor_items(execs).await {
                        ops.signal_done();
                        break;
                    }
                }
            }
        });
        false
    }
}
