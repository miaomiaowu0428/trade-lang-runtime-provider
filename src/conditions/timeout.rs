//! TimeoutCondition — 通用等待条件
//!
//! 等待指定时长后返回 true；任务 Done 时提前退出返回 false。
//! 与 Solana 无关，作为通用 plugin 导出，由下游 impl crate 按需注册。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use log::info;

use trade_lang_core::{ConditionHandler, TradeTaskContext};
use trade_meta_compiler::RuntimeValue;

pub struct TimeoutCondition;

#[async_trait]
impl ConditionHandler for TimeoutCondition {
    async fn evaluate(
        &self,
        args: &HashMap<String, RuntimeValue>,
        ctx: &Arc<TradeTaskContext>,
    ) -> bool {
        let secs = args.get("duration").map(|v| v.as_f64()).unwrap_or(0.0);

        if secs <= 0.0 {
            return true;
        }

        info!("  [Timeout] waiting {:.1}s ...", secs);

        let duration = std::time::Duration::from_secs_f64(secs);
        tokio::select! {
            _ = tokio::time::sleep(duration) => {
                info!("  [Timeout] {:.1}s elapsed", secs);
                true
            }
            _ = ctx.done_future() => {
                false
            }
        }
    }
}
