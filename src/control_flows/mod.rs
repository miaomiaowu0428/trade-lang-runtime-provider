//! 内置控制流 handler 实现
//!
//! 目前只有 OneOf。Spawn 由 pipeline 直接处理（无需 handler）,
//! All 降级为复合条件，由 pipeline 的 eval_condition 处理。

mod one_of;

pub use one_of::OneOfHandler;
