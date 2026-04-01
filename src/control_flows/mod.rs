//! 内置控制流 handler 实现
//!
//! Spawn / OneOf / All 均属控制流语义，统一在此模块组织。
//! 下游 impl crate 可覆盖注册（如带取消信号的 SolanaSpawnHandler）。

mod all;
mod one_of;
mod spawn;

pub use all::AllHandler;
pub use one_of::OneOfHandler;
pub use spawn::SpawnHandler;
