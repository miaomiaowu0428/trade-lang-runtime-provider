//! 通用条件 handler 实现
//!
//! 这里的条件与具体链无关，作为 plugin 导出，由 impl crate 按需注册。
//! （不在 register_builtins 中自动注册）

mod timeout;

pub use timeout::TimeoutCondition;
