//! 本地传输 — tokio mpsc channel
//!
//! 同进程内 MonitorSide 和 ExecutorSide 之间的零网络开销通信。

use async_trait::async_trait;
use log::info;
use tokio::sync::mpsc;

use super::{TaskEnvelope, TaskPublisher, TaskSubscriber};

pub struct LocalPublisher {
    tx: mpsc::Sender<TaskEnvelope>,
}

pub struct LocalSubscriber {
    rx: mpsc::Receiver<TaskEnvelope>,
}

/// 创建一对本地传输端点
pub fn local_transport(capacity: usize) -> (LocalPublisher, LocalSubscriber) {
    let (tx, rx) = mpsc::channel(capacity);
    (LocalPublisher { tx }, LocalSubscriber { rx })
}

#[async_trait]
impl TaskPublisher for LocalPublisher {
    async fn publish(
        &self,
        envelope: &TaskEnvelope,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "[Router:Local] ▶ Task #{} strategy={:?} contexts=[{}] → local channel",
            envelope.task_id,
            envelope.strategy_name,
            envelope
                .contexts
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", "),
        );
        self.tx
            .send(envelope.clone())
            .await
            .map_err(|e| format!("local publish failed: {}", e))?;
        Ok(())
    }
}

#[async_trait]
impl TaskSubscriber for LocalSubscriber {
    async fn recv(&mut self) -> Option<TaskEnvelope> {
        let envelope = self.rx.recv().await?;
        info!(
            "[Router:Local] ◀ Task #{} strategy={:?} received from local channel",
            envelope.task_id, envelope.strategy_name,
        );
        Some(envelope)
    }
}
