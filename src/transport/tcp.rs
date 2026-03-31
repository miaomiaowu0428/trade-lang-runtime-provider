//! TCP 传输 — length-prefix framing + bincode
//!
//! ```text
//! Monitor binary:
//!   let publisher = TcpPublisher::bind("0.0.0.0:9100").await?;
//!   // ... 每个连接的 executor 都会收到 publish 的 envelope
//!
//! Executor binary:
//!   let subscriber = TcpSubscriber::connect("monitor-host:9100").await?;
//!   // ... subscriber.recv() 阻塞等待
//! ```

use async_trait::async_trait;
use log::{error, info};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, broadcast};

use super::{TaskEnvelope, TaskPublisher, TaskSubscriber, decode_envelope, encode_envelope};

// ── Publisher (Monitor 侧) ───────────────────────────────────────────────────

/// TCP 发布者：监听端口，接受 executor 连接，广播 envelope 给所有已连接客户端
pub struct TcpPublisher {
    broadcast_tx: broadcast::Sender<Vec<u8>>,
}

impl TcpPublisher {
    /// 绑定端口并开始接受连接
    ///
    /// 返回 publisher 后，后台 task 持续 accept 新连接。
    /// 每个连接的 executor 会收到 publish 后的所有 envelope。
    pub async fn bind(addr: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(addr).await?;
        let (broadcast_tx, _) = broadcast::channel::<Vec<u8>>(256);
        let broadcast_tx_clone = broadcast_tx.clone();

        info!("[TcpPublisher] Listening on {}", addr);

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        info!("[TcpPublisher] Executor connected: {}", peer);
                        let mut rx = broadcast_tx_clone.subscribe();
                        tokio::spawn(async move {
                            let mut stream = stream;
                            while let Ok(data) = rx.recv().await {
                                if stream.write_all(&data).await.is_err() {
                                    info!("[TcpPublisher] Executor {} disconnected", peer);
                                    break;
                                }
                            }
                        });
                    }
                    Err(e) => {
                        error!("[TcpPublisher] Accept error: {}", e);
                    }
                }
            }
        });

        Ok(Self { broadcast_tx })
    }
}

#[async_trait]
impl TaskPublisher for TcpPublisher {
    async fn publish(
        &self,
        envelope: &TaskEnvelope,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let receivers = self.broadcast_tx.receiver_count();
        info!(
            "[Router:TCP] ▶ Task #{} strategy={:?} contexts=[{}] → {} executor(s)",
            envelope.task_id,
            envelope.strategy_name,
            envelope
                .contexts
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", "),
            receivers,
        );
        let data = encode_envelope(envelope)?;
        let _ = self.broadcast_tx.send(data);
        Ok(())
    }
}

// ── Subscriber (Executor 侧) ─────────────────────────────────────────────────

/// TCP 订阅者：连接到 monitor 节点，接收 envelope
pub struct TcpSubscriber {
    stream: Mutex<TcpStream>,
}

impl TcpSubscriber {
    /// 连接到 monitor 节点
    pub async fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        info!("[TcpSubscriber] Connected to {}", addr);
        Ok(Self {
            stream: Mutex::new(stream),
        })
    }
}

#[async_trait]
impl TaskSubscriber for TcpSubscriber {
    async fn recv(&mut self) -> Option<TaskEnvelope> {
        let mut stream = self.stream.lock().await;
        let envelope = decode_envelope(&mut *stream).await?;
        info!(
            "[Router:TCP] ◀ Task #{} strategy={:?} received from TCP stream",
            envelope.task_id, envelope.strategy_name,
        );
        Some(envelope)
    }
}
