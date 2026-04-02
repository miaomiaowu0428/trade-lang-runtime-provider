//! QUIC 传输 — quinn 多路复用
//!
//! 优势：0-RTT 重连、内建 TLS 1.3、多流无 head-of-line blocking。
//!
//! ```text
//! Monitor binary:
//!   let publisher = QuicPublisher::bind("0.0.0.0:9200", server_config).await?;
//!
//! Executor binary:
//!   let subscriber = QuicSubscriber::connect("monitor-host:9200", client_config).await?;
//! ```

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use log::{error, info, warn};
use tokio::sync::broadcast;

use super::{TaskEnvelope, TaskPublisher, TaskSubscriber, decode_envelope, encode_envelope};

// ── 自签名证书辅助（开发/内网用）─────────────────────────────────────────────

/// 生成自签名证书，返回 (server_config, client_config)
///
/// 仅用于开发/内网部署。生产环境应使用正式证书。
pub fn self_signed_config()
-> Result<(quinn::ServerConfig, quinn::ClientConfig), Box<dyn std::error::Error + Send + Sync>> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])?;
    let cert_der = rustls::pki_types::CertificateDer::from(cert.cert);
    let key_der = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());

    // 共享传输配置：禁用空闲超时 + 客户端定期发 keep-alive ping
    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(None); // 禁用空闲超时
    transport.keep_alive_interval(Some(std::time::Duration::from_secs(10))); // 每10秒 ping
    let transport = Arc::new(transport);

    // Server config
    let mut server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?;
    server_config.transport_config(Arc::clone(&transport));

    // Client config — 跳过证书验证（内网开发使用）
    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(cert_der)?;
    let client_crypto = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let mut client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(client_crypto)?,
    ));
    client_config.transport_config(transport);

    Ok((server_config, client_config))
}

// ── Publisher (Monitor 侧) ───────────────────────────────────────────────────

/// QUIC 发布者：接受 executor 连接，对每个连接的客户端开 uni stream 推送
pub struct QuicPublisher {
    broadcast_tx: broadcast::Sender<Vec<u8>>,
}

impl QuicPublisher {
    /// 绑定端口并开始执行
    pub async fn bind(
        addr: &str,
        server_config: quinn::ServerConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let addr: SocketAddr = addr.parse()?;
        let endpoint = quinn::Endpoint::server(server_config, addr)?;
        let (broadcast_tx, _) = broadcast::channel::<Vec<u8>>(256);
        let tx = broadcast_tx.clone();

        info!("[QuicPublisher] Listening on {}", addr);

        tokio::spawn(async move {
            while let Some(incoming) = endpoint.accept().await {
                let mut rx = tx.subscribe();
                tokio::spawn(async move {
                    let conn = match incoming.await {
                        Ok(c) => c,
                        Err(e) => {
                            error!("[QuicPublisher] Connection failed: {}", e);
                            return;
                        }
                    };
                    let peer = conn.remote_address();
                    info!("[QuicPublisher] Executor connected: {}", peer);

                    // 每个 envelope 开一个 uni stream 发送
                    while let Ok(data) = rx.recv().await {
                        match conn.open_uni().await {
                            Ok(mut stream) => {
                                if let Err(e) = stream.write_all(&data).await {
                                    warn!("[QuicPublisher] Write error to {}: {}", peer, e);
                                    break;
                                }
                                let _ = stream.finish();
                            }
                            Err(e) => {
                                warn!("[QuicPublisher] Open stream error to {}: {}", peer, e);
                                break;
                            }
                        }
                    }
                    info!("[QuicPublisher] Executor {} disconnected", peer);
                });
            }
        });

        Ok(Self { broadcast_tx })
    }
}

#[async_trait]
impl TaskPublisher for QuicPublisher {
    async fn publish(
        &self,
        envelope: &TaskEnvelope,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let receivers = self.broadcast_tx.receiver_count();
        info!(
            "[Router:QUIC] ▶ Task #{} strategy={:?} contexts=[{}] → {} executor(s)",
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
        // 打入发布时间戳（纳秒）
        let mut envelope = envelope.clone();
        envelope.sent_at_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let data = encode_envelope(&envelope)?;;
        let _ = self.broadcast_tx.send(data);
        Ok(())
    }
}

// ── Subscriber (Executor 侧) ─────────────────────────────────────────────────

/// QUIC 订阅者：连接到 monitor 节点，接收 envelope
pub struct QuicSubscriber {
    conn: quinn::Connection,
}

impl QuicSubscriber {
    /// 连接到 monitor 节点
    pub async fn connect(
        addr: &str,
        client_config: quinn::ClientConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let addr: SocketAddr = addr.parse()?;
        let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse()?)?;
        endpoint.set_default_client_config(client_config);

        let conn = endpoint.connect(addr, "localhost")?.await?;
        info!("[QuicSubscriber] Connected to {}", addr);

        Ok(Self { conn })
    }
}

#[async_trait]
impl TaskSubscriber for QuicSubscriber {
    async fn recv(&mut self) -> Option<TaskEnvelope> {
        // 接收一个 uni stream，读取 length-prefix envelope
        let mut stream = self.conn.accept_uni().await.ok()?;
        let envelope = decode_envelope(&mut stream).await?;
        let recv_at_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let dispatch_us = recv_at_ns.saturating_sub(envelope.sent_at_ns) / 1_000;
        info!(
            "[Router:QUIC] ◀ Task #{} strategy={:?} received from QUIC stream dispatch={}µs",
            envelope.task_id, envelope.strategy_name, dispatch_us,
        );
        Some(envelope)
    }
}
