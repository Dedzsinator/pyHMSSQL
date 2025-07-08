//! High-performance geo-routing sidecar for pyHMSSQL
//! 
//! This Rust service provides sub-millisecond geo-routing decisions
//! for the pyHMSSQL distributed database system.

use anyhow::{Context, Result};
use clap::Parser;
use dashmap::DashMap;
use maxminddb::Reader;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UnixListener};
use tracing::{debug, error, info, warn};

pub mod geo;
pub mod routing;
pub mod metrics;

use geo::{GeoLocation, GeoResolver};
use routing::{ReplicaInfo, RoutingEngine, RoutingRequest, RoutingResponse};
use metrics::MetricsCollector;

#[derive(Parser, Debug)]
#[command(name = "geo_router_sidecar")]
#[command(about = "High-performance geo-routing sidecar for pyHMSSQL")]
pub struct Args {
    /// TCP port to listen on
    #[arg(short, long, default_value = "19999")]
    pub port: u16,

    /// Unix socket path
    #[arg(short, long, default_value = "/tmp/pyhmssql_geo_router.sock")]
    pub socket: PathBuf,

    /// Maximum concurrent connections
    #[arg(short = 'c', long, default_value = "1000")]
    pub max_connections: usize,

    /// GeoIP database path
    #[arg(short = 'g', long)]
    pub geoip_db: Option<PathBuf>,

    /// Log level
    #[arg(short = 'l', long, default_value = "info")]
    pub log_level: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SidecarRequest {
    #[serde(flatten)]
    pub inner: SidecarRequestType,
    pub timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SidecarRequestType {
    #[serde(rename = "route")]
    Route {
        client_ip: String,
        query_type: String,
    },
    #[serde(rename = "update_routing_table")]
    UpdateRoutingTable {
        replicas: Vec<ReplicaInfo>,
    },
    #[serde(rename = "ping")]
    Ping,
    #[serde(rename = "metrics")]
    GetMetrics,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SidecarResponse {
    pub success: bool,
    pub data: Option<serde_json::Value>,
    pub error: Option<String>,
    pub timestamp: u64,
}

impl SidecarResponse {
    pub fn success(data: serde_json::Value) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
            timestamp: current_timestamp_micros(),
        }
    }

    pub fn error(error: String) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(error),
            timestamp: current_timestamp_micros(),
        }
    }
}

pub struct GeoRouterSidecar {
    args: Args,
    geo_resolver: Arc<GeoResolver>,
    routing_engine: Arc<RwLock<RoutingEngine>>,
    metrics: Arc<MetricsCollector>,
    active_connections: Arc<dashmap::DashMap<String, SystemTime>>,
}

impl GeoRouterSidecar {
    pub fn new(args: Args) -> Result<Self> {
        let geo_resolver = Arc::new(GeoResolver::new(args.geoip_db.clone())?);
        let routing_engine = Arc::new(RwLock::new(RoutingEngine::new()));
        let metrics = Arc::new(MetricsCollector::new());
        let active_connections = Arc::new(DashMap::new());

        Ok(Self {
            args,
            geo_resolver,
            routing_engine,
            metrics,
            active_connections,
        })
    }

    pub async fn run(&self) -> Result<()> {
        info!("Starting geo-routing sidecar on port {}", self.args.port);

        // Start both TCP and Unix socket listeners
        let tcp_task = self.start_tcp_listener();
        let unix_task = self.start_unix_listener();
        let metrics_task = self.start_metrics_collector();

        // Run all tasks concurrently
        tokio::select! {
            result = tcp_task => {
                error!("TCP listener stopped: {:?}", result);
                result
            }
            result = unix_task => {
                error!("Unix socket listener stopped: {:?}", result);
                result
            }
            result = metrics_task => {
                error!("Metrics collector stopped: {:?}", result);
                result
            }
        }
    }

    async fn start_tcp_listener(&self) -> Result<()> {
        let addr = SocketAddr::from(([127, 0, 0, 1], self.args.port));
        let listener = TcpListener::bind(addr)
            .await
            .context("Failed to bind TCP listener")?;

        info!("TCP listener bound to {}", addr);

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            
            // Check connection limit
            if self.active_connections.len() >= self.args.max_connections {
                warn!("Connection limit reached, rejecting {}", peer_addr);
                continue;
            }

            let connection_id = format!("tcp:{}", peer_addr);
            self.active_connections.insert(connection_id.clone(), SystemTime::now());

            let geo_resolver = Arc::clone(&self.geo_resolver);
            let routing_engine = Arc::clone(&self.routing_engine);
            let metrics = Arc::clone(&self.metrics);
            let active_connections = Arc::clone(&self.active_connections);

            tokio::spawn(async move {
                if let Err(e) = handle_connection(
                    stream,
                    geo_resolver,
                    routing_engine,
                    metrics,
                ).await {
                    debug!("Connection error for {}: {}", peer_addr, e);
                }
                active_connections.remove(&connection_id);
            });
        }
    }

    async fn start_unix_listener(&self) -> Result<()> {
        // Remove existing socket file
        if self.args.socket.exists() {
            std::fs::remove_file(&self.args.socket)?;
        }

        let listener = UnixListener::bind(&self.args.socket)
            .context("Failed to bind Unix socket")?;

        info!("Unix socket listener bound to {:?}", self.args.socket);

        loop {
            let (stream, _) = listener.accept().await?;
            
            // Check connection limit
            if self.active_connections.len() >= self.args.max_connections {
                warn!("Connection limit reached, rejecting Unix socket connection");
                continue;
            }

            let connection_id = format!("unix:{}", current_timestamp_micros());
            self.active_connections.insert(connection_id.clone(), SystemTime::now());

            let geo_resolver = Arc::clone(&self.geo_resolver);
            let routing_engine = Arc::clone(&self.routing_engine);
            let metrics = Arc::clone(&self.metrics);
            let active_connections = Arc::clone(&self.active_connections);

            tokio::spawn(async move {
                if let Err(e) = handle_connection(
                    stream,
                    geo_resolver,
                    routing_engine,
                    metrics,
                ).await {
                    debug!("Unix socket connection error: {}", e);
                }
                active_connections.remove(&connection_id);
            });
        }
    }

    async fn start_metrics_collector(&self) -> Result<()> {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
        
        loop {
            interval.tick().await;
            
            // Clean up old connections
            let now = SystemTime::now();
            self.active_connections.retain(|_, &mut connected_at| {
                now.duration_since(connected_at)
                    .map_or(false, |d| d.as_secs() < 300) // 5 minutes
            });

            // Log metrics
            let metrics = self.metrics.get_snapshot();
            info!("Metrics: active_connections={}, total_requests={}, avg_latency_us={:.2}", 
                self.active_connections.len(),
                metrics.total_requests,
                metrics.avg_latency_micros
            );
        }
    }
}

async fn handle_connection<S>(
    mut stream: S,
    geo_resolver: Arc<GeoResolver>,
    routing_engine: Arc<RwLock<RoutingEngine>>,
    metrics: Arc<MetricsCollector>,
) -> Result<()>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    let mut buffer = [0u8; 4];
    
    loop {
        // Read request length
        stream.read_exact(&mut buffer).await?;
        let length = u32::from_be_bytes(buffer) as usize;
        
        if length > 1024 * 1024 {
            return Err(anyhow::anyhow!("Request too large: {} bytes", length));
        }

        // Read request data
        let mut request_data = vec![0u8; length];
        stream.read_exact(&mut request_data).await?;

        let start_time = std::time::Instant::now();

        // Process request
        let response = match process_request(
            &request_data,
            &geo_resolver,
            &routing_engine,
        ).await {
            Ok(resp) => resp,
            Err(e) => SidecarResponse::error(e.to_string()),
        };

        // Record metrics
        let latency_micros = start_time.elapsed().as_micros() as u64;
        metrics.record_request(latency_micros, response.success);

        // Send response
        let response_data = serde_json::to_vec(&response)?;
        let response_length = (response_data.len() as u32).to_be_bytes();
        
        stream.write_all(&response_length).await?;
        stream.write_all(&response_data).await?;
        stream.flush().await?;
    }
}

async fn process_request(
    request_data: &[u8],
    geo_resolver: &GeoResolver,
    routing_engine: &Arc<RwLock<RoutingEngine>>,
) -> Result<SidecarResponse> {
    let request: SidecarRequest = serde_json::from_slice(request_data)?;

    match request.inner {
        SidecarRequestType::Route { client_ip, query_type } => {
            let routing_request = RoutingRequest {
                client_ip: client_ip.parse()?,
                query_type,
                timestamp: request.timestamp,
            };

            let routing_response = routing_engine
                .read()
                .route_request(&routing_request, geo_resolver)?;

            Ok(SidecarResponse::success(serde_json::to_value(routing_response)?))
        }
        
        SidecarRequestType::UpdateRoutingTable { replicas } => {
            routing_engine.write().update_replicas(replicas)?;
            Ok(SidecarResponse::success(serde_json::json!({"updated": true})))
        }
        
        SidecarRequestType::Ping => {
            Ok(SidecarResponse::success(serde_json::json!({"pong": true})))
        }
        
        SidecarRequestType::GetMetrics => {
            // Return current metrics
            Ok(SidecarResponse::success(serde_json::json!({"metrics": "todo"})))
        }
    }
}

fn current_timestamp_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

fn init_tracing(level: &str) -> Result<()> {
    let level = match level.to_lowercase().as_str() {
        "trace" => tracing::Level::TRACE,
        "debug" => tracing::Level::DEBUG,
        "info" => tracing::Level::INFO,
        "warn" => tracing::Level::WARN,
        "error" => tracing::Level::ERROR,
        _ => tracing::Level::INFO,
    };

    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_target(false)
        .with_thread_ids(true)
        .init();

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    init_tracing(&args.log_level)?;
    
    info!("Starting pyHMSSQL geo-routing sidecar v{}", env!("CARGO_PKG_VERSION"));
    
    let sidecar = GeoRouterSidecar::new(args)?;
    sidecar.run().await
}
