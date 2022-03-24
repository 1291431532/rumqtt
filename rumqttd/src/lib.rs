#[macro_use]
extern crate log;

#[macro_use]
extern crate rouille;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use std::net::SocketAddr;

mod link;
mod protocol;
mod replicator;
mod router;
mod server;
mod segments;

pub type ConnectionId = usize;
pub type RouterId = usize;
pub type NodeId = usize;
pub type Topic = String;
pub type Filter = String;
pub type TopicId = usize;
pub type Offset = (u64, u64);
pub type Cursor = (u64, u64);

pub use link::local::{Link, LinkError, LinkRx, LinkTx};
pub use router::Notification;
pub use server::Broker;

const MAX_NODES: usize = 3;
const MAX_REPLICA_ID: usize = MAX_NODES - 1;

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Config {
    pub id: usize,
    pub router: RouterConfig,
    pub tcp: HashMap<String, ServerSettings>,
    pub websocket: HashMap<String, ServerSettings>,
    pub cluster: Option<ClusterSettings>,
    pub console: ConsoleSettings,
    pub bridge: Option<BridgeConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum TlsConfig {
    Rustls {
        ca_path: String,
        cert_path: String,
        key_path: String,
    },
    NativeTls {
        pkcs12_path: String,
        pkcs12_pass: String,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerSettings {
    pub listen: SocketAddr,
    pub tls_config: Option<TlsConfig>,
    pub next_connection_delay_ms: u64,
    pub connections: ConnectionSettings,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConnectionSettings {
    pub connection_timeout_ms: u16,
    pub max_client_id_len: usize,
    pub throttle_delay_ms: u64,
    pub max_payload_size: usize,
    pub max_inflight_count: u16,
    pub max_inflight_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSettings {
    /// Id with which this node connects to other nodes of the mesh
    pub node_id: NodeId,
    /// Address on which this broker is listening for mesh connections
    pub listen: String,
    /// Address of clusters that this node has to initiate connection
    pub seniors: Vec<(ConnectionId, String)>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RouterConfig {
    pub instant_ack: bool,
    pub max_segment_size: usize,
    pub max_mem_segments: usize,
    pub max_disk_segments: usize,
    pub max_read_len: u64,
    pub log_dir: Option<PathBuf>,
    pub max_connections: usize,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ConsoleSettings {
    pub listen: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BridgeConfig {
    pub url: String,
    pub port: u16,
    pub qos: u8,
    pub sub_path: String,
    pub reconnection_delay: u64,
    pub ping_delay: u64,
    pub timeout_delay: u64,
    #[serde(default)]
    pub transport: Transport,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Transport {
    #[serde(rename = "tcp")]
    Tcp,
    #[serde(rename = "tls")]
    #[cfg(feature = "rustls")]
    Tls {
        ca: PathBuf,
        client_auth: Option<ClientAuth>,
    },
}

impl Default for Transport {
    fn default() -> Self {
        Transport::Tcp
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientAuth {
    certs: PathBuf,
    key: PathBuf,
}
