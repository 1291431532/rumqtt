use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{protocol::v4::publish::Publish, ConnectionId, Filter, RouterConfig, RouterId};

mod connection;
mod graveyard;
pub mod iobufs;
mod logs;
mod routing;
mod tracker;
mod waiters;

pub use connection::Connection;
pub use routing::Router;
pub use waiters::Waiters;
pub const MAX_SCHEDULE_ITERATIONS: usize = 100;
pub const MAX_CHANNEL_CAPACITY: usize = 200;

pub(crate) type FilterIdx = usize;

#[derive(Debug)]
pub enum Event {
    /// Client id and connection handle
    Connect {
        connection: Box<connection::Connection>,
        incoming: iobufs::Incoming,
        outgoing: Box<iobufs::Outgoing>,
    },
    /// Connection ready to receive more data
    Ready,
    /// Data for native commitlog
    DeviceData,
    /// Data for replicated commitlog
    ReplicaData(Bytes),
    /// Disconnection request
    Disconnect(Disconnection),
    /// Shadow
    Shadow(ShadowRequest),
    /// Get metrics of a connection or all connections
    Metrics(MetricsRequest),
}

/// Notification from router to connection
#[derive(Debug)]
pub enum Notification {
    /// Connection reply
    ConnectionAck(ConnectionAck),
    /// Data reply
    Forward {
        cursor: (u64, u64),
        size: usize,
        topic: Bytes,
        qos: u8,
        pkid: u16,
        payload: Bytes,
    },
    /// Acks reply for connection data
    DeviceAck(Ack),
    /// Data reply
    ReplicaData {
        cursor: (u64, u64),
        size: usize,
        payload: Bytes,
    },
    /// Acks reply for replication data
    ReplicaAcks {
        offset: (u64, u64),
        payload: Bytes,
    },
    /// All metrics
    Metrics(MetricsReply),
    /// Shadow
    Shadow(ShadowReply),
    Unschedule,
}

#[derive(Debug)]
pub enum Ack {
    PubAck(u16),
    SubAck(u16, Vec<u8>),
    PingResp,
}

fn packetid(ack: &Ack) -> u16 {
    match ack {
        Ack::PubAck(pkid) => *pkid,
        Ack::SubAck(pkid, _) => *pkid,
        Ack::PingResp => 0,
    }
}

/// Request that connection/linker makes to extract data from commitlog
/// NOTE Connection can make one sweep request to get data from multiple topics
/// but we'll keep it simple for now as multiple requests in one message can
/// makes constant extraction size harder
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataRequest {
    /// Commitlog this request is pulling data from
    pub filter: Filter,
    pub filter_idx: FilterIdx,
    /// Qos of the outgoing data
    pub qos: u8,
    /// (segment, offset) tuples per replica (1 native and 2 replicas)
    pub cursor: (u64, u64),
    /// number of messages read from subscription
    pub read_count: usize,
    /// Maximum count of payload buffer per replica
    max_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcksRequest;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Request {
    Data(DataRequest),
    Ack(AcksRequest),
}

/// A single message from connection to router
pub struct Message {
    /// Log to sweep
    pub topic: String,
    /// Qos of the topic
    pub qos: u8,
    /// Reply data chain
    pub payload: Bytes,
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Topic = {:?}, Payload size = {}", self.topic, self.payload.len())
    }
}

/// A batch of messages from connection to router
pub struct Data {
    /// (segment, offset) tuples per replica (1 native and 2 replicas)
    pub offset: (u64, u64),
    /// Payload size
    pub size: usize,
    /// Reply data chain
    pub payload: Vec<Publish>,
}

impl fmt::Debug for Data {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Cursors = {:?}, Payload size = {}, Payload count = {}",
            self.offset,
            self.size,
            self.payload.len()
        )
    }
}

#[derive(Debug)]
pub enum ConnectionAck {
    /// Id assigned by the router for this connection and
    /// previous session status
    Success(usize, bool),
    /// Failure and reason for failure string
    Failure(String),
}

#[derive(Debug)]
pub struct Disconnection {
    pub id: String,
    pub execute_will: bool,
    pub pending: Vec<Notification>,
}

#[derive(Debug, Clone)]
pub struct ShadowRequest {
    pub filter: String,
}

#[derive(Debug, Clone)]
pub struct ShadowReply {
    pub topic: String,
    pub payload: Bytes,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RouterMetrics {
    pub router_id: RouterId,
    pub total_connections: usize,
    pub total_subscriptions: usize,
    pub total_publishes: usize,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SubscriptionMeter {
    pub count: usize,
    pub total_size: usize,
    pub head_and_tail_id: (u64, u64),
    pub append_offset: (u64, u64),
    pub read_offset: usize,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ConnectionMeter {
    publish_count: usize,
    publish_size: usize,
    subscriptions: HashSet<Filter>,
    events: VecDeque<String>,
}

impl ConnectionMeter {
    pub fn increment_publish_count(&mut self) {
        self.publish_count += 1
    }

    pub fn add_publish_size(&mut self, size: usize) {
        self.publish_size += size;
    }

    pub fn push_subscription(&mut self, filter: Filter) {
        self.subscriptions.insert(filter);
    }

    pub fn push_subscriptions(&mut self, filters: HashSet<Filter>) {
        self.subscriptions = filters;
    }

    pub fn push_event(&mut self, event: String) {
        self.events.push_back(event);
        if self.events.len() > 10 {
            self.events.pop_front();
        }
    }
}

#[derive(Debug, Clone)]
pub enum MetricsRequest {
    Config,
    Router,
    ReadyQueue,
    Connection(String),
    Subscriptions,
    Subscription(Filter),
    Waiters(Filter),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricsReply {
    Config(RouterConfig),
    Router(RouterMetrics),
    Connection(Option<(ConnectionMeter, tracker::Tracker)>),
    Subscriptions(HashMap<Filter, Vec<String>>),
    Subscription(Option<SubscriptionMeter>),
    Waiters(Option<VecDeque<(String, DataRequest)>>),
    ReadyQueue(VecDeque<ConnectionId>),
}
