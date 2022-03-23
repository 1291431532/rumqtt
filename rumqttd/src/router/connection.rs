use crate::Filter;
use flume::{bounded, Receiver, Sender};
use std::collections::HashSet;

use super::{ConnectionMeter, MetricsReply};

/// Used to register a new connection with the router
/// Connection messages encompasses a handle for router to
/// communicate with this connection
#[derive(Debug)]
pub struct Connection {
    /// Clean session
    pub clean: bool,
    /// Subscriptions
    pub subscriptions: HashSet<Filter>,
    /// Handle to send metrics reply
    pub metrics: Sender<MetricsReply>,
    /// Connection metrics
    pub meter: ConnectionMeter,
}

impl Connection {
    /// Create connection state to hold identifying information of connecting device
    pub fn new(clean: bool) -> (Connection, Receiver<MetricsReply>) {
        let (metrics_tx, metrics_rx) = bounded(1);
        let connection = Connection {
            clean,
            subscriptions: HashSet::default(),
            metrics: metrics_tx,
            meter: ConnectionMeter::default(),
        };

        (connection, metrics_rx)
    }
}
