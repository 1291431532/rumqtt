use crate::link::local::{LinkRx, LinkTx};
use crate::protocol::v4::publish::PublishBytes;
use crate::protocol::v4::{self, publish::Publish, Packet};
use crate::*;
use flume::{bounded, Receiver, RecvError, Sender, TryRecvError};
use log::*;
use parking_lot::Mutex;
use segments::Position;
use slab::Slab;
use std::collections::{HashMap, HashSet, VecDeque};
use std::thread;
use std::time::SystemTime;
use thiserror::Error;

use super::graveyard::Graveyard;
use super::iobufs::{Incoming, Outgoing};
use super::logs::{AcksLog, DataLog};
use super::tracker::Tracker;
use super::{
    packetid, Ack, Connection, ConnectionAck, DataRequest, Event, FilterIdx, MetricsReply,
    MetricsRequest, Notification, RouterMetrics, ShadowReply, ShadowRequest, MAX_CHANNEL_CAPACITY,
    MAX_SCHEDULE_ITERATIONS,
};

#[derive(Error, Debug)]
pub enum RouterError {
    #[error("Receive error = {0}")]
    Recv(#[from] RecvError),
    #[error("Try Receive error = {0}")]
    TryRecv(#[from] TryRecvError),
    #[error("Disconnection")]
    Disconnected,
}

type ReplicaLink = (Receiver<()>, Arc<Mutex<VecDeque<Notification>>>);

pub struct Router {
    id: RouterId,
    /// Id of this router. Used to index native commitlog to store data from
    /// local connections
    config: RouterConfig,
    /// Saved state of dead persistent connections
    graveyard: Graveyard,
    /// Data log of all the subscriptions
    datalog: DataLog,
    /// Acks log of the all the connections
    ackslog: AcksLog,
    /// List of connections
    connections: Slab<Box<Connection>>,
    ibufs: Slab<Incoming>,
    obufs: Slab<Box<Outgoing>>,
    /// Connection map from device id to connection id
    connection_map: HashMap<String, ConnectionId>,
    /// Subscription map to interested connection ids
    subscription_map: HashMap<Filter, HashSet<ConnectionId>>,
    /// Subscriptions and matching topics maintained per connection
    trackers: Slab<Tracker>,
    /// Connections with more pending requests and ready to make progress
    readyqueue: VecDeque<ConnectionId>,
    /// Channel receiver to receive data from all the active connections and
    /// replicators. Each connection will have a tx handle which they use
    /// to send data and requests to router
    router_rx: Receiver<(ConnectionId, Event)>,
    /// Channel sender to send data to this router. This is given to active
    /// network connections, local connections and replicators to communicate
    /// with this router
    router_tx: Sender<(ConnectionId, Event)>,
    /// Router metrics
    router_metrics: RouterMetrics,
    /// Replica links
    _replica_links: HashMap<NodeId, ReplicaLink>,
}

impl Router {
    pub fn new(router_id: RouterId, config: RouterConfig) -> Router {
        let (router_tx, router_rx) = bounded(1000);

        let mut connections: Slab<Box<Connection>> = Slab::with_capacity(config.max_connections);
        let mut ibufs = Slab::with_capacity(config.max_connections);
        let mut obufs: Slab<Box<Outgoing>> = Slab::with_capacity(config.max_connections);
        let mut trackers = Slab::with_capacity(config.max_connections);
        let mut ackslog = AcksLog::with_capacity(config.max_connections);
        let mut links = HashMap::new();

        let router_metrics = RouterMetrics {
            router_id,
            ..RouterMetrics::default()
        };

        // Reserve MAX_NODES slots so that new non replica connections
        // starts from id > MAX_NODES. Data from connections 0..MAX_REPLICAS
        // is used to identify replication data of # subscription from other
        // nodes and written to replicated commitlog related to node id
        for replica_id in 0..MAX_NODES {
            let client_id = format!("replica@{}", replica_id);
            let (connection, _) = Connection::new(false);
            let (outgoing, rx) = Outgoing::new(client_id.clone());
            let incoming = Incoming::new(client_id.clone());
            let notif_buffer = Arc::new(Mutex::new(VecDeque::with_capacity(MAX_CHANNEL_CAPACITY)));

            let id = connections.insert(Box::new(connection));
            debug!("{:15.15}[>] reserving slot {} for replica {}", client_id, id, id);
            assert_eq!(trackers.insert(Tracker::new()), id);
            assert_eq!(ackslog.insert(), id);
            assert_eq!(obufs.insert(Box::new(outgoing)), id);
            assert_eq!(ibufs.insert(incoming), id);

            links.insert(replica_id, (rx, notif_buffer));
        }

        let max_connections = config.max_connections;
        Router {
            id: router_id,
            config: config.clone(),
            graveyard: Graveyard::new(),
            datalog: DataLog::new(config).unwrap(),
            ackslog,
            connections,
            ibufs,
            obufs,
            connection_map: Default::default(),
            subscription_map: Default::default(),
            trackers,
            readyqueue: VecDeque::with_capacity(max_connections),
            router_rx,
            router_tx,
            _replica_links: links,
            router_metrics,
        }
    }

    /// Gets handle to the router. This is not a public method to ensure that link
    /// is created only after the router starts
    fn link(&self) -> Sender<(ConnectionId, Event)> {
        self.router_tx.clone()
    }

    pub(crate) fn get_replica_handle(&mut self, _replica_id: NodeId) -> (LinkTx, LinkRx) {
        unimplemented!()
    }

    /// Starts the router in a background thread and returns link to it. Link
    /// to communicate with router should only be returned only after it starts.
    /// For that reason, all the public methods should start the router in the
    /// background
    pub fn spawn(mut self) -> Sender<(ConnectionId, Event)> {
        let router = thread::Builder::new().name(format!("router-{}", self.id));
        let link = self.link();
        router
            .spawn(move || {
                let e = self.run(0);
                error!("Router done! Reason = {:?}", e);
            })
            .unwrap();
        link
    }

    /// Waits on incoming events when ready queue is empty.
    /// After pulling 1 event, tries to pull 500 more events
    /// before polling ready queue 100 times (connections)
    fn run(&mut self, count: usize) -> Result<(), RouterError> {
        match count {
            0 => loop {
                self.run_inner()?;
            },
            n => {
                for _ in 0..n {
                    self.run_inner()?;
                }
            }
        };

        Ok(())
    }

    fn run_inner(&mut self) -> Result<(), RouterError> {
        // Block on incoming events if there are no ready connections
        if self.readyqueue.is_empty() {
            // trace!("{}:: {:20} {:20} {:?}", self.id, "", "done-await", self.readyqueue);
            let (id, data) = self.router_rx.recv()?;
            self.events(id, data);
        }

        // Try reading more from connections in a non-blocking
        // fashion to accumulate data and handle subscriptions.
        // Accumulating more data lets requests retrieve bigger
        // bulks which in turn increases efficiency
        for _ in 0..500 {
            // All these methods will handle state and errors
            match self.router_rx.try_recv() {
                Ok((id, data)) => self.events(id, data),
                Err(TryRecvError::Disconnected) => return Err(RouterError::Disconnected),
                Err(TryRecvError::Empty) => break,
            }
        }

        // A connection should not be scheduled multiple times
        debug_assert!(self.check_readyqueue_duplicates());

        // Poll 100 connections which are ready in ready queue
        for _ in 0..100 {
            match self.readyqueue.pop_front() {
                Some(id) => self.consume(id),
                None => break,
            }
        }

        Ok(())
    }

    /// Consumes exactly count events of the receiver. If readyqueue has some ids to consume, this
    /// will first consume all ids before it finds the one which matches the id of the current
    /// received event, in each iteration.
    #[cfg(test)]
    fn run_exact(&mut self, count: usize) -> Result<(), RouterError> {
        for _ in 0..count {
            let (id, data) = self.router_rx.recv()?;
            self.events(id, data);

            if let Some(mut rqid) = self.readyqueue.pop_front() {
                if rqid == id {
                    self.consume(rqid);
                } else {
                    while rqid != id {
                        self.consume(rqid);
                        rqid = match self.readyqueue.pop_front() {
                            Some(id) => id,
                            None => break,
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn check_readyqueue_duplicates(&self) -> bool {
        let mut uniq = HashSet::new();
        self.readyqueue.iter().all(move |x| uniq.insert(x))
    }

    fn check_tracker_duplicates(&self, id: ConnectionId) -> bool {
        let mut uniq = HashSet::new();

        let tracker = self.trackers.get(id).unwrap();
        let no_duplicates = tracker
            .get_data_requests()
            .iter()
            .all(move |x| uniq.insert(x.filter_idx));

        if !no_duplicates {
            dbg!(tracker.get_data_requests());
        }

        no_duplicates
    }

    fn events(&mut self, id: ConnectionId, data: Event) {
        match data {
            Event::Connect {
                connection,
                incoming,
                outgoing,
            } => self.handle_new_connection(connection, incoming, outgoing),
            Event::DeviceData => self.handle_device_payload(id),
            Event::ReplicaData(_data) => {
                // self.handle_replica_payload(id, data)
            }
            Event::Disconnect(_request) => self.handle_disconnection(id),
            Event::Ready => self.connection_ready_reschedule(id),
            Event::Shadow(request) => self.retrieve_shadow(id, request),
            Event::Metrics(metrics) => self.retrieve_metrics(id, metrics),
        }
    }

    fn connection_ready_reschedule(&mut self, id: ConnectionId) {
        let tracker = self.trackers.get_mut(id).unwrap();
        if tracker.busy_unschedule() {
            tracker.set_busy_unschedule(false);
            self.readyqueue.push_back(id);
            trace!("{:15.15}[I] {:20}", self.obufs[id].client_id, "ready-reschedule");
        }
    }

    fn ack_freeslots_reschedule(&mut self, id: ConnectionId) {
        let tracker = self.trackers.get_mut(id).unwrap();
        if tracker.inflight_full_unschedule() {
            trace!("{:15.15}[I] {:20}", self.obufs[id].client_id, "ack-reschedule");
            tracker.set_inflight_full_unschedule(false);
            self.readyqueue.push_back(id);
        }
    }

    fn handle_new_connection(
        &mut self,
        mut connection: Box<Connection>,
        incoming: Incoming,
        outgoing: Box<Outgoing>,
    ) {
        let client_id = outgoing.client_id.clone();

        if self.connections.len() > self.config.max_connections {
            error!("{:15.15}[E] {:20}", client_id, "no space for new connection");
            // let ack = ConnectionAck::Failure("No space for new connection".to_owned());
            // let message = Notification::ConnectionAck(ack);
            return;
        }

        // Retrieve previous connection state from graveyard
        let saved = self.graveyard.retrieve(&client_id);
        let clean_session = connection.clean;
        let previous_session = saved.is_some();
        let tracker = if !clean_session {
            let saved = saved.map_or(Default::default(), |s| s);
            connection.subscriptions = saved.subscriptions;
            connection.meter = saved.metrics;
            saved.tracker
        } else {
            // Only retrieve metrics in clean session
            let saved = saved.map_or(Default::default(), |s| s);
            connection.meter = saved.metrics;
            connection.meter.subscriptions.clear();
            Default::default()
        };

        let time = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(v) => v.as_millis().to_string(),
            Err(e) => format!("Time error = {:?}", e),
        };

        let event = "connection at ".to_owned() + &time + ", clean = " + &clean_session.to_string();
        connection.meter.push_event(event);
        connection
            .meter
            .push_subscriptions(connection.subscriptions.clone());

        let connection_id = self.connections.insert(connection);
        assert_eq!(self.ibufs.insert(incoming), connection_id);
        assert_eq!(self.obufs.insert(outgoing), connection_id);

        self.connection_map.insert(client_id.clone(), connection_id);
        info!("{:15.15}[I] {:20} id = {}", client_id, "connect", connection_id);

        // Add connection to ready queue if the tracker already has data requests
        if !tracker.is_empty() {
            trace!("{:15.15}[S] {:20}", client_id, "graveyard-reschedule");
            self.readyqueue.push_back(connection_id);
        }
        assert_eq!(self.ackslog.insert(), connection_id);
        assert_eq!(self.trackers.insert(tracker), connection_id);

        // Check if there are multiple data requests on same filter.
        debug_assert!(self.check_tracker_duplicates(connection_id));

        self.ackslog.clear(connection_id);

        // Send connack notification
        // NOTE: Last failed message of previous connection doesn't need to be
        // handled as cursor in data requests of all the subscriptions are rewinded
        // last acked offset during reconnection
        let outgoing = self.obufs.get_mut(connection_id).unwrap();

        // this notifies the `RemoteLink` to forward the connack to network using
        // `connection.handle`, and thus we don't need to add the `connection_id` to `self.readyqueue`.
        info!("{:15.15}[O] {:20} id = {}", outgoing.client_id, "connack", connection_id);
        let ack = ConnectionAck::Success(connection_id, previous_session);
        let message = Notification::ConnectionAck(ack);
        let len = outgoing.push_notification(message);
        let _should_unschedule = if len >= MAX_CHANNEL_CAPACITY - 1 {
            outgoing.push_notification(Notification::Unschedule);
            true
        } else {
            false
        };
        outgoing.handle.try_send(()).ok();
    }

    fn handle_disconnection(&mut self, id: ConnectionId) {
        // Some clients can choose to send Disconnect packet before network disconnection.
        // This will lead to double Disconnect packets in router `events`
        let client_id = match &self.obufs.get(id) {
            Some(v) => v.client_id.clone(),
            None => {
                error!("{:15.15}[E] {:20} id {} is already gone", "", "no-connection", id);
                return;
            }
        };

        info!("{:15.15}[I] {:20} id = {}", client_id, "disconnect", id);

        // Remove connection from router
        let mut connection = self.connections.remove(id);
        let _incoming = self.ibufs.remove(id);
        let _outgoing = self.obufs.remove(id);
        let mut tracker = self.trackers.remove(id);
        self.connection_map.remove(&client_id);
        self.ackslog.remove(id);

        // Don't remove connection id from readyqueue with index. This will
        // remove wrong connection from readyqueue. Instead just leave diconnected
        // connection in readyqueue and allow 'consume()' method to deal with this
        // self.readyqueue.remove(id);

        let inflight_data_requests = self.datalog.clean(id);

        // Remove this connection from subscriptions
        for filter in connection.subscriptions.iter() {
            if let Some(connections) = self.subscription_map.get_mut(filter) {
                connections.remove(&id);
            }
        }

        // Add disconnection event to metrics
        let time = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(v) => v.as_millis().to_string(),
            Err(e) => format!("Time error = {:?}", e),
        };

        let event = "disconnection at ".to_owned() + &time;
        connection.meter.push_event(event);

        // Save state for persistent sessions
        if !connection.clean {
            // Add inflight data requests back to tracker
            inflight_data_requests
                .into_iter()
                .for_each(|r| tracker.register_data_request(r));

            self.graveyard
                .save(&client_id, tracker, connection.subscriptions, connection.meter);
        } else {
            // Only save metrics in clean session
            connection.meter.subscriptions.clear();
            self.graveyard
                .save(&client_id, Tracker::new(), HashSet::new(), connection.meter);
        }
    }

    /// Handles new incoming data on a topic
    fn handle_device_payload(&mut self, id: ConnectionId) {
        let mut data = {
            let incoming = match self.ibufs.get_mut(id) {
                Some(v) => v,
                None => {
                    debug!("{:15.15}[E] {:20} id {} is already gone", "", "no-connection", id);
                    return;
                }
            };
            let data = match incoming.pop() {
                Some(data) => data,
                None => {
                    trace!("{:15.15}[I] {:20}", incoming.client_id, "extra-data-event");
                    return;
                }
            };

            info!("{:15.15}[I] {:20} length = {}", incoming.client_id, "payload", data.len());
            data
        };

        let mut force_ack = false;
        let mut new_data = false;
        let mut disconnect = false;

        loop {
            // Extract packet from raw bytes. Size is dummy (already checked in connection read)
            let packet = match v4::read(&mut data, 1024 * 1024 * 1024) {
                Ok(p) => p,
                Err(v4::Error::InsufficientBytes(2)) => break,
                Err(e) => {
                    error!("{:15.15}[I] read error {:20?}", self.obufs[id].client_id, e);
                    disconnect = true;
                    break;
                }
            };

            match packet {
                Packet::Publish(publish) => {
                    let (_, qos, pkid, _, _) = match publish.view_meta() {
                        Ok(v) => v,
                        Err(e) => {
                            let client_id = &self.obufs[id].client_id;
                            error!("{:15.15}[I] Publish view error = {:?}", client_id, e);
                            disconnect = true;
                            break;
                        }
                    };

                    let size = publish.raw.len();

                    // Try to append publish to commitlog
                    let offset = match self.append_to_commitlog(id, publish) {
                        Ok(offset) => offset,
                        Err(publish) => {
                            let topic = match publish.view_topic() {
                                Ok(v) => v,
                                Err(e) => {
                                    let client_id = &self.obufs[id].client_id;
                                    error!("{:15.15}[I] Publish view error = {:?}", client_id, e);
                                    disconnect = true;
                                    break;
                                }
                            };
                            error!(
                                "{:15.15}[E] {:20} topic = {}",
                                &self.obufs[id].client_id, "no-filter", topic
                            );

                            // If any of the publish doesn't append to commitlog, set global
                            // offset to (0, 0) and prepare disconnection. All the inflight
                            // publishes and prepared acks before disconnection propagates will
                            // remain in state possibly leading to duplicates
                            disconnect = true;
                            break;
                        }
                    };

                    // TODO: fix checking of offsets
                    //
                    // Even if one of the data in the batch is appended to commitlog,
                    // set new data. This triggers notifications to wake waiters.
                    // Note: != (0, 0) is used to set flag because of at least one successful
                    // write. Don't overwrite this flag (e.g last publish of the batch failed)
                    new_data = true;
                    self.router_metrics.total_publishes += 1;

                    // Update metrics
                    if let Some(metrics) = self.connections.get_mut(id).map(|v| &mut v.meter) {
                        metrics.increment_publish_count();
                        metrics.add_publish_size(size);
                    }
                    let meter = &mut self.ibufs.get_mut(id).unwrap().meter;
                    meter.publish_count += 1;
                    meter.total_size += size;
                    meter.update_data_rate(size);

                    // println!("{}, {}", self.router_metrics.total_publishes, pkid);
                    // Prepare acks for the above publish
                    // If any of the publish in the batch results in force flush,
                    // set global force flush flag. Force flush is triggered when the
                    // router is in instant ack more or connection data is from a replica
                    if self.prepare_publish_ack(id, qos, pkid, offset) {
                        force_ack = true;
                    }
                }
                Packet::Subscribe(s) => {
                    let mut return_codes = Vec::new();
                    let pkid = s.pkid;
                    let len = s.len();

                    for f in s.filters {
                        let filter = f.path;
                        let qos = f.qos;

                        if !self.validate_subscription(id, &filter, qos as u8) {
                            disconnect = true;
                            break;
                        }

                        // Update metrics
                        self.connections
                            .get_mut(id)
                            .unwrap()
                            .meter
                            .push_subscription(filter.clone());

                        let (idx, cursor) = self.datalog.next_native_offset(&filter);
                        self.prepare_consumption(id, cursor, idx, filter, qos as u8);

                        return_codes.push(qos as u8);
                    }
                    let meter = &mut self.ibufs.get_mut(id).unwrap().meter;
                    meter.update_data_rate(len);

                    // Prepare acks for the above subscription
                    let f = self.prepare_subscribe_ack(id, pkid, return_codes);

                    // If any of the subscribe in the batch results in force flush,
                    // set global force flush flag. Force flush is triggered for
                    // device subscriptions. Replica subscriptions are not acked
                    if f {
                        force_ack = true;
                    }
                }
                Packet::PubAck(puback) => {
                    let outgoing = self.obufs.get_mut(id).unwrap();
                    let (unsolicited, out_of_order) = outgoing.register_ack(puback.pkid);

                    if unsolicited {
                        error!("{:15.15}[E] {:10} pkid = {:?}", id, "unsolicited-ack", puback.pkid);
                        disconnect = true;
                        break;
                    }

                    if out_of_order {
                        error!("{:15.15}[E] {:10} pkid = {:?}", id, "unordered-ack", puback.pkid);
                        disconnect = true;
                        break;
                    }

                    self.ack_freeslots_reschedule(id);
                }
                Packet::PingReq => {
                    self.handle_connection_pingreq(id);
                    force_ack = true;
                }
                Packet::Disconnect => {
                    disconnect = true;
                    break;
                }
                incoming => {
                    warn!("Packet = {:?} not supported by router yet", incoming);
                }
            }
        }

        // Prepare AcksRequest in tracker if router is operating in a
        // single node mode or force ack request for subscriptions
        if force_ack {
            let tracker = self.trackers.get_mut(id).unwrap();
            tracker.register_acks_request();
            if tracker.caughtup_unschedule() {
                tracker.set_caughtup_unschedule(false);
                self.readyqueue.push_back(id);
                trace!("{:15.15}[S] {:20}", self.obufs[id].client_id, "acks-reschedule");
            }
        }

        // Notify waiting consumers only if there is publish data. During
        // subscription, data request is added to data waiter. With out this
        // if condition, connection will be woken up even during subscription
        if new_data {
            self.connection_data_notification();
        }

        // Incase BytesMut represents 10 packets, publish error/diconnect event
        // on say 5th packet should not block new data notifications for packets
        // 1 - 4. Hence we use a flag instead of diconnecting immediately
        if disconnect {
            self.handle_disconnection(id);
        }
    }

    // TODO: fix the return type
    fn append_to_commitlog(
        &mut self,
        id: ConnectionId,
        publish: Publish,
    ) -> Result<Offset, Publish> {
        let (_, _, pkid, _, _) = publish.view_meta().unwrap();
        let offsets_and_filters = self.datalog.native_append(publish)?;
        for (offset, filter_idx) in &offsets_and_filters {
            debug!(
                "{:15.15}[I] {:20} append = {}[{}, {}), pkid = {}",
                self.obufs[id].client_id,
                "publish",
                // unwrap fine as we just got the filter_idx from datalog itself
                self.datalog.get_topic(*filter_idx).unwrap(),
                offset.0,
                offset.1,
                pkid
            );
        }
        Ok(offsets_and_filters[0].0)
    }

    // TODO: handle multiple offsets
    //
    // The problem with multiple offsets is that when using replication with the current
    // architecture, a single publish might get appended to multiple commit logs, resulting in
    // multiple offsets (see `append_to_commitlog` function), meaning replicas will need to
    // coordinate using multiple offsets, and we don't have any idea how to do so right now.
    // Currently as we don't have replication, we just use a single offset, even when appending to
    // multiple commit logs.
    fn prepare_publish_ack(
        &mut self,
        id: ConnectionId,
        qos: u8,
        pkid: u16,
        offset: Offset,
    ) -> bool {
        // Don't write ackslog for qos1
        if qos == 0 {
            return false;
        }

        // Write to committed log in-case instant ack flag is set or
        // for replicas (replica data should be committed immediately)
        if self.config.instant_ack || id <= MAX_REPLICA_ID {
            let puback = Ack::PubAck(pkid);
            self.ackslog.commit(id, puback);
            true
        } else {
            self.ackslog.push(id, offset, pkid);
            false
        }
    }

    /// Apply filter and prepare this connection to receive subscription data
    fn prepare_consumption(
        &mut self,
        id: ConnectionId,
        cursor: Offset,
        filter_idx: FilterIdx,
        filter: String,
        qos: u8,
    ) {
        // Add connection id to subscription list
        match self.subscription_map.get_mut(&filter) {
            Some(connections) => {
                connections.insert(id);
            }
            None => {
                let mut connections = HashSet::new();
                connections.insert(id);
                self.subscription_map.insert(filter.clone(), connections);
            }
        }

        // Prepare consumer to pull data in case of subscription
        let connection = self.connections.get_mut(id).unwrap();
        let outgoing = self.obufs.get_mut(id).unwrap();

        if !connection.subscriptions.contains(&filter) {
            connection.subscriptions.insert(filter.clone());
            debug!("{:15.15}[I] {:20} filter = {}", outgoing.client_id, "subscribe", filter);
            let tracker = self.trackers.get_mut(id).unwrap();
            let request = DataRequest {
                filter,
                filter_idx,
                qos,
                cursor,
                read_count: 0,
                max_count: 100,
            };

            tracker.register_data_request(request);
            if tracker.caughtup_unschedule() {
                tracker.set_caughtup_unschedule(false);
                self.readyqueue.push_back(id);
                trace!("{:15.15}[S] {:20}", outgoing.client_id, "subscribe-reschedule");
            }

            debug_assert!(self.check_tracker_duplicates(id))
        }
        let meter = &mut self.ibufs.get_mut(id).unwrap().meter;
        meter.subscribe_count += 1;
    }

    fn validate_subscription(&mut self, id: usize, filter: &str, qos: u8) -> bool {
        /*
           TODO:
                At the moment, we don't support unconfigured wildcard subscriptions or overlapping
                subscriptions. A publish only gets written to first matching subscription.
                To avoid bugs where new wildcard subscriptions are preventing other subscriptions
                from receiving data (due to non support of overlapping subscriptions), we
                only support configured wildcard subscriptions
           TODO:
                Support only configured subscriptions (including concrete). Allow dynamic
                configuration only through frontend and console
           TODO:
                Validate configured subscriptions to prevent overlapping subscriptions
        */
        if qos == 2 {
            error!(
                "{:15.15}[E] qos 2 not supported. filter = {}",
                self.obufs[id].client_id, filter
            );
            return false;
        }

        if filter.starts_with("test") || filter.starts_with('$') {
            error!(
                "{:15.15}[E] subscriptions can't start with 'test' or '$'. filter = {}",
                self.obufs[id].client_id, filter
            );
            return false;
        }

        true
    }

    /// Commit to ackslog and enable force ack flag if this
    /// is not a replica. No need to ack replica subscriptions
    fn prepare_subscribe_ack(
        &mut self,
        id: ConnectionId,
        pkid: u16,
        return_codes: Vec<u8>,
    ) -> bool {
        if id > MAX_REPLICA_ID {
            let suback = Ack::SubAck(pkid, return_codes);
            self.ackslog.commit(id, suback);
            true
        } else {
            false
        }
    }

    fn handle_connection_pingreq(&mut self, id: ConnectionId) -> bool {
        let pingresp = Ack::PingResp;
        self.ibufs
            .get_mut(id)
            .unwrap()
            .meter
            .average_last_data_rate();
        self.obufs
            .get_mut(id)
            .unwrap()
            .meter
            .average_last_data_rate();
        self.ackslog.commit(id, pingresp);
        true
    }

    fn retrieve_metrics(&mut self, id: ConnectionId, metrics: MetricsRequest) {
        info!("{:15.15}[I] {:20} request = {:?}", self.obufs[id].client_id, "metrics", metrics);
        let message = match metrics {
            MetricsRequest::Config => MetricsReply::Config(self.config.clone()),
            MetricsRequest::Router => MetricsReply::Router(self.router_metrics.clone()),
            MetricsRequest::Connection(id) => {
                let metrics = self.connection_map.get(&id).map(|v| {
                    let c = self.connections.get(*v).map(|v| v.meter.clone()).unwrap();
                    let t = self.trackers.get(*v).cloned().unwrap();
                    (c, t)
                });

                let metrics = match metrics {
                    Some(v) => Some(v),
                    None => self.graveyard.retrieve(&id).map(|v| (v.metrics, v.tracker)),
                };

                MetricsReply::Connection(metrics)
            }
            MetricsRequest::Subscriptions => {
                let metrics: HashMap<Filter, Vec<String>> = self
                    .subscription_map
                    .iter()
                    .map(|v| {
                        let filter = v.0.clone();
                        let connections =
                            v.1.iter()
                                .map(|v| self.obufs[*v].client_id.clone())
                                .collect();

                        (filter, connections)
                    })
                    .collect();

                MetricsReply::Subscriptions(metrics)
            }
            MetricsRequest::Subscription(filter) => {
                let metrics = self.datalog.meter(&filter);
                MetricsReply::Subscription(metrics)
            }
            MetricsRequest::Waiters(filter) => {
                let metrics = self.datalog.waiters(&filter).map(|v| {
                    // Convert (connection id, data request) list to (device id, data request) list
                    v.waiters()
                        .iter()
                        .map(|(id, request)| (self.obufs[*id].client_id.clone(), request.clone()))
                        .collect()
                });

                MetricsReply::Waiters(metrics)
            }
            MetricsRequest::ReadyQueue => {
                let metrics = self.readyqueue.clone();
                MetricsReply::ReadyQueue(metrics)
            }
        };

        let connection = self.connections.get_mut(id).unwrap();
        connection.metrics.try_send(message).ok();
    }

    fn retrieve_shadow(&mut self, id: ConnectionId, shadow: ShadowRequest) {
        if let Some(reply) = self.datalog.shadow(&shadow.filter) {
            let publish: Result<Publish, v4::Error> = PublishBytes(reply).into();
            let publish = publish.unwrap();
            let (topic, payload) = publish.take_topic_and_payload().unwrap();
            let topic = std::str::from_utf8(&topic).unwrap().to_owned();
            let shadow_reply = ShadowReply { topic, payload };
            let outgoing = self.obufs.get_mut(id).unwrap();

            // FIll notify shadow
            let message = Notification::Shadow(shadow_reply);
            let len = outgoing.push_notification(message);
            let _should_unschedule = if len >= MAX_CHANNEL_CAPACITY - 1 {
                outgoing.push_notification(Notification::Unschedule);
                true
            } else {
                false
            };
            outgoing.handle.try_send(()).ok();
        }
    }

    /// When a connection is ready, it should sweep native data from 'datalog',
    /// send data and notifications to consumer.
    /// To activate a connection, first connection's tracker is fetched and
    /// all the requests are handled.
    fn consume(&mut self, id: ConnectionId) {
        let outgoing = match self.obufs.get_mut(id) {
            Some(v) => v,
            None => {
                debug!("{:15.15}[E] {:20} id {} is already gone", "", "no-connection", id);
                return;
            }
        };

        trace!("{:15.15}[S] {:20} id = {}", outgoing.client_id, "consume", id);

        let tracker = self.trackers.get_mut(id).unwrap();
        let datalog = &mut self.datalog;
        let ackslog = &mut self.ackslog;

        if tracker.next_acks_request().is_some() {
            let busy = if id > MAX_REPLICA_ID {
                // forward device acks
                trace!("{:15.15}[T] {:20}", &outgoing.client_id, "acks-request");
                let acks = ackslog.readv(id);
                trace!("{:15.15}[T] {:20}", &outgoing.client_id, "acks-response");

                // Acks for this connection are not ready. Register current request in waiters
                let (busy, done) = if acks.is_empty() {
                    (false, true)
                } else {
                    let should_unschedule = {
                        let mut count = 0;
                        let mut buffer = outgoing.buffer.lock();

                        for ack in acks.drain(..) {
                            trace!(
                                "{:15.15}[O] {:20} packet = {:?}",
                                outgoing.client_id,
                                "ack",
                                packetid(&ack)
                            );
                            let message = Notification::DeviceAck(ack);
                            buffer.push_back(message);
                            count += 1;
                        }

                        info!(
                            "{:15.15}[O] {:20} count = {:?}",
                            outgoing.client_id, "acks-reply", count
                        );
                        if count >= MAX_CHANNEL_CAPACITY - 1 {
                            buffer.push_back(Notification::Unschedule);
                            true
                        } else {
                            false
                        }
                    };

                    outgoing.handle.try_send(()).ok();
                    (should_unschedule, false)
                };

                // When all the data in the log is caught up, current request is
                // registered in waiters and not added back to the tracker. This
                // ensures that tracker.next() stops when all the requests are done
                if done {
                    trace!("{:15.15}[T] {:20}", outgoing.client_id, "acks-done");
                } else {
                    tracker.register_acks_request();
                }

                busy
            } else {
                unimplemented!();
            };

            if busy {
                trace!("{:15.15}[S] {:20}", outgoing.client_id, "busy-unschedule");
                tracker.set_busy_unschedule(true);
                return;
            }
        }

        // A new connection's tracker is always initialized with acks request.
        // A subscribe will register data request.
        // So a new connection is always scheduled with at least one request
        for _ in 0..MAX_SCHEDULE_ITERATIONS {
            let mut request = match tracker.next_data_request() {
                // Handle next data or acks request
                Some(request) => request,
                // No requests in the queue. This implies that consumer data and
                // acks are completely caught up. Pending requests are registered
                // in waiters and awaiting new notifications (device or replica data)
                None => {
                    trace!("{:15.15}[S] {:20}", outgoing.client_id, "done-unschedule");
                    tracker.set_caughtup_unschedule(true);
                    return;
                }
            };

            let status = if id > MAX_REPLICA_ID {
                forward_device_data(&mut request, datalog, outgoing)
            } else {
                unimplemented!();
                // forward_replica_data(&request, datalog, conn)
            };

            let busy = status.0;
            let done = status.1;
            let inflight_full = status.2;

            // When all the data in the log is caught up, current request is
            // registered in waiters and not added back to the tracker. This
            // ensures that tracker.next() stops when all the requests are done
            if done {
                trace!(
                    "{:15.15}[S] {:20} f = {}",
                    outgoing.client_id,
                    "caughtup-park",
                    request.filter
                );
                datalog.park(id, request);
            } else {
                tracker.register_data_request(request);
            }

            if busy {
                trace!("{:15.15}[S] {:20}", outgoing.client_id, "busy-unschedule");
                tracker.set_busy_unschedule(true);
                return;
            }

            if inflight_full {
                trace!("{:15.15}[S] {:20}", outgoing.client_id, "inflight-full");
                tracker.set_inflight_full_unschedule(true);
                return;
            }
        }

        // If there are more requests in the tracker, reschedule this connection
        debug!("{:15.15}[S] {:20}", outgoing.client_id, "pending-reschedule");
        self.readyqueue.push_back(id);
    }

    /// Prepare all the consumers which are waiting for new data
    fn connection_data_notification(&mut self) {
        while let Some((id, request)) = self.datalog.pop_notification() {
            let tracker = self.trackers.get_mut(id).unwrap();

            // If connection is removed from ready queue because of 0 requests,
            // add connection back to ready queue.
            tracker.register_data_request(request);
            // If connection is removed from ready queue because of 0 requests,
            // add connection back to ready queue.
            if tracker.caughtup_unschedule() {
                tracker.set_caughtup_unschedule(false);
                self.readyqueue.push_back(id);
                trace!("{:15.15}[S] {:20}", self.obufs[id].client_id, "newdata-reschedule");
            }
        }
    }
}

/// Sweep datalog from offset in DataRequest and updates DataRequest
/// for next sweep. Returns (busy, caughtup) status
/// Returned arguments:
/// 1. `busy`: whether the data request was completed or not.
/// 2. `done`: whether the connection was busy or not.
/// 3. `inflight_full`: whether the inflight requests were completely filled
fn forward_device_data(
    request: &mut DataRequest,
    datalog: &DataLog,
    outgoing: &mut Outgoing,
) -> (bool, bool, bool) {
    trace!(
        "{:15.15}[T] {:20} cursor = {}[{}, {}]",
        outgoing.client_id,
        "data-request",
        request.filter,
        request.cursor.0,
        request.cursor.1
    );

    let len = if request.qos == 1 {
        let len = outgoing.free_slots();
        if len == 0 {
            return (false, false, true);
        }

        len as u64
    } else {
        datalog.config.max_read_len
    };

    let (next, publishes) = match datalog.native_readv(request.filter_idx, request.cursor, len) {
        Ok(v) => v,
        Err(e) => {
            error!("Failed to read from commitlog. Error = {:?}", e);
            return (false, true, false);
        }
    };

    let (start, next, caughtup) = match next {
        Position::Next { start, end } => (start, end, false),
        Position::Done { start, end } => (start, end, true),
    };

    if start != request.cursor {
        error!("Read cursor jump. Cursor = {:?}, Start = {:?}", request.cursor, start);
    }

    trace!(
        "{:15.15}[T] {:20} cursor = {}[{}, {})",
        outgoing.client_id,
        "data-response",
        request.filter,
        next.0,
        next.1,
    );

    let qos = request.qos;
    let filter_idx = request.filter_idx;
    request.read_count += publishes.len();
    request.cursor = next;
    // println!("{:?} {:?} {}", start, next, request.read_count);

    if publishes.is_empty() {
        return (false, caughtup, false);
    }

    // Fill and notify device data
    info!("{:15.15}[O] {:20} length = {:?}", outgoing.client_id, "data-proxy", publishes.len());
    let len = outgoing.push_device_data(publishes, qos, filter_idx, next);
    let pause = if len >= MAX_CHANNEL_CAPACITY - 1 {
        outgoing.push_notification(Notification::Unschedule);
        true
    } else {
        false
    };

    outgoing.handle.try_send(()).ok();

    (pause, caughtup, false)
}

#[cfg(test)]
#[allow(non_snake_case)]
mod test {
    use std::{
        thread,
        time::{Duration, Instant},
    };

    use bytes::BytesMut;

    use super::*;
    use crate::{
        link::local::*,
        protocol::v4::{self, subscribe::SubscribeFilter, QoS},
        router::ConnectionMeter,
    };

    /// Create a router and n connections
    fn new_router(count: usize, clean: bool) -> (Router, VecDeque<Link>) {
        let config = RouterConfig {
            instant_ack: true,
            max_segment_size: 1024 * 10, // 10 KB
            max_mem_segments: 10,
            max_disk_segments: 0,
            max_read_len: 128,
            log_dir: None,
            max_connections: 128,
        };

        let mut router = Router::new(0, config);
        let link = router.link();
        let handle = thread::spawn(move || {
            (0..count)
                .map(|i| Link::new(&format!("link-{}", i), link.clone(), clean))
                .collect::<VecDeque<Result<_, _>>>()
        });

        router.run_exact(count).unwrap();
        let links = handle
            .join()
            .unwrap()
            .into_iter()
            .map(|x| x.unwrap())
            .collect();
        (router, links)
    }

    fn reconnect(router: &mut Router, link_id: usize, clean: bool) -> Link {
        let link = router.link();
        let handle =
            thread::spawn(move || Link::new(&format!("link-{}", link_id), link, clean).unwrap());
        router.run(1).unwrap();
        handle.join().unwrap()
    }

    #[test]
    fn test_graveyard_retreive_metrics_always() {
        let (mut router, mut links) = new_router(1, false);
        let Link(tx, _) = links.pop_front().unwrap();
        let id = tx.connection_id;
        let conn = router.connections.get_mut(id).unwrap();
        conn.meter = ConnectionMeter {
            publish_size: 1000,
            publish_count: 1000,
            subscriptions: HashSet::new(),
            events: conn.meter.events.clone(),
        };
        conn.meter.push_event("graveyard-testing".to_string());
        router.handle_disconnection(id);

        let Link(tx, _) = reconnect(&mut router, 0, false);
        let id = tx.connection_id;
        let conn = router.connections.get_mut(id).unwrap();
        assert_eq!(conn.meter.publish_size, 1000);
        assert_eq!(conn.meter.publish_count, 1000);
        assert_eq!(conn.meter.events.len(), 4);

        let (mut router, mut links) = new_router(1, true);
        let Link(tx, _) = links.pop_front().unwrap();
        let id = tx.connection_id;
        let conn = router.connections.get_mut(id).unwrap();
        conn.meter = ConnectionMeter {
            publish_size: 1000,
            publish_count: 1000,
            subscriptions: HashSet::new(),
            events: conn.meter.events.clone(),
        };
        conn.meter.push_event("graveyard-testing".to_string());
        router.handle_disconnection(id);

        let Link(tx, _) = reconnect(&mut router, 0, true);
        let id = tx.connection_id;
        let conn = router.connections.get_mut(id).unwrap();
        assert_eq!(conn.meter.publish_size, 1000);
        assert_eq!(conn.meter.publish_count, 1000);
        assert_eq!(conn.meter.events.len(), 4);
    }

    #[test]
    #[should_panic]
    fn test_blocking_too_many_connections() {
        new_router(512, true);
    }

    #[test]
    fn test_not_clean_sessions() {
        // called once per data push (subscribe to 2 topics in our case)
        // called once per each sub topic
        // if prepare_data_request called, then Tracker::register_data_request is also called so
        // no need to check separately for that

        let (mut router, mut links) = new_router(2, false);
        let Link(tx1, mut rx1) = links.pop_front().unwrap();
        let id1 = tx1.connection_id;
        let Link(tx2, mut rx2) = links.pop_front().unwrap();
        let id2 = tx2.connection_id;

        // manually subscribing to avoid calling Router::run(), so that we can see and test changes
        // in Router::readyqueue.
        let mut buf = BytesMut::new();
        v4::subscribe::write(
            vec![
                SubscribeFilter::new("hello/1/world".to_string(), QoS::AtMostOnce),
                SubscribeFilter::new("hello/2/world".to_string(), QoS::AtMostOnce),
            ],
            0,
            &mut buf,
        )
        .unwrap();
        let buf = buf.freeze();
        {
            let incoming = router.ibufs.get_mut(id1).unwrap();
            let mut recv_buf = incoming.buffer.lock();
            recv_buf.push_back(buf.clone());
            assert_eq!(router.subscription_map.len(), 0);
        }
        {
            let incoming = router.ibufs.get_mut(id2).unwrap();
            let mut recv_buf = incoming.buffer.lock();
            recv_buf.push_back(buf.clone());
            assert_eq!(router.subscription_map.len(), 0);
        }

        router.handle_device_payload(id1);
        assert_eq!(router.subscription_map.len(), 2);
        assert_eq!(router.readyqueue.len(), 1);
        router.consume(id1);
        assert_eq!(
            router
                .datalog
                .waiters(&"hello/1/world".to_string())
                .unwrap()
                .waiters()
                .len(),
            1
        );
        assert_eq!(
            router
                .datalog
                .waiters(&"hello/2/world".to_string())
                .unwrap()
                .waiters()
                .len(),
            1
        );

        router.handle_device_payload(id2);
        assert_eq!(router.subscription_map.len(), 2);
        assert_eq!(router.readyqueue.len(), 2);
        router.consume(id2);
        assert_eq!(
            router
                .datalog
                .waiters(&"hello/1/world".to_string())
                .unwrap()
                .waiters()
                .len(),
            2
        );
        assert_eq!(
            router
                .datalog
                .waiters(&"hello/2/world".to_string())
                .unwrap()
                .waiters()
                .len(),
            2
        );

        assert!(matches!(rx1.recv().unwrap(), Some(Notification::DeviceAck(_))));
        assert!(matches!(rx2.recv().unwrap(), Some(Notification::DeviceAck(_))));

        router.handle_disconnection(id1);
        router.handle_disconnection(id2);
        router.run(1).unwrap();

        let Link(_, _) = reconnect(&mut router, 0, false);
        assert_eq!(
            router
                .datalog
                .waiters(&"hello/1/world".to_string())
                .unwrap()
                .waiters()
                .len(),
            1
        );
        assert_eq!(
            router
                .datalog
                .waiters(&"hello/2/world".to_string())
                .unwrap()
                .waiters()
                .len(),
            1
        );

        let Link(_, _) = reconnect(&mut router, 1, false);
        assert_eq!(
            router
                .datalog
                .waiters(&"hello/1/world".to_string())
                .unwrap()
                .waiters()
                .len(),
            2
        );
        assert_eq!(
            router
                .datalog
                .waiters(&"hello/2/world".to_string())
                .unwrap()
                .waiters()
                .len(),
            2
        );
    }

    #[test]
    fn test_publish_appended_to_commitlog() {
        let (mut router, mut links) = new_router(2, true);
        let Link(mut sub_tx, _) = links.pop_front().unwrap();
        let Link(mut pub_tx, _) = links.pop_front().unwrap();

        sub_tx.subscribe("hello/1/world").unwrap();
        router.run(1).unwrap();

        // Each LinkTx::publish creates a single new event
        for _ in 0..10 {
            pub_tx.publish("hello/1/world", b"hello".to_vec()).unwrap();
        }
        router.run(1).unwrap();
        assert_eq!(router.datalog.next_native_offset("hello/1/world").1, (0, 10));
    }

    #[test]
    fn test_adding_caughtups_to_waiters() {
        // number of times forward_device_data hit
        //     = router.run times * topics * subscribers per topic
        //     = 2 * 2 * 2 = 8
        //
        // we also call consume 2 times per subsriber, and one time it will call
        // forward_device_data again, but no data will actually be forwarded.
        // as 256 publishes at once, and router's config only reads 128 at a time, data needs to be
        // read from same log twice, and thus half of the times the data reading is not done

        let (mut router, mut links) = new_router(4, true);
        let Link(mut sub1_tx, _) = links.pop_front().unwrap();
        let Link(mut pub1_tx, _) = links.pop_front().unwrap();
        let Link(mut sub2_tx, _) = links.pop_front().unwrap();
        let Link(mut pub2_tx, _) = links.pop_front().unwrap();
        sub1_tx.subscribe("hello/1/world").unwrap();
        sub1_tx.subscribe("hello/2/world").unwrap();
        sub2_tx.subscribe("hello/1/world").unwrap();
        sub2_tx.subscribe("hello/2/world").unwrap();
        router.run(1).unwrap();

        assert_eq!(
            router
                .datalog
                .waiters(&"hello/1/world".to_string())
                .unwrap()
                .waiters()
                .len(),
            2
        );
        assert_eq!(
            router
                .datalog
                .waiters(&"hello/2/world".to_string())
                .unwrap()
                .waiters()
                .len(),
            2
        );

        for _ in 0..256 {
            pub1_tx.publish("hello/1/world", b"hello".to_vec()).unwrap();
            pub2_tx.publish("hello/2/world", b"hello".to_vec()).unwrap();
        }
        router.run(1).unwrap();

        let tracker = router.trackers.get(sub1_tx.connection_id).unwrap();
        assert_eq!(tracker.get_data_requests().len(), 2);
        let tracker = router.trackers.get(sub2_tx.connection_id).unwrap();
        assert_eq!(tracker.get_data_requests().len(), 2);

        router.run(1).unwrap();

        let tracker = router.trackers.get(sub1_tx.connection_id).unwrap();
        assert_eq!(tracker.get_data_requests().len(), 2);
        let tracker = router.trackers.get(sub2_tx.connection_id).unwrap();
        assert_eq!(tracker.get_data_requests().len(), 2);

        router.consume(sub1_tx.connection_id);
        router.consume(sub2_tx.connection_id);

        let tracker = router.trackers.get(sub1_tx.connection_id).unwrap();
        assert_eq!(tracker.get_data_requests().len(), 1);
        let tracker = router.trackers.get(sub2_tx.connection_id).unwrap();
        assert_eq!(tracker.get_data_requests().len(), 1);

        router.consume(sub1_tx.connection_id);
        router.consume(sub2_tx.connection_id);

        let tracker = router.trackers.get(sub1_tx.connection_id).unwrap();
        assert_eq!(tracker.get_data_requests().len(), 0);
        let tracker = router.trackers.get(sub2_tx.connection_id).unwrap();
        assert_eq!(tracker.get_data_requests().len(), 0);

        assert_eq!(
            router
                .datalog
                .waiters(&"hello/1/world".to_string())
                .unwrap()
                .waiters()
                .len(),
            2
        );
        assert_eq!(
            router
                .datalog
                .waiters(&"hello/2/world".to_string())
                .unwrap()
                .waiters()
                .len(),
            2
        );
    }

    #[test]
    fn test_disconnect_invalid_sub() {
        let (mut router, mut links) = new_router(3, true);

        let Link(mut tx, mut rx) = links.pop_front().unwrap();
        tx.subscribe("$hello/world").unwrap();
        router.run(1).unwrap();
        assert!(matches!(rx.recv(), Err(LinkError::Recv(flume::RecvError::Disconnected))));

        let Link(mut tx, mut rx) = links.pop_front().unwrap();
        tx.subscribe("test/hello").unwrap();
        router.run(1).unwrap();
        assert!(matches!(rx.recv(), Err(LinkError::Recv(flume::RecvError::Disconnected))));
    }

    #[test]
    fn test_disconnect_invalid_pub() {
        let (mut router, mut links) = new_router(1, true);
        let Link(mut tx, mut rx) = links.pop_front().unwrap();
        tx.publish("invalid/topic", b"hello".to_vec()).unwrap();
        router.run(1).unwrap();
        assert!(matches!(rx.recv(), Err(LinkError::Recv(flume::RecvError::Disconnected))));
    }

    #[test]
    fn test_pingreq() {
        let (mut router, mut links) = new_router(1, true);
        let Link(mut tx, mut rx) = links.pop_front().unwrap();
        let mut buf = BytesMut::new();
        mqttbytes::v4::PingReq.write(&mut buf).unwrap();
        let buf = buf.freeze();
        for _ in 0..10 {
            tx.push(buf.clone()).unwrap();
        }
        router.run(1).unwrap();
        for _ in 0..10 {
            let ret = rx.recv().unwrap().unwrap();
            assert!(matches!(ret, Notification::DeviceAck(Ack::PingResp)));
        }
    }

    #[test]
    fn test_disconnect() {
        let (mut router, mut links) = new_router(1, true);
        let Link(mut tx, mut rx) = links.pop_front().unwrap();
        let mut buf = BytesMut::new();
        mqttbytes::v4::Disconnect.write(&mut buf).unwrap();
        let buf = buf.freeze();
        tx.push(buf).unwrap();
        router.run(1).unwrap();
        assert!(matches!(rx.recv(), Err(LinkError::Recv(flume::RecvError::Disconnected))));
    }

    #[test]
    fn test_connection_in_qos1() {
        // connection as in crate::connection::Connection struct should get filled for all unacked,
        // and refuse to add more packets than unacked.

        let config = RouterConfig {
            instant_ack: true,
            max_segment_size: 1024 * 10, // 10 KB
            max_mem_segments: 10,
            max_disk_segments: 0,
            max_read_len: 256,
            log_dir: None,
            max_connections: 128,
        };

        let mut router = Router::new(0, config);
        let link = router.link();
        let handle = thread::spawn(move || {
            (0..2)
                .map(|i| Link::new(&format!("link-{}", i), link.clone(), true))
                .collect::<VecDeque<Result<_, _>>>()
        });

        router.run(2).unwrap();
        let mut links: VecDeque<Link> = handle
            .join()
            .unwrap()
            .into_iter()
            .map(|x| x.unwrap())
            .collect();

        let Link(mut sub_tx, _sub_rx) = links.pop_front().unwrap();
        let Link(mut pub_tx, _) = links.pop_front().unwrap();

        let mut buf = BytesMut::new();
        mqttbytes::v4::Subscribe::new("hello/1/world", mqttbytes::QoS::AtLeastOnce)
            .write(&mut buf)
            .unwrap();
        let buf = buf.freeze();
        sub_tx.push(buf).unwrap();
        router.run(1).unwrap();

        for _ in 0..202 {
            pub_tx.publish("hello/1/world", b"hello".to_vec()).unwrap();
        }
        router.run(1).unwrap();

        let id = sub_tx.connection_id;
        let tracker = router.trackers.get(id).unwrap();
        assert_eq!(tracker.get_data_requests().front().unwrap().cursor, (0, 200));
    }

    #[test]
    #[ignore]
    fn test_resend_in_qos1() {
        unimplemented!("we don't resend QoS1 packets yet")
    }

    #[test]
    fn test_wildcard_subs() {
        let (mut router, mut links) = new_router(3, true);

        let Link(mut sub1_tx, mut sub1_rx) = links.pop_front().unwrap();
        sub1_tx.subscribe("#").unwrap();

        let Link(mut sub2_tx, mut sub2_rx) = links.pop_front().unwrap();
        sub2_tx.subscribe("hello/+/world").unwrap();

        let Link(mut pub_tx, mut _pub_rx) = links.pop_front().unwrap();
        for _ in 0..10 {
            for i in 0..10 {
                pub_tx
                    .publish(format!("hello/{}/world", i), b"hello".to_vec())
                    .unwrap();
            }
        }

        for _ in 0..10 {
            pub_tx.publish("hello/world", b"hello".to_vec()).unwrap();
        }

        router.run(1).unwrap();

        let mut count = 0;
        while let Ok(Some(notification)) =
            sub1_rx.recv_deadline(Instant::now() + Duration::from_millis(1))
        {
            match notification {
                Notification::Forward { .. } => count += 1,
                _ => {}
            }
        }
        assert_eq!(count, 110);

        count = 0;
        while let Ok(Some(notification)) =
            sub2_rx.recv_deadline(Instant::now() + Duration::from_millis(1))
        {
            match notification {
                Notification::Forward { .. } => count += 1,
                _ => {}
            }
        }
        assert_eq!(count, 100);
    }
}

// #[cfg(test)]
// mod test {
//     use crate::connection::Connection;
//     use crate::link::local::{Link, LinkRx, LinkTx};
//     use crate::tracker::Tracker;
//     use crate::{ConnectionId, Notification, Router, RouterConfig};
//     use bytes::BytesMut;
//     use jackiechan::Receiver;
//     use mqttbytes::v4::{Publish, Subscribe};
//     use mqttbytes::QoS;
//     use std::collections::VecDeque;
//     use std::sync::atomic::AtomicBool;
//     use std::sync::Arc;
//     use std::thread;

//     /// Create a router and n connections
//     fn router(count: usize) -> (Router, VecDeque<(LinkTx, LinkRx)>) {
//         let config = RouterConfig {
//             data_filter: "hello/world".to_owned(),
//             wildcard_filters: vec![],
//             instant_ack: true,
//             max_segment_size: 10 * 1024,
//             max_segment_count: 10 * 1024,
//             max_connections: 10,
//         };

//         let mut router = Router::new(0, config);
//         let link = router.link();
//         let handle = thread::spawn(move || {
//             (0..count)
//                 .map(|i| Link::new(&format!("link-{}", i), link.clone()).unwrap())
//                 .collect()
//         });

//         router.run(count).unwrap();
//         let links = handle.join().unwrap();
//         (router, links)
//     }

//     /// Creates a connection
//     fn connection(client_id: &str, clean: bool) -> (Connection, Receiver<Notification>) {
//         let (tx, rx) = jackiechan::bounded(10);
//         let connection = Connection::new(client_id, clean, tx, Arc::new(AtomicBool::new(false)));
//         (connection, rx)
//     }

//     /// Raw publish message
//     fn publish(topic: &str) -> BytesMut {
//         let mut publish = Publish::new(topic, QoS::AtLeastOnce, vec![1, 2, 3]);
//         publish.pkid = 1;

//         let mut o = BytesMut::new();
//         publish.write(&mut o).unwrap();
//         o
//     }

//     /// Raw publish message
//     fn subscribe(topic: &str) -> BytesMut {
//         let mut subscribe = Subscribe::new(topic, QoS::AtLeastOnce);
//         subscribe.pkid = 1;

//         let mut o = BytesMut::new();
//         subscribe.write(&mut o).unwrap();
//         o
//     }

//     /// When there is new data on a subscription's commitlog, it's
//     /// consumer should start triggering data requests to pull data
//     /// from commitlog
//     #[test]
//     fn new_connection_data_triggers_acks_of_self_and_data_of_consumer() {
//         let (mut router, mut links) = router(2);
//         let (mut l1tx, _l1rx) = links.pop_front().unwrap();
//         let (mut l2tx, l2rx) = links.pop_front().unwrap();

//         l2tx.subscribe("hello/world").unwrap();
//         l1tx.publish("hello/world", vec![1, 2, 3]).unwrap();
//         let _ = router.run(1);

//         // Suback
//         match l2rx.recv().unwrap() {
//             Notification::DeviceAcks(_) => {}
//             v => panic!("{:?}", v),
//         }

//         // Consumer data
//         match l2rx.recv().unwrap() {
//             Notification::DeviceData { cursor, .. } => assert_eq!(cursor, (0, 1)),
//             v => panic!("{:?}", v),
//         }

//         // No acks for qos0 publish
//     }

//     #[test]
//     fn half_open_connections_are_handled_correctly() {
//         let config = RouterConfig {
//             data_filter: "hello/world".to_owned(),
//             wildcard_filters: vec![],
//             instant_ack: true,
//             max_segment_size: 10 * 1024,
//             max_segment_count: 10 * 1024,
//             max_connections: 10,
//         };

//         let mut router = Router::new(0, config);
//         let (c, rx) = connection("test", true);
//         router.handle_new_connection(c);

//         let id = *router.connection_map.get("test").unwrap();
//         router.handle_device_payload(id, subscribe("hello/data"));
//         router.handle_device_payload(id, publish("hello/data"));

//         let trackers = router.trackers.get(id).unwrap();
//         assert!(trackers.get_data_requests().len() > 0);
//         dbg!(trackers);

//         // A new connection with same client id
//         let (c, rx) = connection("test", true);
//         router.handle_new_connection(c);

//         let id = *router.connection_map.get("test").unwrap();
//         let trackers = router.trackers.get(id).unwrap();
//         dbg!(trackers);
//     }
// }
