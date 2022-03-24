use crate::protocol::v4::subscribe::{self, SubscribeFilter};
use crate::protocol::v4::{self, pingresp, puback, publish, suback, QoS};
use crate::router::{
    iobufs::{Incoming, Outgoing},
    Ack, Connection, ConnectionAck, Event, MetricsReply, Notification, WebsocketRequest,
};
use crate::ConnectionId;
use bytes::{Bytes, BytesMut};
use flume::{Receiver, RecvError, RecvTimeoutError, SendError, Sender, TrySendError};
use parking_lot::Mutex;

use std::collections::VecDeque;
use std::mem;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, thiserror::Error)]
pub enum LinkError {
    #[error("Unexpected router message")]
    NotConnectionAck(Box<Notification>),
    #[error("ConnAck error {0}")]
    ConnectionAck(String),
    #[error("Channel try send error")]
    TrySend(#[from] TrySendError<(ConnectionId, Event)>),
    #[error("Channel send error")]
    Send(#[from] SendError<(ConnectionId, Event)>),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
    #[error("Channel timeout recv error")]
    RecvTimeout(#[from] RecvTimeoutError),
    #[error("Mqtt error = {0}")]
    Mqtt(#[from] v4::Error),
    #[error("Timeout = {0}")]
    Elapsed(#[from] tokio::time::error::Elapsed),
}

pub struct Link(pub LinkTx, pub LinkRx);

impl Link {
    pub fn new(
        client_id: &str,
        router_tx: Sender<(ConnectionId, Event)>,
        clean: bool,
    ) -> Result<Self, LinkError> {
        // Connect to router
        // Local connections to the router shall have access to all subscriptions
        let (connection, metrics_rx) = Connection::new(clean);
        let incoming = Incoming::new(client_id.to_string());
        let (outgoing, link_rx) = Outgoing::new(client_id.to_string());
        let outgoing_data_buffer = outgoing.buffer();
        let incoming_data_buffer = incoming.buffer();

        let message = (
            0,
            Event::Connect {
                connection: Box::new(connection),
                incoming,
                outgoing: Box::new(outgoing),
            },
        );
        router_tx.try_send(message)?;

        link_rx.recv()?;
        let notification = outgoing_data_buffer.lock().pop_front().unwrap();

        // Right now link identifies failure with dropped rx in router, which is probably ok
        // We need this here to get id assigned by router
        let id = match notification {
            Notification::ConnectionAck(ack) => match ack {
                ConnectionAck::Success(id, _) => id,
                ConnectionAck::Failure(reason) => return Err(LinkError::ConnectionAck(reason)),
            },
            message => return Err(LinkError::NotConnectionAck(Box::new(message))),
        };

        let tx = LinkTx::new(id, router_tx.clone(), incoming_data_buffer);
        let rx = LinkRx::new(id, router_tx, link_rx, metrics_rx, outgoing_data_buffer);
        Ok(Link(tx, rx))
    }
}

pub struct LinkTx {
    pub(crate) connection_id: ConnectionId,
    router_tx: Sender<(ConnectionId, Event)>,
    recv_buffer: Arc<Mutex<VecDeque<Bytes>>>,
}

impl LinkTx {
    pub fn new(
        connection_id: ConnectionId,
        router_tx: Sender<(ConnectionId, Event)>,
        recv_buffer: Arc<Mutex<VecDeque<Bytes>>>,
    ) -> LinkTx {
        LinkTx {
            connection_id,
            router_tx,
            recv_buffer,
        }
    }

    /// Send raw device data
    pub fn push(&mut self, data: Bytes) -> Result<usize, LinkError> {
        let len = {
            let mut buffer = self.recv_buffer.lock();
            buffer.push_back(data);
            buffer.len()
        };

        self.router_tx
            .send((self.connection_id, Event::DeviceData))?;

        Ok(len)
    }

    pub fn try_push(&mut self, data: Bytes) -> Result<usize, LinkError> {
        let len = {
            let mut buffer = self.recv_buffer.lock();
            buffer.push_back(data);
            buffer.len()
        };

        self.router_tx
            .try_send((self.connection_id, Event::DeviceData))?;
        Ok(len)
    }

    /// Send raw device data
    pub(crate) async fn send(&mut self, data: Bytes) -> Result<usize, LinkError> {
        let len = {
            let mut buffer = self.recv_buffer.lock();
            buffer.push_back(data);
            buffer.len()
        };

        self.router_tx
            .send_async((self.connection_id, Event::DeviceData))
            .await?;

        // TODO: Remote item in buffer after failure and write unittest
        Ok(len)
    }

    /// Sends a MQTT Publish to the router
    pub fn publish<S, V>(&mut self, topic: S, payload: V) -> Result<usize, LinkError>
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        let mut data = BytesMut::with_capacity(1024);
        publish::write(
            &topic.into(),
            QoS::AtMostOnce,
            0,
            false,
            false,
            &payload.into(),
            &mut data,
        )?;

        let len = self.push(data.freeze())?;
        Ok(len)
    }

    /// Sends a MQTT Publish to the router
    pub fn try_publish<S, V>(&mut self, topic: S, payload: V) -> Result<usize, LinkError>
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        let mut data = BytesMut::with_capacity(1024);
        publish::write(
            &topic.into(),
            QoS::AtMostOnce,
            0,
            false,
            false,
            &payload.into(),
            &mut data,
        )?;

        let len = self.try_push(data.freeze())?;
        // TODO: Remote item in buffer after failure and write unittest
        Ok(len)
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub fn subscribe<S: Into<String>>(&mut self, filter: S) -> Result<usize, LinkError> {
        let filters = vec![SubscribeFilter::new(filter.into(), QoS::AtMostOnce)];
        let mut data = BytesMut::with_capacity(1024);
        subscribe::write(filters, 1, &mut data)?;

        let len = self.push(data.freeze())?;
        Ok(len)
    }

    /// Request to get Websocket
    pub fn websocket<S: Into<String>>(&mut self, filter: S) -> Result<(), LinkError> {
        let message = Event::Websocket(WebsocketRequest {
            filter: filter.into(),
        });

        self.router_tx.try_send((self.connection_id, message))?;
        Ok(())
    }
}

pub struct LinkRx {
    connection_id: ConnectionId,
    router_tx: Sender<(ConnectionId, Event)>,
    router_rx: Receiver<()>,
    metrics_rx: Receiver<MetricsReply>,
    send_buffer: Arc<Mutex<VecDeque<Notification>>>,
    cache: VecDeque<Notification>,
    write: BytesMut,
}

impl LinkRx {
    pub fn new(
        connection_id: ConnectionId,
        router_tx: Sender<(ConnectionId, Event)>,
        router_rx: Receiver<()>,
        metrics_rx: Receiver<MetricsReply>,
        outgoing_data_buffer: Arc<Mutex<VecDeque<Notification>>>,
    ) -> LinkRx {
        LinkRx {
            connection_id,
            router_tx,
            router_rx,
            metrics_rx,
            send_buffer: outgoing_data_buffer,
            cache: VecDeque::with_capacity(100),
            write: BytesMut::with_capacity(10 * 1024),
        }
    }

    /// Returns router notification batch in a serialized format to write to network.
    /// This method can also return empty bytes indicatin
    pub fn iter(&mut self) -> Result<Bytes, LinkError> {
        // Each receive implies 'n' pending notifications.
        self.router_rx.recv()?;

        // Swap locked buffer and serialize all the pending notifications
        mem::swap(&mut *self.send_buffer.lock(), &mut self.cache);
        let notifications = self.cache.drain(..);
        let out = &mut self.write;
        for notification in notifications {
            let unscheduled = write(notification, out)?;
            if unscheduled {
                // Send reschedule event if this connection is unscheduled by router
                let event = (self.connection_id, Event::Ready);
                self.router_tx.send(event)?;
            }
        }

        let out = self.write.split().freeze();
        Ok(out)
    }

    pub async fn stream(&mut self) -> Result<Bytes, LinkError> {
        self.router_rx.recv_async().await?;
        mem::swap(&mut *self.send_buffer.lock(), &mut self.cache);
        let notifications = self.cache.drain(..);
        let out = &mut self.write;
        for notification in notifications {
            let unscheduled = write(notification, out)?;
            if unscheduled {
                let event = (self.connection_id, Event::Ready);
                self.router_tx.send_async(event).await?;
            }
        }

        let out = self.write.split().freeze();
        Ok(out)
    }

    pub fn recv(&mut self) -> Result<Option<Notification>, LinkError> {
        // Read from cache first
        // One router_rx trigger signifies a bunch of notifications. So we
        // should always check cache first
        if let Some(notification) = self.cache.pop_front() {
            match notification {
                Notification::Unschedule => {
                    // Send ready event to router to tell that this connection
                    // is not busy any more
                    self.router_tx.send((self.connection_id, Event::Ready))?;
                    return Ok(None);
                }
                v => return Ok(Some(v)),
            }
        }

        // If cache is empty, check for router trigger and get fresh notifications
        self.router_rx.recv()?;
        mem::swap(&mut *self.send_buffer.lock(), &mut self.cache);
        Ok(self.cache.pop_front())
    }

    pub fn recv_deadline(&mut self, deadline: Instant) -> Result<Option<Notification>, LinkError> {
        // Read from cache first
        // One router_rx trigger signifies a bunch of notifications. So we
        // should always check cache first
        if let Some(notification) = self.cache.pop_front() {
            match notification {
                Notification::Unschedule => {
                    // Send ready event to router to tell that this connection
                    // is not busy any more
                    self.router_tx.send((self.connection_id, Event::Ready))?;
                    return Ok(None);
                }
                v => return Ok(Some(v)),
            }
        }

        // If cache is empty, check for router trigger and get fresh notifications
        self.router_rx.recv_deadline(deadline)?;
        mem::swap(&mut *self.send_buffer.lock(), &mut self.cache);
        Ok(self.cache.pop_front())
    }

    pub async fn next(&mut self) -> Result<Option<Notification>, LinkError> {
        // Read from cache first
        // One router_rx trigger signifies a bunch of notifications. So we
        // should always check cache first
        if let Some(notification) = self.cache.pop_front() {
            match notification {
                Notification::Unschedule => {
                    // Send ready event to router to tell that this connection
                    // is not busy any more
                    self.router_tx
                        .send_async((self.connection_id, Event::Ready))
                        .await?;
                    return Ok(None);
                }
                v => return Ok(Some(v)),
            }
        }

        // If cache is empty, check for router trigger and get fresh notifications
        self.router_rx.recv_async().await?;
        mem::swap(&mut *self.send_buffer.lock(), &mut self.cache);
        Ok(self.cache.pop_front())
    }

    pub fn metrics(&self) -> Option<MetricsReply> {
        self.metrics_rx
            .recv_deadline(Instant::now() + Duration::from_secs(1))
            .ok()
    }
}

pub fn write(notification: Notification, write: &mut BytesMut) -> Result<bool, LinkError> {
    match notification {
        Notification::Forward {
            topic,
            qos,
            pkid,
            payload,
            ..
        } => {
            let qos = v4::qos(qos).unwrap();
            let topic = std::str::from_utf8(&topic).unwrap();
            publish::write(topic, qos, pkid, false, false, &payload, write).unwrap();
        }
        Notification::DeviceAck(ack) => match ack {
            Ack::PubAck(pkid) => {
                puback::write(pkid, write).unwrap();
            }
            Ack::SubAck(pkid, codes) => {
                let codes = suback::codes(codes);
                suback::write(codes, pkid, write).unwrap();
            }
            Ack::PingResp => {
                pingresp::write(write).unwrap();
            }
        },
        Notification::Unschedule => return Ok(true),
        v => unreachable!("{:?}", v),
    }

    Ok(false)
}

#[cfg(test)]
mod test {
    use super::LinkTx;
    use flume::bounded;
    use parking_lot::Mutex;
    use std::{collections::VecDeque, sync::Arc, thread};

    #[test]
    fn push_sends_all_data_and_notifications_to_router() {
        let (router_tx, router_rx) = bounded(10);
        let mut buffers = Vec::new();
        const CONNECTIONS: usize = 1000;
        const MESSAGES_PER_CONNECTION: usize = 100;

        for i in 0..CONNECTIONS {
            let buffer = Arc::new(Mutex::new(VecDeque::new()));
            let mut link_tx = LinkTx::new(i, router_tx.clone(), buffer.clone());
            buffers.push(buffer);
            thread::spawn(move || {
                for _ in 0..MESSAGES_PER_CONNECTION {
                    link_tx.publish("hello/world", vec![1, 2, 3]).unwrap();
                }
            });
        }

        // Router should receive notifications from all the connections
        for _ in 0..CONNECTIONS * MESSAGES_PER_CONNECTION {
            let _v = router_rx.recv().unwrap();
        }

        // Every connection has expected number of messages
        for i in 0..CONNECTIONS {
            assert_eq!(buffers[i].lock().len(), MESSAGES_PER_CONNECTION);
        }

        // TODO: Write a similar test to benchmark buffer vs channels
    }
}
