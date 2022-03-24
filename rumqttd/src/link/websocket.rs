use crate::link::local::{LinkError, LinkRx, LinkTx};
use crate::router::{iobufs, Connection, ConnectionAck, Event, Notification};
use crate::{ConnectionId, ConnectionSettings, Filter};
use flume::{RecvError, SendError, Sender, TrySendError};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashSet;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::error::Elapsed;
use tokio::{select, time};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{accept_async, tungstenite, WebSocketStream};
use tungstenite::protocol::frame::coding::CloseCode;

use super::network::N;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O")]
    Io(#[from] io::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("Channel send error")]
    Send(#[from] SendError<(ConnectionId, Event)>),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
    #[error("Persistent session requires valid client id")]
    InvalidClientId,
    #[error("Unexpected router message")]
    NotConnectionAck(Notification),
    #[error("ConnAck error {0}")]
    ConnectionAck(String),
    #[error("Channel try send error")]
    TrySend(#[from] TrySendError<(ConnectionId, Event)>),
    #[error("Link error = {0}")]
    Link(#[from] LinkError),
    #[error("Websocket error = {0}")]
    Ws(#[from] tungstenite::Error),
    #[error("Json error = {0}")]
    Json(#[from] serde_json::Error),
    #[error("Reqwest Error = {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("Websocket filter not set properly")]
    InvalidFilter,
}

pub struct WebsocketLink {
    pub(crate) client_id: String,
    pub(crate) connection_id: ConnectionId,
    network: Network,
    link_tx: LinkTx,
    link_rx: LinkRx,
    subscriptions: HashSet<Filter>,
}

impl WebsocketLink {
    pub async fn new(
        config: Arc<ConnectionSettings>,
        router_tx: Sender<(ConnectionId, Event)>,
        stream: Box<dyn N>
    ) -> Result<WebsocketLink, Error> {
        // Connect to router
        let mut network = Network::new(stream).await?;

        // Wait for MQTT connect packet and error out if it's not received in time to prevent
        // DOS attacks by filling total connections that the server can handle with idle open
        // connections which results in server rejecting new connections
        let connect = network.read_connect(config.connection_timeout_ms).await?;
        let subscriptions = HashSet::new();

        // Register this connection with the router. Router replys with ack which if ok will
        // start the link. Router can sometimes reject the connection (ex max connection limit)
        let client_id = connect.client_id.clone();
        if client_id.is_empty() {
            return Err(Error::InvalidClientId);
        }

        let (connection, metrics_rx) = Connection::new(true);
        let incoming = iobufs::Incoming::new(client_id.clone());
        let (outgoing, link_rx) = iobufs::Outgoing::new(client_id.clone());
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
                ConnectionAck::Failure(reason) => return Err(Error::ConnectionAck(reason)),
            },
            message => return Err(Error::NotConnectionAck(message)),
        };

        let link_tx = LinkTx::new(id, router_tx.clone(), incoming_data_buffer);
        let link_rx = LinkRx::new(id, router_tx, link_rx, metrics_rx, outgoing_data_buffer);

        // Send connection acknowledgement back to the client
        network.connack().await?;
        Ok(WebsocketLink {
            client_id,
            connection_id: id,
            network,
            link_tx,
            link_rx,
            subscriptions,
        })
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        let mut interval = time::interval(Duration::from_secs(5));
        let mut ping = time::interval(Duration::from_secs(10));
        let mut pong = true;

        // Note:
        // Shouldn't result in bounded queue deadlocks because of blocking n/w send
        loop {
            select! {
                o = self.network.read() => {
                    let message = o?;
                    debug!("{:15.15} {:20} size = {} bytes", self.connection_id, "read",  message.len());
                    match message {
                        Message::Text(m) => {
                            match serde_json::from_str(&m)? {
                                Incoming::Websocket { filter } => {
                                    match validate_websocket(&self.client_id, &filter) {
                                        Err(e) => {
                                            error!("{:15.15} {:20}: {}", self.connection_id, "validation error", e);
                                            self.network.write(close(&e)).await?;
                                            return Err(e)
                                        },
                                        _ => self.link_tx.websocket(filter)?,
                                    }
                                }
                                Incoming::Ping { .. } => {
                                    let message = Message::Text(serde_json::to_string(&Outgoing::Pong{ pong: true })?);
                                    self.network.write(message).await?;
                                    continue
                                }
                                Incoming::Publish { topic, data } => {
                                    let payload = serde_json::to_vec(&data)?;
                                    self.link_tx.try_publish(topic, payload)?;
                                }
                            }
                        }
                        Message::Close(..) => {
                            let error = "Connection closed".to_string();
                            let error = io::Error::new(io::ErrorKind::ConnectionAborted, error);
                            return Err(Error::Io(error));
                        }
                        Message::Pong(_) => {
                            pong = true;
                        }
                        packet => {
                            let error = format!("Expecting connect text. Received = {:?}", packet);
                            let error = io::Error::new(io::ErrorKind::InvalidData, error);
                            return Err(Error::Io(error));
                        }
                    };
                }
                // Receive from router when previous when state isn't in collision
                // due to previously received data request
                notification = self.link_rx.next() => {
                    let message = match notification? {
                        Some(v) => v,
                        None => continue,
                    };

                    let message = match message {
                        Notification::Websocket(websocket) => {
                            let publish = Outgoing::Publish { topic: websocket.topic, data: serde_json::from_slice(&websocket.payload)? };
                            Message::Text(serde_json::to_string(&publish)?)
                        },
                        v => unreachable!("Expecting only data or device acks. Received = {:?}", v)
                    };

                    let write_len = message.len();
                    self.network.write(message).await?;
                    debug!("{:15.15} {:20} size = {} bytes", self.connection_id, "write", write_len);
                }
                _ = interval.tick() => {
                    for filter in self.subscriptions.iter() {
                        self.link_tx.websocket(filter)?;
                    }
                }
                _ = ping.tick() => {
                    if !pong {
                        error!("{:15.15} {:20}", self.connection_id, "no-pong");
                        break
                    }

                    let message = Message::Ping(vec![1, 2, 3]);
                    self.network.write(message).await?;
                    pong = false;
                }
            }
        }

        Ok(())
    }
}

/// Validates that the fields `device_id` are the same as that of device connected
/// for a topic filter of format "/device/device_id/..."
fn validate_websocket(client_id: &str, filter: &str) -> Result<(), Error> {
    let tokens: Vec<&str> = filter.split('/').collect();
    let id = tokens.get(2).ok_or(Error::InvalidFilter)?.to_owned();

    match id == client_id {
        true => Ok(()),
        false => Err(Error::InvalidClientId),
    }
}

fn close(e: &Error) -> Message {
    let msg = match e {
        Error::InvalidClientId => "Provided client id is not valid",
        Error::InvalidFilter => "Websocket filter not set properly",
        _ => "ERROR",
    };
    Message::Close(Some(CloseFrame {
        code: CloseCode::Unsupported,
        reason: Cow::Borrowed(msg),
    }))
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Connect {
    client_id: String,
    body: Jwt,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Headers {
    authorization: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Jwt {
    headers: Headers,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type")]
pub enum Outgoing {
    #[serde(alias = "connack")]
    ConnAck { status: bool },
    #[serde(alias = "publish")]
    Publish { topic: String, data: Vec<Value> },
    #[serde(alias = "pong")]
    Pong { pong: bool },
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type")]
pub enum Incoming {
    #[serde(alias = "websocket")]
    Websocket { filter: String },
    #[serde(alias = "ping")]
    Ping { ping: bool },
    #[serde(alias = "publish")]
    Publish { topic: String, data: Vec<Value> },
}

/// Network transforms packets <-> frames efficiently. It takes
/// advantage of pre-allocation, buffering and vectorization when
/// appropriate to achieve performance
pub struct Network {
    /// Socket for IO
    socket: WebSocketStream<Box<dyn N>>,
}

impl Network {
    pub async fn new(socket: Box<dyn N>) -> Result<Network, Error> {
        let ws_stream = accept_async(socket).await?;
        Ok(Network { socket: ws_stream })
    }

    pub async fn read_connect(&mut self, t: u16) -> Result<Connect, Error> {
        let message = time::timeout(Duration::from_millis(t.into()), async {
            let message = match self.socket.next().await {
                Some(v) => v?,
                None => {
                    let error = "Connection closed".to_string();
                    let error = io::Error::new(io::ErrorKind::ConnectionAborted, error);
                    return Err(Error::Io(error));
                }
            };

            Ok::<_, Error>(message)
        })
        .await??;

        let connect: Connect = match message {
            Message::Text(m) => serde_json::from_str(&m)?,
            Message::Close(..) => {
                let error = "Connection closed".to_string();
                let error = io::Error::new(io::ErrorKind::ConnectionAborted, error);
                return Err(Error::Io(error));
            }
            packet => {
                let error = format!("Expecting connect text. Received = {:?}", packet);
                let error = io::Error::new(io::ErrorKind::InvalidData, error);
                return Err(Error::Io(error));
            }
        };

        Ok(connect)
    }

    pub async fn read(&mut self) -> Result<Message, Error> {
        let message = match self.socket.next().await {
            Some(v) => v?,
            None => {
                let error = "Connection closed".to_string();
                let error = io::Error::new(io::ErrorKind::ConnectionAborted, error);
                return Err(Error::Io(error));
            }
        };

        Ok(message)
    }

    pub async fn connack(&mut self) -> Result<(), Error> {
        let ack = Outgoing::ConnAck { status: true };
        let message = Message::Text(serde_json::to_string(&ack)?);
        self.socket.send(message).await?;
        Ok(())
    }

    pub async fn write(&mut self, message: Message) -> Result<(), Error> {
        self.socket.send(message).await?;
        Ok(())
    }
}
