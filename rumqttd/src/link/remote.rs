use crate::link::local::{LinkError, LinkRx, LinkTx};
use crate::link::network;
use crate::link::network::Network;
use crate::protocol::v4::connack::ConnectReturnCode;
use crate::protocol::v4::connect::Connect;
use crate::router::{
    iobufs::{Incoming, Outgoing},
    Connection, ConnectionAck, Event, Notification,
};
use crate::{ConnectionId, ConnectionSettings};

use flume::{RecvError, SendError, Sender, TrySendError};
use std::io;
use std::sync::Arc;
use tokio::select;
use tokio::time::error::Elapsed;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O")]
    Io(#[from] io::Error),
    #[error("Network {0}")]
    Network(#[from] network::Error),
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
}

/// Orchestrates between Router and Network.
pub struct RemoteLink {
    connect: Connect,
    pub(crate) client_id: String,
    pub(crate) connection_id: ConnectionId,
    network: Network,
    link_tx: LinkTx,
    link_rx: LinkRx,
}

impl RemoteLink {
    pub async fn new(
        config: Arc<ConnectionSettings>,
        router_tx: Sender<(ConnectionId, Event)>,
        mut network: Network,
    ) -> Result<RemoteLink, Error> {
        // Wait for MQTT connect packet and error out if it's not received in time to prevent
        // DOS attacks by filling total connections that the server can handle with idle open
        // connections which results in server rejecting new connections
        let connect = network.read_connect(config.connection_timeout_ms).await?;

        // Register this connection with the router. Router replys with ack which if ok will
        // start the link. Router can sometimes reject the connection (ex max connection limit)
        let client_id = connect.client_id.clone();
        let clean_session = connect.clean_session;
        if !clean_session && client_id.is_empty() {
            return Err(Error::InvalidClientId);
        }

        let (connection, metrics_rx) = Connection::new(clean_session);
        let incoming = Incoming::new(client_id.clone());
        let (outgoing, link_rx) = Outgoing::new(client_id.clone());
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

        // TODO When a new connection request is sent to the router, router should ack with error
        // TODO if it exceeds maximum allowed active connections
        // Right now link identifies failure with dropped rx in router, which
        // is probably ok. We need this here to get id assigned by router
        link_rx.recv_async().await?;
        let notification = outgoing_data_buffer.lock().pop_front().unwrap();

        // Right now link identifies failure with dropped rx in router, which is probably ok
        // We need this here to get id assigned by router
        let (id, session) = match notification {
            Notification::ConnectionAck(ack) => match ack {
                ConnectionAck::Success(id, session) => (id, session),
                ConnectionAck::Failure(reason) => return Err(Error::ConnectionAck(reason)),
            },
            message => return Err(Error::NotConnectionAck(message)),
        };

        // Send connection acknowledgement back to the client
        network.connack(ConnectReturnCode::Success, session).await?;

        let link_tx = LinkTx::new(id, router_tx.clone(), incoming_data_buffer);
        let link_rx = LinkRx::new(id, router_tx, link_rx, metrics_rx, outgoing_data_buffer);
        Ok(RemoteLink {
            connect,
            client_id,
            connection_id: id,
            network,
            link_tx,
            link_rx,
        })
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        self.network.set_keepalive(self.connect.keep_alive);

        // Note:
        // Shouldn't result in bounded queue deadlocks because of blocking n/w send
        loop {
            select! {
                o = self.network.readb() => {
                    let packets = o?;
                    debug!("{:15.15}[I] {:20} size = {} bytes", self.client_id, "packets", packets.len());
                    self.link_tx.send(packets.freeze()).await?;
                }
                // Receive from router when previous when state isn't in collision
                // due to previously received data request
                message = self.link_rx.stream() => {
                    let message = message?;
                    self.network.write_all(&message[..]).await?;
                }
            }
        }
    }
}
