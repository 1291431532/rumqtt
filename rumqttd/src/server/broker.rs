use crate::link::bridge;
use crate::link::console::ConsoleLink;
use crate::link::network::{Network, N};
use crate::link::remote::{self, RemoteLink};
use crate::link::websocket::{self, ShadowLink};
use crate::server::{create_nativetls_acceptor, create_rustls_acceptor, TLSAcceptor};
use crate::ConnectionSettings;
use flume::{RecvError, SendError, Sender};
use log::*;
use std::sync::Arc;

use std::time::Duration;
use std::{io, thread};

use crate::link::console;
use crate::link::local::{self, Link};
use crate::replicator::local::Cluster as LocalCluster;
use crate::replicator::local::ReplicationData;
use crate::replicator::remote::Cluster as RemoteCluster;
use crate::router::{Disconnection, Event, Router};
use crate::{server, Config, ConnectionId, NodeId, ServerSettings, TlsConfig, MAX_NODES};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::error::Elapsed;
use tokio::{task, time};

// All requirements for `native-tls`
#[derive(Debug, thiserror::Error)]
#[error("Acceptor error")]
pub enum Error {
    #[error("I/O {0}")]
    Io(#[from] io::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
    #[error("Channel send error")]
    Send(#[from] SendError<(ConnectionId, Event)>),
    #[error("Certs error = {0}")]
    Certs(#[from] server::Error),
    #[error("Accept error = {0}")]
    Accept(String),
    #[error("No peer certificate")]
    NoPeerCertificate,
    #[error("Remote error = {0}")]
    Remote(#[from] remote::Error),
}

pub struct Broker {
    config: Arc<Config>,
    router_tx: Sender<(ConnectionId, Event)>,
}

impl Broker {
    pub fn new(config: Config) -> Broker {
        let config = Arc::new(config);
        let router_config = config.router.clone();
        let mut router = Router::new(config.id, router_config);

        // Setup cluster if cluster settings are configured
        match config.cluster.clone() {
            Some(cluster_config) => {
                let node_id = cluster_config.node_id;
                let listen = cluster_config.listen;
                let seniors = cluster_config.seniors;
                let mut cluster = RemoteCluster::new(node_id, &listen, seniors);
                Broker::setup_remote_cluster(&mut router, node_id, &mut cluster);

                // Start router first and then cluster in the background
                let router_tx = router.spawn();
                cluster.spawn();
                Broker { config, router_tx }
            }
            None => {
                let router_tx = router.spawn();

                Broker { config, router_tx }
            }
        }
    }

    pub fn new_local_cluster(
        config: Config,
        node_id: NodeId,
        seniors: Vec<(NodeId, Sender<ReplicationData>)>,
    ) -> (Broker, Sender<ReplicationData>) {
        let config = Arc::new(config);
        let router_config = config.router.clone();

        let mut router = Router::new(config.id, router_config);
        let (mut cluster, tx) = LocalCluster::new(node_id, seniors);
        Broker::setup_local_cluster(&mut router, node_id, &mut cluster);

        // Start router first and then cluster in the background
        let router_tx = router.spawn();
        cluster.spawn();

        (Broker { config, router_tx }, tx)
    }

    fn setup_remote_cluster(router: &mut Router, node_id: NodeId, cluster: &mut RemoteCluster) {
        // Retrieve pre-configured list of router <-> replica link from router
        // and add the link to cluster
        let nodes: Vec<NodeId> = (0..MAX_NODES).filter(|v| *v != node_id).collect();
        for node_id in nodes {
            let link = router.get_replica_handle(node_id);
            cluster.add_replica_router_handle(node_id, link);
        }
    }

    fn setup_local_cluster(router: &mut Router, node_id: NodeId, cluster: &mut LocalCluster) {
        // Retrieve pre-configured list of router <-> replica link from router
        // and add the link to cluster
        let nodes: Vec<NodeId> = (0..MAX_NODES).filter(|v| *v != node_id).collect();
        for node_id in nodes {
            let link = router.get_replica_handle(node_id);
            cluster.add_replica_router_handle(node_id, link);
        }
    }

    pub fn link(&self, client_id: &str) -> Result<Link, local::LinkError> {
        // Register this connection with the router. Router replies with ack which if ok will
        // start the link. Router can sometimes reject the connection (ex max connection limit)
        Link::new(client_id, self.router_tx.clone(), true)
    }

    pub fn start(&mut self) -> Result<(), Error> {
        // spawn bridge in a separate thread
        if let Some(bridge_config) = self.config.bridge.clone() {
            let router_tx = self.router_tx.clone();
            thread::Builder::new()
                .name("bridge-thread".to_string())
                .spawn(move || {
                    let mut runtime = tokio::runtime::Builder::new_current_thread();
                    let runtime = runtime.enable_all().build().unwrap();
                    let router_tx = router_tx.clone();

                    runtime.block_on(async move {
                        if let Err(e) = bridge::bridge_launch(bridge_config, router_tx).await {
                            error!("bridge: {:?}", e);
                        };
                    });
                })?;
        }

        // spawn servers in a separate thread
        for (id, config) in self.config.servers.clone() {
            let server_name = format!("mqtt-{}", id);
            let server_thread = thread::Builder::new().name(server_name.clone());
            let server = Server::new(server_name, config, self.router_tx.clone());
            server_thread.spawn(move || {
                let mut runtime = tokio::runtime::Builder::new_current_thread();
                let runtime = runtime.enable_all().build().unwrap();

                runtime.block_on(async {
                    if let Err(e) = server.start(false).await {
                        error!("{:15.15}[I] Remote link error = {:?}", "", e);
                    }
                });
            })?;
        }

        for (id, config) in self.config.shadows.clone() {
            let server_name = format!("shadow-{}", id);
            let server_thread = thread::Builder::new().name(server_name.clone());
            let server = Server::new(server_name, config, self.router_tx.clone());
            server_thread.spawn(move || {
                let mut runtime = tokio::runtime::Builder::new_current_thread();
                let runtime = runtime.enable_all().build().unwrap();

                runtime.block_on(async {
                    if let Err(e) = server.start(true).await {
                        error!("Accept loop error: {:?}", e.to_string());
                    }
                });
            })?;
        }

        let console_link = ConsoleLink::new(self.config.console.clone(), self.router_tx.clone());

        let console_link = Arc::new(console_link);
        console::start(console_link);

        Ok(())
    }
}

struct Server {
    id: String,
    config: ServerSettings,
    router_tx: Sender<(ConnectionId, Event)>,
}

impl Server {
    pub fn new(
        id: String,
        config: ServerSettings,
        router_tx: Sender<(ConnectionId, Event)>,
    ) -> Server {
        Server {
            id,
            config,
            router_tx,
        }
    }

    // Depending on TLS or not create a new Network
    async fn tls_accept(&self, stream: TcpStream) -> Result<Box<dyn N>, Error> {
        let acceptor = match &self.config.tls_config {
            Some(c) => match c {
                TlsConfig::Rustls {
                    ca_path,
                    cert_path,
                    key_path,
                } => create_rustls_acceptor(cert_path, key_path, ca_path)?,
                TlsConfig::NativeTls {
                    pkcs12_path,
                    pkcs12_pass,
                } => create_nativetls_acceptor(pkcs12_path, pkcs12_pass)?,
            },
            None => {
                let network = Box::new(stream);
                return Ok(network);
            }
        };

        match acceptor {
            #[cfg(feature = "rustls")]
            TLSAcceptor::Rustls { acceptor } => {
                let stream = acceptor.accept(stream).await?;
                let network = Box::new(stream);
                Ok(network)
            }
            #[cfg(feature = "native-tls")]
            TLSAcceptor::NativeTLS { acceptor } => match acceptor.accept(stream).await {
                Ok(stream) => {
                    let network = Box::new(stream);
                    Ok(network)
                }
                Err(e) => Err(Error::Certs(server::Error::NativeTls(e))),
            },
        }
    }

    async fn start(&self, shadow: bool) -> Result<(), Error> {
        let listener = TcpListener::bind(&self.config.listen).await?;
        let delay = Duration::from_millis(self.config.next_connection_delay_ms);
        let mut count: usize = 0;

        let config = Arc::new(self.config.connections.clone());
        info!("{:15.15}[>] waiting for remote connections", self.id);
        loop {
            // Await new network connection.
            let (stream, addr) = match listener.accept().await {
                Ok((s, r)) => (s, r),
                Err(e) => {
                    error!("Unable to accept socket. Error = {:?}", e);
                    continue;
                }
            };

            let network = match self.tls_accept(stream).await {
                Ok(o) => o,
                Err(e) => {
                    error!("Tls accept error = {:?}", e);
                    continue;
                }
            };

            info!(
                "{:15.15}[I] {:20} addr = {} count {}",
                self.id, "accept", addr, count
            );

            let config = config.clone();
            let router_tx = self.router_tx.clone();
            count += 1;

            match shadow {
                false => task::spawn(device_connection(config, router_tx, network)),
                true => task::spawn(shadow_connection(config, router_tx, network)),
            };

            time::sleep(delay).await;
        }
    }
}

/// A new network connection should wait for mqtt connect packet. This handling should be handled
/// asynchronously to avoid listener from not blocking new connections while this connection is
/// waiting for mqtt connect packet. Also this honours connection wait time as per config to prevent
/// denial of service attacks (rogue clients which only does network connection without sending
/// mqtt connection packet to make make the server reach its concurrent connection limit)
async fn device_connection(
    config: Arc<ConnectionSettings>,
    router_tx: Sender<(ConnectionId, Event)>,
    stream: Box<dyn N>,
) {
    let network = Network::new(stream, config.max_payload_size);
    // Start the link
    let mut link = match RemoteLink::new(config, router_tx.clone(), network).await {
        Ok(l) => l,
        Err(e) => {
            error!("{:15.15}[E] Remote link error = {:?}", "", e);
            return;
        }
    };

    let client_id = link.client_id.clone();
    let connection_id = link.connection_id;

    match link.start().await {
        // Connection get close. This shouldn't usually happen
        Ok(_) => error!("{:15.15}[E] connection-stop", client_id),
        // No need to send a disconnect message when disconnetion
        // originated internally in the router
        Err(remote::Error::Link(e)) => {
            error!("{:15.15}[E] {:20} {:?}", client_id, "router-drop", e);
            return;
        }
        // Any other error
        Err(e) => error!("{:15.15}[E] Disconnected!! {}", client_id, e.to_string()),
    };

    let disconnect = Disconnection {
        id: client_id,
        execute_will: false,
        pending: vec![],
    };

    let disconnect = Event::Disconnect(disconnect);
    let message = (connection_id, disconnect);
    router_tx.send(message).ok();
}

async fn shadow_connection(
    config: Arc<ConnectionSettings>,
    router_tx: Sender<(ConnectionId, Event)>,
    stream: Box<dyn N>,
) {
    // Start the link
    let mut link = match ShadowLink::new(config, router_tx.clone(), stream).await {
        Ok(l) => l,
        Err(e) => {
            error!("{:15.15}[E] Remote link error = {:?}", "", e);
            return;
        }
    };

    let client_id = link.client_id.clone();
    let connection_id = link.connection_id;

    match link.start().await {
        // Connection get close. This shouldn't usually happen
        Ok(_) => error!("{:15.15}[E] connection-stop", client_id),
        // No need to send a disconnect message when disconnetion
        // originated internally in the router
        Err(websocket::Error::Link(e)) => {
            error!("{:15.15}[E] {:20} {:?}", client_id, "router-drop", e);
            return;
        }
        Err(e) => error!("{:15.15}[E] {}", client_id, e.to_string()),
    };

    let disconnect = Disconnection {
        id: client_id,
        execute_will: false,
        pending: vec![],
    };

    let disconnect = Event::Disconnect(disconnect);
    let message = (connection_id, disconnect);
    router_tx.send(message).ok();
}
