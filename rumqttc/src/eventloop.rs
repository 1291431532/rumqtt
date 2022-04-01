use crate::framed::Network;
#[cfg(feature = "use-rustls")]
use crate::tls;
use crate::{Incoming, MqttOptions, MqttState, Outgoing, Packet, Request, StateError, Transport};

use crate::mqttbytes::v4::*;
use async_channel::{bounded, Receiver, Sender};
/*#[cfg(feature = "websocket")]
use async_tungstenite::tokio::connect_async;
#[cfg(all(feature = "use-rustls", feature = "websocket"))]
use async_tungstenite::tokio::connect_async_with_tls_connector;
// use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
// use tokio::select;
// use tokio::time::{self, error::Elapsed, Instant, Sleep};
#[cfg(feature = "websocket")]
use ws_stream_tungstenite::WsStream;

#[cfg(unix)]
use std::path::Path;
*/

use std::{fmt, io};
use std::pin::Pin;
use std::time::{Duration, Instant};
use std::vec::IntoIter;
use smol::future::{FutureExt};
use smol::net::TcpStream;
use smol::Timer;
use crate::rr::{Return5, Select5};

/// Errors returned by `Timeout`.
#[derive(Debug, PartialEq)]
pub struct Elapsed;
impl fmt::Display for Elapsed {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        "deadline has elapsed".fmt(fmt)
    }
}

impl std::error::Error for Elapsed {}

impl From<Elapsed> for std::io::Error {
    fn from(_err: Elapsed) -> std::io::Error {
        std::io::ErrorKind::TimedOut.into()
    }
}

/// Critical errors during eventloop polling
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("Mqtt state: {0}")]
    MqttState(#[from] StateError),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[cfg(feature = "websocket")]
    #[error("Websocket: {0}")]
    Websocket(#[from] async_tungstenite::tungstenite::error::Error),
    #[cfg(feature = "websocket")]
    #[error("Websocket Connect: {0}")]
    WsConnect(#[from] http::Error),
    #[cfg(feature = "use-rustls")]
    #[error("TLS: {0}")]
    Tls(#[from] tls::Error),
    #[error("I/O: {0}")]
    Io(#[from] io::Error),
    #[error("Connection refused, return code: {0:?}")]
    ConnectionRefused(ConnectReturnCode),
    #[error("Expected ConnAck packet, received: {0:?}")]
    NotConnAck(Packet),
    #[error("Requests done")]
    RequestsDone,
    #[error("Cancel request by the user")]
    Cancel,
}

/// Eventloop with all the state of a connection
pub struct EventLoop {
    /// Options of the current mqtt connection
    pub options: MqttOptions,
    /// Current state of the connection
    pub state: MqttState,
    /// Request stream
    pub requests_rx: Receiver<Request>,
    /// Requests handle to send requests
    pub requests_tx: Sender<Request>,
    /// Pending packets from last session
    pub pending: IntoIter<Request>,
    /// Network connection to the broker
    pub(crate) network: Option<Network>,
    /// Keep alive time
    pub(crate) keepalive_timeout: Option<Pin<Box<Timer>>>,
    /// Handle to read cancellation requests
    pub(crate) cancel_rx: Receiver<()>,
    /// Handle to send cancellation requests (and drops)
    pub(crate) cancel_tx: Sender<()>,
}

/// Events which can be yielded by the event loop
#[derive(Debug, PartialEq, Clone)]
pub enum Event {
    Incoming(Incoming),
    Outgoing(Outgoing),
}

impl EventLoop {
    /// New MQTT `EventLoop`
    ///
    /// When connection encounters critical errors (like auth failure), user has a choice to
    /// access and update `options`, `state` and `requests`.
    pub fn new(options: MqttOptions, cap: usize) -> EventLoop {
        let (cancel_tx, cancel_rx) = bounded(5);
        let (requests_tx, requests_rx) = bounded(cap);
        let pending = Vec::new();
        let pending = pending.into_iter();
        let max_inflight = options.inflight;
        let manual_acks = options.manual_acks;

        EventLoop {
            options,
            state: MqttState::new(max_inflight, manual_acks),
            requests_tx,
            requests_rx,
            pending,
            network: None,
            keepalive_timeout: None,
            cancel_rx,
            cancel_tx,
        }
    }

    /// Returns a handle to communicate with this eventloop
    pub fn handle(&self) -> Sender<Request> {
        self.requests_tx.clone()
    }

    /// Handle for cancelling the eventloop.
    ///
    /// Can be useful in cases when connection should be halted immediately
    /// between half-open connection detections or (re)connection timeouts
    pub(crate) fn cancel_handle(&mut self) -> Sender<()> {
        self.cancel_tx.clone()
    }

    fn clean(&mut self) {
        self.network = None;
        self.keepalive_timeout = None;
        let pending = self.state.clean();
        self.pending = pending.into_iter();
    }

    /// Yields Next notification or outgoing request and periodically pings
    /// the broker. Continuing to poll will reconnect to the broker if there is
    /// a disconnection.
    /// **NOTE** Don't block this while iterating
    pub async fn poll(&mut self) -> Result<Event, ConnectionError> {
        if self.network.is_none() {
            let (network, connack) = connect_or_cancel(&self.options, &self.cancel_rx).await?;
            self.network = Some(network);

            if self.keepalive_timeout.is_none() {
                self.keepalive_timeout = Some(Box::pin(smol::Timer::after(self.options.keep_alive)));
            }

            return Ok(Event::Incoming(connack));
        }

        match self.select().await {
            Ok(v) => Ok(v),
            Err(e) => {
                self.clean();
                Err(e)
            }
        }
    }

    /// Select on network and requests and generate keepalive pings when necessary
    async fn select(&mut self) -> Result<Event, ConnectionError> {
        let network = self.network.as_mut().unwrap();
        // let await_acks = self.state.await_acks;
        let inflight_full = self.state.inflight >= self.options.inflight;
        let throttle = self.options.pending_throttle;
        let pending = self.pending.len() > 0;
        let collision = self.state.collision.is_some();


        // Read buffered events from previous polls before calling a new poll
        if let Some(event) = self.state.events.pop_front() {
            return Ok(event);
        }
        // this loop is necessary since self.incoming.pop_front() might return None. In that case,
        // instead of returning a None event, we try again.
        let select5 = Select5{
            future1: Some(network.readb(&mut self.state)),
            future2: Some(self.requests_rx.recv()),
            future3: Some(next_pending(throttle, &mut self.pending)),
            future4: Some(self.keepalive_timeout.as_mut().unwrap()),
            future5: Some(self.cancel_rx.recv()),
            fr1: |_:&Result<(), StateError>| {
                true
            },
            fr2: |_:&Result<Request, async_channel::RecvError>| {
                !inflight_full && !pending && !collision
            },
            fr3: |r3:&Option<_>| {
                pending && r3.is_some()
            },
            fr4: |_:&Instant| {
                true
            },
            fr5: |_:&Result<(), async_channel::RecvError>| {
                true
            },
            start: fastrand::u8(0..5)
        }.await;
        let result= match select5 {
            Return5::R1(o) => {
                println!("r1 {:?}",&o);
                o?;
                // flush all the acks and return first incoming packet
                network.flush(&mut self.state.write).await?;
                Ok(self.state.events.pop_front().unwrap())
            }
            Return5::R2(o) => {
                println!("r2 {:?}",&o);
                match o {
                    Ok(request) => {
                        self.state.handle_outgoing_packet(request)?;
                        network.flush(&mut self.state.write).await?;
                        Ok(self.state.events.pop_front().unwrap())
                    }
                    Err(_) => Err(ConnectionError::RequestsDone)
                }
            }
            Return5::R3(request) => {
                println!("r3 {:?}",&request);
                self.state.handle_outgoing_packet(request.unwrap())?;
                network.flush(&mut self.state.write).await?;
                Ok(self.state.events.pop_front().unwrap())
            }
            Return5::R4(_) => {
                println!("r4 ");
                let timeout = self.keepalive_timeout.as_mut().unwrap();
                timeout.as_mut().set(Timer::after(self.options.keep_alive));
                // timeout.as_mut().set_after(self.options.keep_alive);

                self.state.handle_outgoing_packet(Request::PingReq)?;
                network.flush(&mut self.state.write).await?;
                Ok(self.state.events.pop_front().unwrap())
            }
            Return5::R5(_) => {
                println!("r5 ");
                Err(ConnectionError::Cancel)
            }
        };
        match &result {
            Ok(rr) => {
                println!("select! 结束 OK: {:?}",&rr);
            }
            Err(re) => {
                println!("select! 结束 Err: {:?}",&re);
            }
        }
        return result;
/*        select! {
            // Pull a bunch of packets from network, reply in bunch and yield the first item
            o = network.readb(&mut self.state) => {
                o?;
                // flush all the acks and return first incoming packet
                network.flush(&mut self.state.write).await?;
                Ok(self.state.events.pop_front().unwrap())
            },
            // Pull next request from user requests channel.
            // If conditions in the below branch are for flow control. We read next user
            // user request only when inflight messages are < configured inflight and there
            // are no collisions while handling previous outgoing requests.
            //
            // Flow control is based on ack count. If inflight packet count in the buffer is
            // less than max_inflight setting, next outgoing request will progress. For this
            // to work correctly, broker should ack in sequence (a lot of brokers won't)
            //
            // E.g If max inflight = 5, user requests will be blocked when inflight queue
            // looks like this                 -> [1, 2, 3, 4, 5].
            // If broker acking 2 instead of 1 -> [1, x, 3, 4, 5].
            // This pulls next user request. But because max packet id = max_inflight, next
            // user request's packet id will roll to 1. This replaces existing packet id 1.
            // Resulting in a collision
            //
            // Eventloop can stop receiving outgoing user requests when previous outgoing
            // request collided. I.e collision state. Collision state will be cleared only
            // when correct ack is received
            // Full inflight queue will look like -> [1a, 2, 3, 4, 5].
            // If 3 is acked instead of 1 first   -> [1a, 2, x, 4, 5].
            // After collision with pkid 1        -> [1b ,2, x, 4, 5].
            // 1a is saved to state and event loop is set to collision mode stopping new
            // outgoing requests (along with 1b).
            o = self.requests_rx.recv(), if !inflight_full && !pending && !collision => match o {
                Ok(request) => {
                    self.state.handle_outgoing_packet(request)?;
                    network.flush(&mut self.state.write).await?;
                    Ok(self.state.events.pop_front().unwrap())
                }
                Err(_) => Err(ConnectionError::RequestsDone),
            },
            // Handle the next pending packet from previous session. Disable
            // this branch when done with all the pending packets
            Some(request) = next_pending(throttle, &mut self.pending), if pending => {
                self.state.handle_outgoing_packet(request)?;
                network.flush(&mut self.state.write).await?;
                Ok(self.state.events.pop_front().unwrap())
            },
            // We generate pings irrespective of network activity. This keeps the ping logic
            // simple. We can change this behavior in future if necessary (to prevent extra pings)
            _ = self.keepalive_timeout.as_mut().unwrap() => {
                let timeout = self.keepalive_timeout.as_mut().unwrap();
                timeout.as_mut().reset(Instant::now() + self.options.keep_alive);

                self.state.handle_outgoing_packet(Request::PingReq)?;
                network.flush(&mut self.state.write).await?;
                Ok(self.state.events.pop_front().unwrap())
            }
            // cancellation requests to stop the polling
            _ = self.cancel_rx.recv() => {
                Err(ConnectionError::Cancel)
            }
        }
*/
    }
}
/*
enum Return5<T1,T2,T3,T4,T5>{
    R1(T1),
    R2(T2),
    R3(T3),
    R4(T4),
    R5(T5)
}
pin_project_lite::pin_project!{
    struct Select5<F1,F2,F3,F4,F5> {
        #[pin]
        pub future1: Option<F1>,
        #[pin]
        pub future2: Option<F2>,
        #[pin]
        pub future3: Option<F3>,
        #[pin]
        pub future4: Option<F4>,
        #[pin]
        pub future5: Option<F5>,
        pub fr1: bool,
        pub fr2: bool,
        pub fr3: bool,
        pub fr4: bool,
        pub fr5: bool,
        pub start: u8
    }
}

impl<O3,F1: Future,F2: Future,F3: Future<Output = Option<O3>>,F4: Future,F5: Future> Future for Select5<F1,F2,F3,F4,F5> {
    type Output = Return5<F1::Output,F2::Output,O3,F4::Output,F5::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let mut current;
        let mut is_pending = false;
        for i in 0..5 {
            current = (self.start + i) % 5;
            match current {
                0 => {
                    if let Some(p1) = this.future1.as_pin_mut() {
                        match p1.poll(cx) {
                            Poll::Ready(r1) => {
                                if self.fr1 {
                                    return Poll::Ready(Return5::R1(r1));
                                }
                                this.future1.take();
                            }
                            Poll::Pending => {
                            }
                        }
                        is_pending = true;
                    }
                }
                1 => {
                    if let Some(p2) = this.future2.as_pin_mut() {
                        match p2.poll(cx) {
                            Poll::Ready(r2) => {
                                if self.fr2 {
                                    return Poll::Ready(Return5::R2(r2));
                                }
                                this.future2.take();
                            }
                            Poll::Pending => {
                            }
                        }
                        is_pending = true;
                    }

                }
                2 => {
                    if let Some(p3) = this.future3.as_pin_mut() {
                        match p3.poll(cx) {
                            Poll::Ready(r3) => {
                                if self.fr3 {
                                    if let Some(r3) = r3 {
                                        return Poll::Ready(Return5::R3(r3));
                                    }
                                }
                                this.future3.take();
                            }
                            Poll::Pending => {
                            }
                        }
                        is_pending = true;
                    }
                }
                3 => {
                    if let Some(p4) = this.future4.as_pin_mut() {
                        match p4.poll(cx) {
                            Poll::Ready(r4) => {
                                if self.fr4 {
                                    return Poll::Ready(Return5::R4(r4));
                                }
                                this.future4.take();
                            }
                            Poll::Pending => {
                            }
                        }
                        is_pending = true;
                    }
                }
                4 => {
                    if let Some(p5) = this.future5.as_pin_mut() {
                        match p5.poll(cx) {
                            Poll::Ready(r5) => {
                                if self.fr5 {
                                    return Poll::Ready(Return5::R5(r5));
                                }
                                this.future5.take();
                            }
                            Poll::Pending => {
                            }
                        }
                        is_pending = true;
                    }
                }
                _ => {}
            }
        }
        if is_pending {
            return Poll::Pending;
        }
        panic!("Select5 NO FOUND DEFAULT!")
    }
}
*/
async fn connect_or_cancel(
    options: &MqttOptions,
    cancel_rx: &Receiver<()>,
) -> Result<(Network, Incoming), ConnectionError> {
    // select here prevents cancel request from being blocked until connection request is
    // resolved. Returns with an error if connections fail continuously
    smol::future::race( connect(options),async {
        let _ = cancel_rx.recv().await;
        Err(ConnectionError::Cancel)
    }).await
    /*select! {
        o = connect(options) => o,
        _ = cancel_rx.recv() => {
            Err(ConnectionError::Cancel)
        }
    }*/
}

/// This stream internally processes requests from the request stream provided to the eventloop
/// while also consuming byte stream from the network and yielding mqtt packets as the output of
/// the stream.
/// This function (for convenience) includes internal delays for users to perform internal sleeps
/// between re-connections so that cancel semantics can be used during this sleep
async fn connect(options: &MqttOptions) -> Result<(Network, Incoming), ConnectionError> {
    // connect to the broker
    let mut network = match network_connect(options).await {
        Ok(network) => network,
        Err(e) => {
            return Err(e);
        }
    };

    // make MQTT connection request (which internally awaits for ack)
    let packet = match mqtt_connect(options, &mut network).await {
        Ok(p) => p,
        Err(e) => return Err(e),
    };

    // Last session might contain packets which aren't acked. MQTT says these packets should be
    // republished in the next session
    // move pending messages from state to eventloop
    // let pending = self.state.clean();
    // self.pending = pending.into_iter();
    Ok((network, packet))
}

async fn network_connect(options: &MqttOptions) -> Result<Network, ConnectionError> {
    let network = match options.transport() {
        Transport::Tcp => {
            let addr = options.broker_addr.as_str();
            let port = options.port;
            let socket = TcpStream::connect((addr, port)).await?;
            Network::new(socket, options.max_incoming_packet_size)
        }
        #[cfg(feature = "use-rustls")]
        Transport::Tls(tls_config) => {
            let socket = tls::tls_connect(options, &tls_config).await?;
            Network::new(socket, options.max_incoming_packet_size)
        }
       /* #[cfg(unix)]
        Transport::Unix => {
            let file = options.broker_addr.as_str();
            let socket = UnixStream::connect(Path::new(file)).await?;
            Network::new(socket, options.max_incoming_packet_size)
        }*/
        #[cfg(feature = "websocket")]
        Transport::Ws => {
            let request = http::Request::builder()
                .method(http::Method::GET)
                .uri(options.broker_addr.as_str())
                .header("Sec-WebSocket-Protocol", "mqttv3.1")
                .body(())?;

            let (socket, _) = connect_async(request).await?;

            Network::new(WsStream::new(socket), options.max_incoming_packet_size)
        }
        #[cfg(all(feature = "use-rustls", feature = "websocket"))]
        Transport::Wss(tls_config) => {
            let request = http::Request::builder()
                .method(http::Method::GET)
                .uri(options.broker_addr.as_str())
                .header("Sec-WebSocket-Protocol", "mqttv3.1")
                .body(())?;

            let connector = tls::tls_connector(&tls_config).await?;

            let (socket, _) = connect_async_with_tls_connector(request, Some(connector)).await?;

            Network::new(WsStream::new(socket), options.max_incoming_packet_size)
        }
    };

    Ok(network)
}

async fn mqtt_connect(
    options: &MqttOptions,
    network: &mut Network,
) -> Result<Incoming, ConnectionError> {
    let keep_alive = options.keep_alive().as_secs() as u16;
    let clean_session = options.clean_session();
    let last_will = options.last_will();

    let mut connect = Connect::new(options.client_id());
    connect.keep_alive = keep_alive;
    connect.clean_session = clean_session;
    connect.last_will = last_will;

    if let Some((username, password)) = options.credentials() {
        let login = Login::new(username, password);
        connect.login = Some(login);
    }
    // mqtt connection with timeout
    network.connect(connect).or(async {
        smol::Timer::after(Duration::from_secs(options.connection_timeout())).await;
        Err(io::ErrorKind::TimedOut.into())
    }).await?;

/*    time::timeout(Duration::from_secs(options.connection_timeout()), async {
        network.connect(connect).await?;
        Ok::<_, ConnectionError>(())
    })
    .await??;
*/
    // wait for 'timeout' time to validate connack
    let packet = async {
        match network.read().await? {
            Incoming::ConnAck(connack) if connack.code == ConnectReturnCode::Success => {
                Ok(Packet::ConnAck(connack))
            }
            Incoming::ConnAck(connack) => Err(ConnectionError::ConnectionRefused(connack.code)),
            packet => Err(ConnectionError::NotConnAck(packet)),
        }
    }.or(async {
        smol::Timer::after(Duration::from_secs(options.connection_timeout())).await;
        Err(ConnectionError::Timeout(Elapsed))
    }).await?;
    /*let packet = time::timeout(Duration::from_secs(options.connection_timeout()), async {
        match network.read().await? {
            Incoming::ConnAck(connack) if connack.code == ConnectReturnCode::Success => {
                Ok(Packet::ConnAck(connack))
            }
            Incoming::ConnAck(connack) => Err(ConnectionError::ConnectionRefused(connack.code)),
            packet => Err(ConnectionError::NotConnAck(packet)),
        }
    })
    .await??;*/

    Ok(packet)
}

/// Returns the next pending packet asynchronously to be used in select!
/// This is a synchronous function but made async to make it fit in select!
pub(crate) async fn next_pending(
    delay: Duration,
    pending: &mut IntoIter<Request>,
) -> Option<Request> {
    // return next packet with a delay
    smol::Timer::after(delay).await;
    // time::sleep(delay).await;
    pending.next()
}
