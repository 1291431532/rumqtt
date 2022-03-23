use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use std::io::{self, ErrorKind};
use tokio::time::{self, error::Elapsed, Duration};

use crate::protocol::v4::{
    self, check,
    connack::{self, ConnectReturnCode},
    connect::Connect,
    read_mut, Packet,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O = {0}")]
    Io(#[from] io::Error),
    #[error("Invalid data = {0}")]
    Mqtt(#[from] v4::Error),
    #[error["Keep alive timeout"]]
    KeepAlive(#[from] Elapsed),
}

/// Network transforms packets <-> frames efficiently. It takes
/// advantage of pre-allocation, buffering and vectorization when
/// appropriate to achieve performance
pub struct Network {
    /// Socket for IO
    socket: Box<dyn N>,
    /// Buffered reads
    read: BytesMut,
    /// Maximum packet size
    max_incoming_size: usize,
    /// Maximum readv count
    max_readb_count: usize,
    /// Keep alive timeout
    keepalive: Duration,
}

impl Network {
    pub fn new(socket: Box<dyn N>, max_incoming_size: usize) -> Network {
        Network {
            socket,
            read: BytesMut::with_capacity(10 * 1024),
            max_incoming_size,
            max_readb_count: 10,
            keepalive: Duration::from_secs(0),
        }
    }

    pub fn set_keepalive(&mut self, keepalive: u16) {
        let keepalive = Duration::from_secs(keepalive as u64);
        self.keepalive = keepalive + keepalive.mul_f32(0.5);
    }

    /// Reads more than 'required' bytes to frame a packet into self.read buffer
    async fn read_bytes(&mut self, required: usize) -> io::Result<usize> {
        let mut total_read = 0;
        loop {
            let read = self.socket.read_buf(&mut self.read).await?;
            if 0 == read {
                let error = if self.read.is_empty() {
                    io::Error::new(ErrorKind::ConnectionAborted, "connection closed by peer")
                } else {
                    io::Error::new(ErrorKind::ConnectionReset, "connection reset by peer")
                };

                return Err(error);
            }

            total_read += read;
            if total_read >= required {
                return Ok(total_read);
            }
        }
    }

    pub async fn read(&mut self) -> Result<Packet, io::Error> {
        loop {
            let required = match read_mut(&mut self.read, self.max_incoming_size) {
                Ok(packet) => return Ok(packet),
                Err(v4::Error::InsufficientBytes(required)) => required,
                Err(e) => return Err(io::Error::new(ErrorKind::InvalidData, e.to_string())),
            };

            // read more packets until a frame can be created. This function
            // blocks until a frame can be created. Use this in a select! branch
            self.read_bytes(required).await?;
        }
    }

    /// Read packets in bulk. This allow replies to be in bulk. This method is used
    /// after the connection is established to read a bunch of incoming packets
    pub async fn readb(&mut self) -> Result<BytesMut, Error> {
        let mut count = 0;
        let mut cursor = 0;

        // TODO: Fuzz test this method across several combinations of data. Faced a cursor bug
        loop {
            match check(self.read[cursor..].iter(), self.max_incoming_size) {
                Ok(header) => {
                    cursor += header.frame_length();
                    count += 1;

                    if count >= self.max_readb_count {
                        let o = self.read.split_to(cursor);
                        return Ok(o);
                    }
                }
                Err(v4::Error::InsufficientBytes(required)) => {
                    if count > 0 {
                        let o = self.read.split_to(cursor);
                        return Ok(o);
                    }

                    // Wait for required number of bytes to frame a packet
                    time::timeout(self.keepalive, async {
                        self.read_bytes(required).await?;
                        Ok::<_, Error>(())
                    })
                    .await??;
                }
                Err(e) => return Err(Error::Mqtt(e)),
            }
        }
    }

    pub async fn read_connect(&mut self, t: u16) -> io::Result<Connect> {
        let packet = time::timeout(Duration::from_millis(t.into()), async {
            let packet = self.read().await?;
            Ok::<_, io::Error>(packet)
        })
        .await??;

        match packet {
            Packet::Connect(connect) => {
                if connect.keep_alive == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionRefused,
                        "0 keep alive not supported",
                    ));
                }

                Ok(connect)
            }
            packet => {
                let error = format!("Expecting connect. Received = {:?}", packet);
                Err(io::Error::new(io::ErrorKind::InvalidData, error))
            }
        }
    }

    pub async fn connack(
        &mut self,
        code: ConnectReturnCode,
        session: bool,
    ) -> Result<usize, io::Error> {
        let mut write = BytesMut::new();
        let len = match connack::write(code, session, &mut write) {
            Ok(size) => size,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
        };

        self.socket.write_all(&write[..]).await?;
        Ok(len)
    }

    pub async fn write_all(&mut self, write: &[u8]) -> Result<(), io::Error> {
        if write.is_empty() {
            return Ok(());
        }

        self.socket.write_all(write).await?;
        Ok(())
    }
}

pub trait N: AsyncRead + AsyncWrite + Send + Sync + Unpin {}
impl<T> N for T where T: AsyncRead + AsyncWrite + Unpin + Send + Sync {}
