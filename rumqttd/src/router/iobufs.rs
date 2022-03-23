use std::{collections::VecDeque, sync::Arc, time::Instant};

use bytes::Bytes;
use flume::{Receiver, Sender};
use parking_lot::Mutex;

use crate::{
    protocol::v4::{
        self,
        publish::{Publish, PublishBytes},
    },
    router::{FilterIdx, MAX_CHANNEL_CAPACITY},
    Cursor, Notification,
};

const MAX_INFLIGHT: u16 = 200;
const INIT: Option<(usize, Cursor)> = None;

#[derive(Debug)]
pub struct Incoming {
    /// Identifier associated with connected client
    pub(crate) client_id: String,
    // Recv buffer
    pub(crate) buffer: Arc<Mutex<VecDeque<Bytes>>>,
    // incoming metrics
    pub(crate) meter: IncomingMeter,
}

#[derive(Debug)]
pub struct IncomingMeter {
    pub(crate) publish_count: usize,
    pub(crate) subscribe_count: usize,
    pub(crate) total_size: usize,
    pub(crate) last_timestamp: Instant,
    start: Instant,
    pub(crate) data_rate: usize,
}

#[derive(Debug)]
pub struct Outgoing {
    /// Identifier associated with connected client
    pub(crate) client_id: String,
    /// Send buffer
    pub(crate) buffer: Arc<Mutex<VecDeque<Notification>>>,
    /// Handle which is given to router to allow router to communicate with this connection
    pub(crate) handle: Sender<()>,
    /// Oldest unacknowledged packet
    head: u16,
    /// The next pkid to push to
    tail: u16,
    /// Number of packet inflight. Even if this is not equal to `MAX_INFLIGHT`, it does not mean
    /// there is any slot free for pushing packets.
    inflight: u16,
    /// The buffer to keep track of inflight packets.
    inflight_buffer: [Option<(FilterIdx, Cursor)>; MAX_INFLIGHT as usize],
    pub(crate) meter: OutgoingMeter,
}

#[derive(Debug)]
pub struct OutgoingMeter {
    pub(crate) publish_count: usize,
    pub(crate) total_size: usize,
    pub(crate) last_timestamp: Instant,
    start: Instant,
    pub(crate) data_rate: usize,
}

impl Incoming {
    #[inline]
    pub(crate) fn new(client_id: String) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_CHANNEL_CAPACITY))),
            meter: Default::default(),
            client_id,
        }
    }

    #[inline]
    pub(crate) fn buffer(&self) -> Arc<Mutex<VecDeque<Bytes>>> {
        self.buffer.clone()
    }

    #[inline]
    pub(crate) fn pop(&self) -> Option<Bytes> {
        self.buffer.lock().pop_front()
    }
}

impl Outgoing {
    #[inline]
    pub(crate) fn new(client_id: String) -> (Self, Receiver<()>) {
        let (handle, rx) = flume::bounded(MAX_CHANNEL_CAPACITY);
        (
            Self {
                client_id,
                buffer: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_CHANNEL_CAPACITY))),
                handle,
                head: 0,
                tail: 0,
                inflight: 0,
                inflight_buffer: [INIT; MAX_INFLIGHT as usize],
                meter: Default::default(),
            },
            rx,
        )
    }

    #[inline]
    pub(crate) fn buffer(&self) -> Arc<Mutex<VecDeque<Notification>>> {
        self.buffer.clone()
    }

    pub fn free_slots(&self) -> u16 {
        match self.inflight {
            // [X X X X X X X X X X X X X X]
            MAX_INFLIGHT => 0,
            // [O O O O O O O O O O O O O O]
            0 => MAX_INFLIGHT,
            // [O O O X X X X X X X O O O O]
            _ if self.head < self.tail => MAX_INFLIGHT - (self.tail - self.head),
            // [X X X O O O O O O O X X X X]
            _ => self.head - self.tail,
        }
    }

    pub fn push_notification(&mut self, notification: Notification) -> usize {
        let mut buffer = self.buffer.lock();
        buffer.push_back(notification);
        buffer.len()
    }

    pub fn push_device_data(
        &mut self,
        publishes: Vec<Bytes>,
        qos: u8,
        filter_idx: usize,
        cursor: Cursor,
    ) -> usize {
        let mut buffer = self.buffer.lock();
        self.meter.publish_count += publishes.len();
        let mut total_size = 0;

        // TODO: Pass an iterateor of (topic, payload) and remove use of protocol from this module?
        if qos == 0 {
            for p in publishes {
                self.meter.total_size += p.len();
                total_size += p.len();

                let publish: Result<Publish, v4::Error> = PublishBytes(p).into();
                let publish = publish.unwrap();
                let (topic, payload) = publish.take_topic_and_payload().unwrap();
                let notification = Notification::Forward {
                    cursor,
                    size: 0,
                    topic,
                    qos: 0,
                    pkid: 0,
                    payload,
                };

                buffer.push_back(notification);
            }
            self.meter.update_data_rate(total_size);
            return buffer.len();
        }

        for p in publishes {
            self.meter.total_size += p.len();
            total_size += p.len();

            let publish: Result<Publish, v4::Error> = PublishBytes(p).into();
            let publish = publish.unwrap();
            let (topic, payload) = publish.take_topic_and_payload().unwrap();
            self.inflight_buffer[self.tail as usize] = Some((filter_idx, cursor));
            self.inflight += 1;

            let notification = Notification::Forward {
                cursor,
                size: 0,
                topic,
                qos: 1,
                pkid: self.tail + 1,
                payload,
            };

            if self.tail == MAX_INFLIGHT - 1 {
                self.tail = 0;
            } else {
                self.tail += 1;
            }
            buffer.push_back(notification);
        }

        debug!("head {} tail {} inflight {}", self.head, self.tail, self.inflight);
        self.meter.update_data_rate(total_size);
        buffer.len()
    }

    // Returns (unsolicited, outoforder) flags
    pub fn register_ack(&mut self, pkid: u16) -> (bool, bool) {
        if self.inflight == 0 {
            return (true, false);
        }

        let idx = pkid - 1;

        // We don't support out of order acks
        if idx != self.head {
            return (false, true);
        }

        // Inorder acks
        // as head always has `Some` if inflight > 0, unwrap here is fine.
        // TODO: use get() instead of index. Bad clients can send random
        // packet id acks which leads to crash
        // TODO: We need to save filter and last acked offset. When a connection
        // is diconnected in persistent session, we need to rewind the cursor in
        // tracker's data request to last acked offset to retransmit unacked qos
        // 1 publishes
        self.inflight_buffer[idx as usize].take().unwrap();
        if self.head == MAX_INFLIGHT - 1 {
            self.head = 0;
        } else {
            self.head += 1;
        }

        self.inflight -= 1;
        (false, false)
    }
}

impl Default for IncomingMeter {
    fn default() -> Self {
        Self {
            publish_count: 0,
            subscribe_count: 0,
            total_size: 0,
            last_timestamp: Instant::now(),
            start: Instant::now(),
            data_rate: 0,
        }
    }
}

impl IncomingMeter {
    pub(crate) fn update_data_rate(&mut self, new_packet_size: usize) {
        let now = Instant::now();
        self.data_rate =
            new_packet_size / now.duration_since(self.last_timestamp).as_micros() as usize;
        self.last_timestamp = now;
    }

    pub(crate) fn average_last_data_rate(&mut self) {
        let now = Instant::now();
        self.data_rate = self.total_size / now.duration_since(self.start).as_micros() as usize;
        self.last_timestamp = now;
    }
}

impl Default for OutgoingMeter {
    fn default() -> Self {
        Self {
            publish_count: 0,
            total_size: 0,
            last_timestamp: Instant::now(),
            start: Instant::now(),
            data_rate: 0,
        }
    }
}

impl OutgoingMeter {
    pub(crate) fn update_data_rate(&mut self, new_packet_size: usize) {
        let now = Instant::now();
        self.data_rate =
            new_packet_size / now.duration_since(self.last_timestamp).as_micros() as usize;
        self.last_timestamp = now;
    }

    pub(crate) fn average_last_data_rate(&mut self) {
        let now = Instant::now();
        self.data_rate = self.total_size / now.duration_since(self.start).as_micros() as usize;
        self.last_timestamp = now;
    }
}
