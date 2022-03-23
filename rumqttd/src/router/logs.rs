use super::Ack;
use slab::Slab;

use crate::protocol::matches;
use crate::protocol::v4::publish::Publish;
use crate::router::{DataRequest, FilterIdx, SubscriptionMeter, Waiters};
use crate::segments::{CommitLog, Position};
use crate::{ConnectionId, Offset, RouterConfig, MAX_NODES};

use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::path::PathBuf;

type Filter = String;

const REPLICA_DIR: &str = "replica";
const NATIVE_DIR: &str = "native";

/// Stores 'device' data and 'actions' data in native commitlog
/// organized by subscription filter. Device data is replicated
/// while actions data is not
pub struct DataLog {
    pub config: RouterConfig,
    /// Native commitlog data organized by subscription. Contains
    /// 'device data' and 'actions data' logs. Device data is replicated
    /// while actions data is not
    /// Also has waiters used to wake connections/replicator tracker
    /// which are caught up with all the data on 'Filter' and waiting
    /// for new data
    native: Slab<Data>,
    sub_map: HashMap<Filter, FilterIdx>,
    pub_map: HashMap<String, Vec<FilterIdx>>,
    /// Other node's Replicated log of 'device data' subscription
    _replicas: [CommitLog; MAX_NODES],
    /// Parked requests that are ready because of new data on the subscription
    notifications: VecDeque<(ConnectionId, DataRequest)>,
    data_path: Option<PathBuf>,
}

impl DataLog {
    pub fn new(config: RouterConfig) -> io::Result<DataLog> {
        let (replica_path, data_path) = config
            .log_dir
            .as_ref()
            .map(|path| (Some(path.join(REPLICA_DIR)), Some(path.join(NATIVE_DIR))))
            .unwrap_or((None, None));

        let r1 = commitlog(
            replica_path.as_ref().map(|path| path.join(1.to_string())),
            config.max_segment_size,
            config.max_mem_segments,
            config.max_disk_segments,
        )?;
        let r2 = commitlog(
            replica_path.as_ref().map(|path| path.join(2.to_string())),
            config.max_segment_size,
            config.max_mem_segments,
            config.max_disk_segments,
        )?;
        let r3 = commitlog(
            replica_path.as_ref().map(|path| path.join(3.to_string())),
            config.max_segment_size,
            config.max_mem_segments,
            config.max_disk_segments,
        )?;
        let replicas = [r1, r2, r3];

        let native = Slab::new();
        let pub_map = HashMap::new();
        let sub_map = HashMap::new();

        Ok(DataLog {
            config,
            _replicas: replicas,
            native,
            pub_map,
            sub_map,
            notifications: VecDeque::new(),
            data_path,
        })
    }

    pub fn meter(&self, filter: &str) -> Option<SubscriptionMeter> {
        self.native
            .get(*self.sub_map.get(filter)?)
            .map(|data| data.meter.clone())
    }

    pub fn waiters(&self, filter: &Filter) -> Option<&Waiters<DataRequest>> {
        self.native
            .get(*self.sub_map.get(filter)?)
            .map(|data| &data.waiters)
    }

    pub fn get_topic(&self, filter_idx: FilterIdx) -> Option<&str> {
        self.native.get(filter_idx).map(|data| data.filter.as_str())
    }

    pub fn next_native_offset(&mut self, filter: &str) -> (FilterIdx, Offset) {
        let (filter_idx, data) = match self.sub_map.get(filter) {
            Some(idx) => (*idx, self.native.get(*idx).unwrap()),
            None => {
                let max_segment_size = self.config.max_segment_size;
                let max_mem_segments = self.config.max_mem_segments;
                let max_disk_segments = self.config.max_disk_segments;

                let log = disk_or_inmemory_commitlog(
                    self.data_path.clone(),
                    max_segment_size,
                    max_mem_segments,
                    max_disk_segments,
                );

                let waiters = Waiters::with_capacity(10);
                let metrics = SubscriptionMeter::default();
                let data = Data {
                    filter: filter.to_owned(),
                    log,
                    waiters,
                    meter: metrics,
                };

                let idx = self.native.insert(data);
                self.sub_map.insert(filter.to_owned(), idx);
                (idx, self.native.get(idx).unwrap())
            }
        };

        (filter_idx, data.log.next_offset())
    }

    pub fn native_append(&mut self, publish: Publish) -> Result<Vec<(Offset, FilterIdx)>, Publish> {
        let topic = publish.view_topic().unwrap();

        let filter_idxs = match self.pub_map.get(topic) {
            Some(idxs) => idxs,
            None => {
                // create a new vec, and check all existing filters for matches
                let mut v = Vec::new();
                for (filter, filter_idx) in self.sub_map.iter() {
                    if matches(topic, filter) {
                        v.push(*filter_idx);
                    }
                }

                // no pubs allowed on topics with no existing subs
                if v.is_empty() {
                    return Err(publish);
                }

                self.pub_map.insert(topic.to_owned(), v);
                self.pub_map.get(topic).unwrap()
            }
        };

        // no pubs allowed on topics with no existing subs
        if filter_idxs.is_empty() {
            return Err(publish);
        }

        let mut offsets_and_filters = Vec::with_capacity(filter_idxs.len());

        for filter_idx in filter_idxs {
            let data = self.native.get_mut(*filter_idx).unwrap();
            let size = publish.raw.len();
            let offset = data.log.append(publish.raw.clone());
            if let Some(mut parked) = data.waiters.take() {
                self.notifications.append(&mut parked);
            }

            data.meter.count += 1;
            data.meter.append_offset = offset;
            data.meter.total_size += size;
            data.meter.head_and_tail_id = data.log.head_and_tail();

            // TODO: avoid this clone
            offsets_and_filters.push((offset, *filter_idx));
        }

        Ok(offsets_and_filters)
    }

    pub fn native_readv(
        &self,
        filter_idx: FilterIdx,
        offset: Offset,
        len: u64,
    ) -> io::Result<(Position, Vec<Bytes>)> {
        // unwrap to get index of `self.native` is fine here, because when a new subscribe packet
        // arrives in `Router::handle_device_payload`, it first calls the function
        // `next_native_offset` which creates a new commitlog if one doesn't exist. So any new
        // reads will definitely happen on a valid filter.
        let data = self.native.get(filter_idx).unwrap();
        let mut o = Vec::new();
        let next = data.log.readv(offset, len, &mut o)?;
        Ok((next, o))
    }

    pub fn shadow(&mut self, filter: &str) -> Option<Bytes> {
        let data = self.native.get_mut(*self.sub_map.get(filter)?)?;
        data.log.last()
    }

    /// This method is called when the subscriber has caught up with the commit log. In which case,
    /// instead of actively checking for commits in each `Router::run_inner` iteration, we instead
    /// wait and only try reading again when new messages have been added to the commit log. This
    /// methods converts a `DataRequest` (which actively reads the commit log in `Router::consume`)
    /// to a `Waiter` (which only reads when notified).
    pub fn park(&mut self, id: ConnectionId, request: DataRequest) {
        // calling unwrap on index here is fine, because only place this function is called is in
        // `Router::consume` method, when the status after reading from commit log of the same
        // filter as `request` is "done", that is, the subscriber has caught up. In other words,
        // there has been atleast 1 call to `native_readv` for the same filter, which means if
        // `native_readv` hasn't paniced, so this won't panic either.
        let data = self.native.get_mut(request.filter_idx).unwrap();
        data.waiters.register(id, request);
    }

    pub fn pop_notification(&mut self) -> Option<(ConnectionId, DataRequest)> {
        self.notifications.pop_front()
    }

    /// Cleanup a connection from all the waiters
    pub fn clean(&mut self, id: ConnectionId) -> Vec<DataRequest> {
        let mut inflight = Vec::new();
        for (_, data) in self.native.iter_mut() {
            inflight.append(&mut data.waiters.remove(id));
        }

        inflight
    }
}

struct Data {
    filter: Filter,
    log: CommitLog,
    waiters: Waiters<DataRequest>,
    meter: SubscriptionMeter,
}

fn commitlog(
    path: Option<PathBuf>,
    max_segment_size: usize,
    max_mem_segments: usize,
    max_disk_segments: usize,
) -> Result<CommitLog, io::Error> {
    let commitlog = match path {
        Some(p) => CommitLog::new(
            max_segment_size,
            max_mem_segments,
            Some((p, max_disk_segments)),
        )?,
        None => CommitLog::new(max_segment_size, max_mem_segments, None)?,
    };

    Ok(commitlog)
}

// Tries to create commit log on disk, and if fails then falls back to in-memory logs. If in-memory
// logs also fail, then there are probably other more concerning this to be taken care of than
// commit log not being created.
fn disk_or_inmemory_commitlog(
    path: Option<PathBuf>,
    max_segment_size: usize,
    max_mem_segments: usize,
    max_disk_segments: usize,
) -> CommitLog {
    match CommitLog::new(
        max_segment_size,
        max_mem_segments,
        path.map(|path| (path, max_disk_segments)),
    ) {
        Ok(commit_log) => commit_log,
        Err(e) => {
            log::error!(
                "Failed to create log on disk, falling back to in-memory: {:?}",
                e
            );
            // this unwrap fine as if `None` is passed then there are no io errors, and as we
            // guarantee that segment size requirements are met.
            //
            // NOTE: this depends upon the failing conditions of `CommitLog::new`. If they are ever
            // changed in future, this unwrap might no longer be fine.
            CommitLog::new(max_segment_size, max_mem_segments, None).unwrap()
        }
    }
}

// mod test {
//     use crate::RouterConfig;
//     use crate::logs::data::DataLog;
//     use mqttbytes::v4::Publish;
//     use mqttbytes::QoS;
//
//     #[test]
//     fn appends_are_written_to_correct_commitlog() {
//         pretty_env_logger::init();
//         let config = RouterConfig {
//             data_filter: "/devices/+/events/+/+".to_owned(),
//             instant_ack: true,
//             max_segment_size: 1024,
//             max_segment_count: 10,
//             max_connections: 10
//         };
//
//         let mut data = DataLog::new(config);
//         data.next_native_offset("/devices/2321/actions");
//         for i in 0..2 {
//             let publish = Publish::new("/devices/2321/events/imu/jsonarray", QoS::AtLeastOnce, vec![1, 2, 3]);
//             let v = data.native_append(publish);
//             dbg!(v);
//         }
//
//         for i in 0..2 {
//             let publish = Publish::new("/devices/2321/actions", QoS::AtLeastOnce, vec![1, 2, 3]);
//             let v = data.native_append(publish);
//             dbg!(v);
//         }
//     }
// }

/// Acks log for a subscription
#[derive(Debug)]
pub struct AcksLog {
    // Connection id, cursor, packet id
    inflight: VecDeque<(ConnectionId, Offset, u16)>,
    // Committed acks per connection. First pkid, last pkid, data
    committed: Slab<VecDeque<Ack>>,
    /// Farthest committed offset
    committed_offset: Offset,
}

impl AcksLog {
    pub fn with_capacity(cap: usize) -> AcksLog {
        AcksLog {
            inflight: VecDeque::new(),
            committed: Slab::with_capacity(cap),
            committed_offset: (0, 0),
        }
    }

    pub fn insert(&mut self) -> usize {
        self.committed.insert(VecDeque::new())
    }

    pub fn clear(&mut self, id: ConnectionId) {
        let c = self.committed.get_mut(id).unwrap();
        c.clear()
    }

    pub fn remove(&mut self, id: ConnectionId) {
        self.committed.remove(id);
    }

    /// Push a new ack to log
    pub fn push(&mut self, id: ConnectionId, offset: (u64, u64), pkid: u16) {
        self.inflight.push_back((id, offset, pkid));
    }

    /// Commit ack
    pub fn commit(&mut self, id: ConnectionId, ack: Ack) {
        let c = self.committed.get_mut(id).unwrap();
        c.push_back(ack);
    }

    //     /// Commit until replicated offset
    //     pub fn commit(&mut self, cursor: (u64, u64), mut apply: impl FnMut(ConnectionId)) {
    //         // When there are 2 replicas, one replica might be slower.
    //         // This fence is used to not send duplicate acks to the
    //         // connection because of multiple replica acks
    //         if cursor <= self.committed_offset {
    //             return;
    //         }

    //         while let Some((id, offset, pkid)) = self.inflight.pop_front() {
    //             self.puback_commit(id, pkid);
    //             apply(id);

    //             if offset == cursor {
    //                 break;
    //             }
    //         }

    //         self.committed_offset = cursor;
    //     }

    pub fn readv(&mut self, id: ConnectionId) -> &mut VecDeque<Ack> {
        let committed = self.committed.get_mut(id).unwrap();
        committed
    }
}
