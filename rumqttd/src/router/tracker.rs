use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

use super::{AcksRequest, DataRequest};

/// Used to register a new connection with the router
/// Connection messages encompasses a handle for router to
/// communicate with this connection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tracker {
    /// Connection unscheduled (from ready queue)
    /// due to connection being busy (channel full)
    busy_unschedule: bool,
    /// Connection unscheduled (from ready queue) due
    /// all the data and ack requests being caughtup
    caughtup_unschedule: bool,
    /// Connection unscheduled (from inflight queue) due
    /// to inflight queue being full
    inflight_full_unschedule: bool,
    /// Data requests of all the subscriptions of this connection
    data_requests: VecDeque<DataRequest>,
    /// Acks request
    acks_request: Option<AcksRequest>,
}

impl Default for Tracker {
    fn default() -> Tracker {
        let requests = VecDeque::with_capacity(2);
        Tracker {
            busy_unschedule: false,
            caughtup_unschedule: true,
            inflight_full_unschedule: false,
            data_requests: requests,
            acks_request: None,
        }
    }
}

impl Tracker {
    pub fn new() -> Tracker {
        let requests = VecDeque::with_capacity(2);
        Tracker {
            busy_unschedule: false,
            caughtup_unschedule: true,
            inflight_full_unschedule: false,
            data_requests: requests,
            acks_request: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data_requests.is_empty()
    }

    pub fn get_data_requests(&self) -> &VecDeque<DataRequest> {
        &self.data_requests
    }

    pub fn busy_unschedule(&self) -> bool {
        self.busy_unschedule
    }

    pub fn caughtup_unschedule(&self) -> bool {
        self.caughtup_unschedule
    }

    pub fn set_busy_unschedule(&mut self, b: bool) {
        self.busy_unschedule = b;
    }

    pub fn set_caughtup_unschedule(&mut self, b: bool) {
        self.caughtup_unschedule = b;
    }

    pub fn set_inflight_full_unschedule(&mut self, b: bool) {
        self.inflight_full_unschedule = b;
    }

    pub fn next_data_request(&mut self) -> Option<DataRequest> {
        self.data_requests.pop_front()
    }

    pub fn inflight_full_unschedule(&self) -> bool {
        self.inflight_full_unschedule
    }

    pub fn next_acks_request(&mut self) -> Option<AcksRequest> {
        self.acks_request.take()
    }

    pub fn register_data_request(&mut self, request: DataRequest) {
        self.data_requests.push_back(request);
    }

    pub fn register_acks_request(&mut self) {
        self.acks_request = Some(AcksRequest)
    }

    pub fn clear_acks_request(&mut self) {
        self.acks_request = None
    }
}
