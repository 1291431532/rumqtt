use std::collections::{HashMap, HashSet};

use super::{tracker::Tracker, ConnectionMeter};

pub struct Graveyard {
    connections: HashMap<String, SavedState>,
}

impl Graveyard {
    pub fn new() -> Graveyard {
        Graveyard {
            connections: HashMap::new(),
        }
    }

    /// Add a new connection.
    /// Return tracker of previous connection if connection id already exists
    pub fn retrieve(&mut self, id: &str) -> Option<SavedState> {
        self.connections.remove(id)
    }

    /// Save connection tracker
    pub fn save(
        &mut self,
        id: &str,
        mut tracker: Tracker,
        subscriptions: HashSet<String>,
        metrics: ConnectionMeter,
    ) {
        tracker.set_busy_unschedule(false);

        // No need to save acks request. Client will any resend unacked publishes
        tracker.clear_acks_request();

        self.connections.insert(
            id.to_owned(),
            SavedState {
                tracker,
                subscriptions,
                metrics,
            },
        );
    }
}

#[derive(Debug, Default)]
pub struct SavedState {
    pub tracker: Tracker,
    pub subscriptions: HashSet<String>,
    pub metrics: ConnectionMeter,
}
