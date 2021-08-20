use crate::messages::ManagerManagerRequest;

pub mod peer_connection;
pub mod peer_tracker;

#[derive(Debug)]
pub enum ConnectionToTracker {
    Request(ManagerManagerRequest),
}

#[derive(Debug)]
pub enum TrackerToConnection {
    Shutdown,
}
