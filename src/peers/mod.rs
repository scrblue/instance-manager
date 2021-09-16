use crate::messages::{ManagerManagerRequest, ManagerManagerResponse};

pub mod peer_connection;
pub mod peer_tracker;

#[derive(Debug)]
pub enum ConnectionToTracker {
    Request(ManagerManagerRequest),
    Response(ManagerManagerResponse),
}

#[derive(Debug)]
pub enum TrackerToConnection {
    Shutdown,
    Request(ManagerManagerRequest),
    Response(ManagerManagerResponse),
    CoreConfigMatch,
    CoreConfigNoMatch,
}
