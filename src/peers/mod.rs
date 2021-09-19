use crate::messages::{
    InstanceManagerMessage, ManagerManagerPayload, ManagerManagerRequest, ManagerManagerResponse,
};

use tokio::sync::oneshot;

pub mod peer_connection;
pub mod peer_tracker;

#[derive(Debug)]
pub enum ConnectionToTracker {
    Request(InstanceManagerMessage<ManagerManagerRequest>),
}

pub enum TrackerToConnection {
    Shutdown,
    Request(
        InstanceManagerMessage<ManagerManagerRequest>,
        oneshot::Sender<ManagerManagerResponse>,
    ),
    /// Only for unexpected Responses -- expected responses are sent via the Sender given in the
    /// request
    Response(InstanceManagerMessage<ManagerManagerResponse>),

    CoreConfigMatch(u64),
    CoreConfigNoMatch(u64),
}

impl std::fmt::Debug for TrackerToConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrackerToConnection::Shutdown => write!(f, "TrackerToConnection::Shutdown"),
            TrackerToConnection::Request(msg, _) => {
                write!(f, "TrackerToConnection::Request({:?}, _)", msg)
            }
            TrackerToConnection::Response(msg) => {
                write!(f, "TrackerToConnection::Response({:?})", msg)
            }
            TrackerToConnection::CoreConfigMatch(id) => {
                write!(f, "TrackerToConnection::CoreConfigMatch({})", id)
            }
            TrackerToConnection::CoreConfigNoMatch(id) => {
                write!(f, "TrackerToConnection::CoreConfigNoMatch({})", id)
            }
        }
    }
}
