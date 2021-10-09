use serde::{Deserialize, Serialize};

use std::net::SocketAddr;

/// A request to spawn a given instance in the instance store anywhere in the network
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum InstanceNetworkRequest {
    /// A request to spawn a new instance with the given name as a child of the requesting instance
    SpawnChild {
        /// A randomly generated ID used to identify the request in the short-term
        request_id: usize,
        /// The String name used to identify the instance type to be spawned
        instance_name: String,
        /// Additional arguments to spawn the instance with -- this depends on the type of instance, but
        /// it could be like a location ID for a player housing instance so the instance knows what map
        /// to fetch, what players to allow to join, etc
        with_args: Vec<String>,
        /// A list of server instances to notify about the new instance in the network -- eg a station
        /// that a house is in needs to know about it so it can be displayed as being available to request
        /// entry for
        notify: Vec<uuid::Uuid>,
    },

    /// A request of an instance to remove itself from the tree
    // TODO: What to do if the instance has children?
    DropSelf(uuid::Uuid),

    UpdateRelationships(()),
}

/// The information related to an instance which has just been spawned
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum InstanceNetworkResponse {
    InstanceInfoResponse {
        /// The request ID relayed back to the requester to identify which server has been spawned
        request_id: usize,

        /// An instance path generated based on the new instance'3 position in the tree
        path: uuid::Uuid,

        /// The address that other instances can connect to in order to communicate with the new instance
        socket_addr: SocketAddr,
    },

    Success,
    Failure,
}
