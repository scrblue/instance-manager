use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// Messages between an admin console and any manager on the network
pub mod console_manager;
pub use console_manager::*;
/// Messages between an admin console and the manager network
pub mod console_network;
pub use console_network::*;

/// Messages requesting information from any manager on the network
pub mod instance_manager;
pub use instance_manager::*;
/// Messages on spawning and dropping instances from any other instance to the manager network
pub mod instance_network;
pub use instance_network::*;

/// Messages between managers on the network -- for use in peer discovery, validation, and the
/// transmission of the actual Raft requests and responses
pub mod manager_manager;
pub use manager_manager::*;
/// Messages between a manager and the whole network -- on sharing and updating configuration or
/// updating the physical server's health
pub mod manager_network;
pub use manager_network::*;

/// The message sent when forming a connection with an instance manager simply defining the instance
/// type, and thus what message types are to be used in deserialization. All messages include the
/// SocketAddr that the connection in question listens on such that they can be added to the
/// distributed state
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum Greeting {
    Console(SocketAddr),
    Instance(SocketAddr),
    Peer(SocketAddr),
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum IncomingMessage {
    ManagerManagerRequest(ManagerManagerRequest),
    ConsoleManagerRequest(ConsoleManagerRequest),
    InstanceManagerRequest(InstanceManagerRequest),
    RaftRequest(RaftRequest),
}

/// A simple struct with statistics on physical server performance to be used in deciding the
/// server to spawn an instance on
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ServerHealth {
    pub ports_free: u8,
    pub cpu_use_as_decimal_fraction: f32,
    pub ram_use_as_decimal_fraction: f32,
    pub ram_free_in_mb: u32,
}

/// A generic message type that contains a payload of the resquests and responses defined in this
/// module's submodules
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct InstanceManagerMessage<T> {
    pub id: u64,
    pub payload: T,
}
