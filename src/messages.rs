pub use common_model::instance_management::{
    // Defines the ConsoleManagerRequest and Response types which return information on instance
    // socket addresses and server health information for administrative purposes
    console_manager::*,
    // Defines the ConsoleNetworkRequest and Response types which for now handle updating shared
    // configuration, bootstrapping and shutting down the entire system, adding new managers to the
    // network, and updating instances in the instance store
    console_network::*,

    // Defines the InstanceManagerRequest, InstanceManagerResponse, and InstanceInfoNotification which
    // all deal with requesting and notifying existing instances about the `SocketAddr`s of other
    // instances
    instance_manager::*,
    // Defines the InstanceNetworkRequest and the InstanceNetworkResponse which handle the spawning
    // and dropping of instances in the tree.
    instance_network::*,
};

use crate::configuration;

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr};

#[derive(Debug, Serialize, Deserialize)]
pub enum IncomingMessage {
    ManagerManagerRequest(ManagerManagerRequest),
    ConsoleManagerRequest(ConsoleManagerRequest),
    InstanceManagerRequest(InstanceManagerRequest),
    RaftRequest(RaftRequest),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ManagerManagerRequest {
    /// For when initiating a connection -- the SocketAddr is the public IP of the peer
    Greeting(SocketAddr),

    AppendEntries {
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<RaftRequest>,
        leader_commit: u64,
    },
    InstallSnapshot {
        term: u64,
        leader_id: u64,
        last_included_index: u64,
        offsest: u64,
        data: Vec<u8>,
        done: bool,
    },
    RequestVote {
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum RaftRequest {
    ConsoleNetworkRequest(ConsoleNetworkRequest),
    InstanceNetworkRequest(InstanceNetworkRequest),
    UpdateServerHealth {
        manager_id: u64,
        ports_free: u8,
        cpu_use_as_decimal_fraction: f32,
        ram_use_as_decimal_fraction: f32,
        ram_free_in_mb: u32,
    },
    ShareInstanceStore(HashMap<String, configuration::InstanceConf>),
}
impl async_raft::AppData for RaftRequest {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RaftResponse {
    ConsoleNetworkResponse(ConsoleNetworkResponse),
    InstanceNetworkResponse(InstanceNetworkResponse),
}
impl async_raft::AppDataResponse for RaftResponse {}
