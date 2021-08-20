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

    manager_manager::*,
    manager_network::*,

    Greeting,
};

use crate::configuration;

use async_raft::raft::*;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, convert::TryInto, net::SocketAddr};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum IncomingMessage {
    ManagerManagerRequest(ManagerManagerRequest),
    ConsoleManagerRequest(ConsoleManagerRequest),
    InstanceManagerRequest(InstanceManagerRequest),
    RaftRequest(RaftRequest),
}
