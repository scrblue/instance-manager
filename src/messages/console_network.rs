use crate::configuration::{instance::InstanceConfig, manager::CoreConfig};

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ConsoleNetworkRequest {
    /// A request to proliferate a new core configuration among the manager nodes
    NewCoreConfiguration(CoreConfig),

    /// A request to update the default instance store for all managers on the network
    UpdateSharedConfig {
        /// The instance name for which the updated configuration is being sent
        name: String,
        /// Will be Some for create or update requests and None for destroy requests
        configuration: Option<InstanceConfig>,
    },

    /// A request for all manager nodes to connect to a new node trying to join the network
    AddPeer {
        address: SocketAddr,
        add_to_core: bool,
    },

    /// A request to launch all instances requisite for the functioning of the system
    Bootstrap,

    /// A request to shutdown a manager and its associated instances
    Shutdown(u64),

    /// A request to stop spawning instances from a specified manager such as to shut it down
    /// eventually
    SoftShutdown(u64),

    // A request for the specified manager to run an arbitrary method for an  existing instances
    // in the store such as to handle installations, rollout updates, or manually spawn an instance
    RunMethod {
        manager: u64,
        instance_name: String,
        method_name: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ConsoleNetworkResponse {
    Success,
    Failure,
}
