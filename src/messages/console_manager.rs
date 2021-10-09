use crate::configuration::{instance::InstanceConfig, manager::CoreConfig};
use super::ServerHealth;

use serde::{Deserialize, Serialize};

// TODO: Explicit queries

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ConsoleManagerRequest {
    GetCoreConfig(CoreConfig),
    GetInstanceConfig(String),
    // QueryConfig(Query),
    // QueryState(Query),
    GetServerHealth(u64),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ConsoleManagerResponse {
    CoreConfig(CoreConfig),
    InstanceConfig(InstanceConfig),
    // ConfigQueryResult(QueryResult),
    // StateQueryResult(QueryResult),
    ServerHealth(ServerHealth),
}
