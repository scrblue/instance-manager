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

// `ManagerManagerRequest`s and `ManagerManagerResponse`s will contain `async_raft`'s requests such
// as to provide more derives

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ManagerManagerRequest {
    /// For when initiating a connection -- the SocketAddr is the public IP of the peer
    Greeting(SocketAddr),

    AppendEntries {
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<Entry<RaftRequest>>,
        leader_commit: u64,
    },
    InstallSnapshot {
        term: u64,
        leader_id: u64,
        last_included_index: u64,
        last_included_term: u64,
        offset: u64,
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
pub enum ManagerManagerResponse {
    ConnectionAccepted,
    ConnectionDenied,

    AppendEntriesResponse {
        term: u64,
        success: bool,
        conflict_opt: Option<(u64, u64)>,
    },
    InstallSnapshotResponse(u64),
    VoteResponse {
        term: u64,
        vote_granted: bool,
    },
}

impl From<AppendEntriesRequest<RaftRequest>> for ManagerManagerRequest {
    fn from(aer: AppendEntriesRequest<RaftRequest>) -> ManagerManagerRequest {
        ManagerManagerRequest::AppendEntries {
            term: aer.term,
            leader_id: aer.leader_id,
            prev_log_index: aer.prev_log_index,
            prev_log_term: aer.prev_log_term,
            entries: aer.entries,
            leader_commit: aer.leader_commit,
        }
    }
}
impl From<InstallSnapshotRequest> for ManagerManagerRequest {
    fn from(isr: InstallSnapshotRequest) -> ManagerManagerRequest {
        ManagerManagerRequest::InstallSnapshot {
            term: isr.term,
            leader_id: isr.leader_id,
            last_included_index: isr.last_included_index,
            last_included_term: isr.last_included_term,
            offset: isr.offset,
            data: isr.data,
            done: isr.done,
        }
    }
}
impl From<VoteRequest> for ManagerManagerRequest {
    fn from(vr: VoteRequest) -> ManagerManagerRequest {
        ManagerManagerRequest::RequestVote {
            term: vr.term,
            candidate_id: vr.candidate_id,
            last_log_index: vr.last_log_index,
            last_log_term: vr.last_log_term,
        }
    }
}

impl TryInto<AppendEntriesResponse> for ManagerManagerResponse {
    type Error = anyhow::Error;
    fn try_into(self) -> Result<AppendEntriesResponse, anyhow::Error> {
        if let ManagerManagerResponse::AppendEntriesResponse {
            term,
            success,
            conflict_opt,
        } = self
        {
            Ok(AppendEntriesResponse {
                term,
                success,
                conflict_opt: conflict_opt.map(|co| ConflictOpt {
                    term: co.0,
                    index: co.1,
                }),
            })
        } else {
            anyhow::bail!("ManagerManagerResponse is not an AppendEntriesResponse")
        }
    }
}
impl TryInto<InstallSnapshotResponse> for ManagerManagerResponse {
    type Error = anyhow::Error;
    fn try_into(self) -> Result<InstallSnapshotResponse, anyhow::Error> {
        if let ManagerManagerResponse::InstallSnapshotResponse(term) = self {
            Ok(InstallSnapshotResponse { term })
        } else {
            anyhow::bail!("ManagerManagerResponse is not an InstallSnapshotResponse")
        }
    }
}
impl TryInto<VoteResponse> for ManagerManagerResponse {
    type Error = anyhow::Error;
    fn try_into(self) -> Result<VoteResponse, anyhow::Error> {
        if let ManagerManagerResponse::VoteResponse { term, vote_granted } = self {
            Ok(VoteResponse { term, vote_granted })
        } else {
            anyhow::bail!("ManagerManagerResponse is not an VoteResponse")
        }
    }
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
