use async_raft::raft::*;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, convert::TryInto, net::SocketAddr};

use super::{console_network::*, instance_network::*, manager_network::*};
use crate::configuration::manager::CoreConfig;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ManagerManagerRequest {
    CompareCoreConfig(CoreConfig),

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
    RequireCoreConfig,
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ManagerManagerPayload {
    Request(ManagerManagerRequest),
    Response(ManagerManagerResponse),
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
pub enum RaftRequestPayload {
    ConsoleNetworkRequest(ConsoleNetworkRequest),
    InstanceNetworkRequest(InstanceNetworkRequest),
    ManagerNetworkRequest(ManagerNetworkRequest),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RaftRequest {
    pub client: uuid::Uuid,
    pub serial: u64,
    pub payload: RaftRequestPayload,
}
impl async_raft::AppData for RaftRequest {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RaftResponse {
    ConsoleNetworkResponse(ConsoleNetworkResponse),
    InstanceNetworkResponse(InstanceNetworkResponse),
    Ok,
}
impl async_raft::AppDataResponse for RaftResponse {}
