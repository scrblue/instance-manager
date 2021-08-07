use crate::messages::{ManagerManagerRequest, ManagerManagerResponse, RaftRequest};

use anyhow::{Context, Result};
use async_raft::{async_trait::async_trait, network::RaftNetwork, raft::*, NodeId};
use std::{
    convert::TryInto,
    sync::{Arc, Mutex},
};
use tokio::sync::{mpsc, oneshot};

pub mod peer_connection;
pub mod peer_tracker;

#[derive(Debug)]
pub enum ConnectionToTracker {
    Request(ManagerManagerRequest),
}

#[derive(Debug)]
pub enum TrackerToConnection {
    Shutdown,
}

pub struct PeerTracker {
    pub to_tracker: mpsc::Sender<peer_tracker::MainToPeerTracker>,
    pub from_tracker: Arc<Mutex<mpsc::Receiver<peer_tracker::PeerTrackerToMain>>>,
}

#[async_trait]
impl RaftNetwork<RaftRequest> for PeerTracker {
    async fn append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<RaftRequest>,
    ) -> Result<AppendEntriesResponse> {
        let (resp_send, resp_recv) = oneshot::channel::<ManagerManagerResponse>();
        self.to_tracker
            .send(peer_tracker::MainToPeerTracker::ManagerManagerRequest(
                target,
                rpc.into(),
                resp_send,
            ))
            .await?;

        resp_recv
            .await
            .context("Error receiving response for AppendEntriesRequest")
            .map(TryInto::try_into)?
    }

    async fn install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        let (resp_send, resp_recv) = oneshot::channel::<ManagerManagerResponse>();
        self.to_tracker
            .send(peer_tracker::MainToPeerTracker::ManagerManagerRequest(
                target,
                rpc.into(),
                resp_send,
            ))
            .await?;

        resp_recv
            .await
            .context("Error receiving response for InstallSnapshotRequest")
            .map(TryInto::try_into)?
    }

    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
        let (resp_send, resp_recv) = oneshot::channel::<ManagerManagerResponse>();
        self.to_tracker
            .send(peer_tracker::MainToPeerTracker::ManagerManagerRequest(
                target,
                rpc.into(),
                resp_send,
            ))
            .await?;

        resp_recv
            .await
            .context("Error receiving response for VoteResponse")
            .map(TryInto::try_into)?
    }
}
