use super::peer_connection;
use crate::{
    configuration::CoreConfig,
    messages::{
        InstanceManagerMessage, ManagerManagerRequest, ManagerManagerResponse, RaftRequest,
    },
    ImRaft,
};

use anyhow::{Context, Result};
use async_raft::{async_trait::async_trait, network::RaftNetwork, raft::*, NodeId};
use futures::stream::{futures_unordered::FuturesUnordered, StreamExt};
use rand::prelude::*;
use std::{collections::HashMap, convert::TryInto, net::SocketAddr, sync::Arc};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
};
use tokio_tls::TlsConnection;

#[derive(Clone, Debug, PartialEq)]
pub enum PeerTrackerResponse {
    Ok,
    ManagerManagerResponse(ManagerManagerResponse),
}
use PeerTrackerResponse as Response;

#[derive(Debug)]
pub enum PeerTrackerRequest {
    NewPeer(u64, SocketAddr, TlsConnection<TcpStream>),
    ManagerManagerRequest(u64, ManagerManagerRequest),
}
use PeerTrackerRequest as Request;

use super::ConnectionToTracker as FromConnection;
use super::TrackerToConnection as ToConnection;

// TODO: Error handling
// TODO: Compare match Request model to separate oneshot senders and receivers per request

#[derive(Debug, Clone)]
pub struct PeerTrackerHandle {
    request_sender: mpsc::Sender<(Request, Option<oneshot::Sender<ManagerManagerResponse>>)>,
    sdr_sender: mpsc::Sender<()>,
}

impl PeerTrackerHandle {
    pub async fn shutdown(&self) -> Result<()> {
        self.sdr_sender.send(()).await?;

        Ok(())
    }

    pub async fn add_peer(
        &self,
        id: u64,
        public_addr: SocketAddr,
        connection: TlsConnection<TcpStream>,
    ) -> Result<()> {
        self.request_sender
            .send((Request::NewPeer(id, public_addr, connection), None))
            .await?;

        Ok(())
    }
}

#[async_trait]
impl RaftNetwork<RaftRequest> for PeerTrackerHandle {
    async fn append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<RaftRequest>,
    ) -> Result<AppendEntriesResponse> {
        let (resp_send, resp_recv) = oneshot::channel::<ManagerManagerResponse>();
        self.request_sender
            .send((
                Request::ManagerManagerRequest(target, rpc.into()),
                Some(resp_send),
            ))
            .await?;

        resp_recv
            .await
            .context("Error receiving response for AppendEntriesRequest")
            .map(|resp| Ok(resp.try_into()?))?
    }

    async fn install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        let (resp_send, resp_recv) = oneshot::channel::<ManagerManagerResponse>();
        self.request_sender
            .send((
                Request::ManagerManagerRequest(target, rpc.into()),
                Some(resp_send),
            ))
            .await?;

        resp_recv
            .await
            .context("Error receiving response for InstallSnapshotRequest")
            .map(|resp| Ok(resp.try_into()?))?
    }

    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
        let (resp_send, resp_recv) = oneshot::channel::<ManagerManagerResponse>();
        self.request_sender
            .send((
                Request::ManagerManagerRequest(target, rpc.into()),
                Some(resp_send),
            ))
            .await?;

        resp_recv
            .await
            .context("Error receiving response for InstallSnapshotRequest")
            .map(|resp| Ok(resp.try_into()?))?
    }
}

pub struct PeerTacker {
    // Used to make comparisons with incoming peers
    core_config: Arc<CoreConfig>,

    // Requests to the PeerTracker
    request_receiver: mpsc::Receiver<(Request, Option<oneshot::Sender<ManagerManagerResponse>>)>,
    shutdown_request_receiver: mpsc::Receiver<()>,

    // Peer connections message Senders and JoinHandles
    peer_handles: HashMap<u64, (tokio::task::JoinHandle<()>, mpsc::Sender<ToConnection>)>,
    unconfirmed_peer_handles:
        HashMap<u64, (tokio::task::JoinHandle<()>, mpsc::Sender<ToConnection>)>,

    // Master peer connection sender and receiver
    to_tracker_master: mpsc::Sender<(u64, FromConnection)>,
    from_peers: mpsc::Receiver<(u64, FromConnection)>,

    // To be cloned when spawning a PeerTrackerHandle
    request_sender: mpsc::Sender<(Request, Option<oneshot::Sender<ManagerManagerResponse>>)>,
    sdr_sender: mpsc::Sender<()>,
}

impl PeerTacker {
    #[tracing::instrument]
    pub fn new(core_config: Arc<CoreConfig>) -> PeerTacker {
        let (request_sender, request_receiver) = mpsc::channel(128);
        let (sdr_sender, shutdown_request_receiver) = mpsc::channel(1);
        let peer_handles = HashMap::new();
        let unconfirmed_peer_handles = HashMap::new();
        let (to_tracker_master, from_peers) = mpsc::channel(128);

        PeerTacker {
            core_config,
            request_receiver,
            shutdown_request_receiver,
            peer_handles,
            unconfirmed_peer_handles,
            to_tracker_master,
            from_peers,
            request_sender,
            sdr_sender,
        }
    }

    pub fn handle(&self) -> PeerTrackerHandle {
        PeerTrackerHandle {
            request_sender: self.request_sender.clone(),
            sdr_sender: self.sdr_sender.clone(),
        }
    }

    #[tracing::instrument(skip(self, raft))]
    pub async fn run(mut self, raft: Arc<ImRaft>) -> Result<()> {
        loop {
            tokio::select! {
                msg = self.shutdown_request_receiver.recv() => {
                    if let Some(msg) = msg {
                        tracing::debug!("Shutting down peer tracker gracefully");
                        for (id, (join_handle, sender)) in self.peer_handles.drain() {
                            sender.send(ToConnection::Shutdown).await.unwrap();
                            if let Err(e) = join_handle.await {
                                tracing::error!("Error shutting down peer session ID {}: {}", id, e);
                            };
                        }

                        break;
                    }
                },

                msg = self.request_receiver.recv() => {
                    if let Some(msg) = msg {
                        self.handle_peer_handle_request(msg).await?;
                    } else {
                        tracing::error!("Main thread channel closed");
                        anyhow::bail!("Main thread channel to peer tracker closed");
                    }
                },

                msg = self.from_peers.recv() => {
                    if let Some(msg) = msg {
                        self.handle_peer_message(msg).await?;
                    } else {
                        tracing::error!("All connection channels closed");
                        anyhow::bail!("All connection channels to peer tracker closed");
                    }

                },
            }
        }

        Ok(())
    }

    async fn handle_peer_handle_request(
        &mut self,
        msg: (Request, Option<oneshot::Sender<ManagerManagerResponse>>),
    ) -> Result<()> {
        match msg {
            (Request::NewPeer(id, socket_addr, mut connection), tx) => {
                let (sender, receiver) = mpsc::channel::<ToConnection>(128);

                connection
                    .send_message(&ManagerManagerRequest::CompareCoreConfig(
                        self.core_config.as_ref().clone(),
                    ))
                    .await?;

                self.unconfirmed_peer_handles.insert(
                    id,
                    (
                        tokio::spawn(peer_connection::handle_peer_connection(
                            self.to_tracker_master.clone(),
                            receiver,
                            connection,
                            id,
                            None,
                        )),
                        sender,
                    ),
                );
            }

            (Request::ManagerManagerRequest(id, mmr), tx) => {
                if let Some((_join_handle, sender)) = self.peer_handles.get(&id) {
                    let id = rand::thread_rng().gen::<u64>();
                    let mmr = InstanceManagerMessage { id, payload: mmr };

                    sender.send(ToConnection::Request(mmr, tx.unwrap())).await?;
                }
            }
        }

        Ok(())
    }

    async fn handle_peer_message(&mut self, msg: (u64, FromConnection)) -> Result<()> {
        tracing::info!("Message from connection: {:?}", msg);
        match msg {
            // When given a CompareCoreConfig request, first fetch the peer
            // handle from the map of unconfirmed_peer_handles, compare the
            // configurations, send either a ToConnection::CoreConfigMatch or
            // CoreConfigNoMatch in response, and if it matches, delete the
            // handle from the unconfirmed_peer_handles and add it to the
            // peer_handles
            (
                peer_id,
                FromConnection::Request(InstanceManagerMessage {
                    id: msg_id,
                    payload: ManagerManagerRequest::CompareCoreConfig(cc),
                }),
            ) => {
                self.handle_compare_core_config(peer_id, msg_id, cc).await?;
            }
            (
                peer_id,
                FromConnection::Request(InstanceManagerMessage {
                    id: msg_id,
                    payload:
                        ManagerManagerRequest::AppendEntries {
                            term,
                            leader_id,
                            prev_log_index,
                            prev_log_term,
                            entries,
                            leader_commit,
                        },
                }),
            ) => {}
            (
                peer_id,
                FromConnection::Request(InstanceManagerMessage {
                    id: msg_id,
                    payload:
                        ManagerManagerRequest::InstallSnapshot {
                            term,
                            leader_id,
                            last_included_index,
                            last_included_term,
                            offset,
                            data,
                            done,
                        },
                }),
            ) => {}
            (
                peer_id,
                FromConnection::Request(InstanceManagerMessage {
                    id: msg_id,
                    payload:
                        ManagerManagerRequest::RequestVote {
                            term,
                            candidate_id,
                            last_log_index,
                            last_log_term,
                        },
                }),
            ) => {}

            (
                peer_id,
                FromConnection::Response(InstanceManagerMessage {
                    id: msg_id,
                    payload: ManagerManagerResponse::RequireCoreConfig,
                }),
            ) => {}
            (
                peer_id,
                FromConnection::Response(InstanceManagerMessage {
                    id: msg_id,
                    payload: ManagerManagerResponse::ConnectionAccepted,
                }),
            ) => {}
            (
                peer_id,
                FromConnection::Response(InstanceManagerMessage {
                    id: msg_id,
                    payload: ManagerManagerResponse::ConnectionDenied,
                }),
            ) => {}
            (
                peer_id,
                FromConnection::Response(InstanceManagerMessage {
                    id: msg_id,
                    payload:
                        ManagerManagerResponse::AppendEntriesResponse {
                            term,
                            success,
                            conflict_opt,
                        },
                }),
            ) => {}
            (
                peer_id,
                FromConnection::Response(InstanceManagerMessage {
                    id: msg_id,
                    payload: ManagerManagerResponse::InstallSnapshotResponse(peer_snapshot_id),
                }),
            ) => {}
            (
                peer_id,
                FromConnection::Response(InstanceManagerMessage {
                    id: msg_id,
                    payload: ManagerManagerResponse::VoteResponse { term, vote_granted },
                }),
            ) => {}
        }

        Ok(())
    }

    async fn handle_compare_core_config(
        &mut self,
        peer_id: u64,
        msg_id: u64,
        cc: CoreConfig,
    ) -> Result<()> {
        let mut delete_from_unconfirmed = false;
        if let Some((_join_handle, sender)) = self.unconfirmed_peer_handles.get(&peer_id) {
            if &cc == self.core_config.as_ref() {
                delete_from_unconfirmed = true;
                sender.send(ToConnection::CoreConfigMatch(msg_id)).await?;
            } else {
                sender.send(ToConnection::CoreConfigNoMatch(msg_id)).await?;
            }
        }

        if delete_from_unconfirmed {
            let (join_handle, sender) = self.unconfirmed_peer_handles.remove(&peer_id).unwrap();
            self.peer_handles.insert(peer_id, (join_handle, sender));
        }

        Ok(())
    }
}
