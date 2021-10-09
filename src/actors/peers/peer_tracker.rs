use super::peer_connection;
use crate::{
    configuration::manager::CoreConfig,
    messages::{
        InstanceManagerMessage, ManagerManagerRequest, ManagerManagerResponse, RaftRequest,
    },
    ImRaft,
};

use anyhow::{Context, Result};
use async_raft::{async_trait::async_trait, network::RaftNetwork, raft::*, NodeId};
use rand::prelude::*;
use std::{collections::HashMap, convert::TryInto, net::SocketAddr, sync::Arc};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
};
use tokio_tls::TlsConnection;

#[derive(Debug)]
pub enum PeerTrackerRequest {
    NewPeer {
        id: u64,
        socket_addr: SocketAddr,
        connection: TlsConnection<TcpStream>,
    },
    InitializeCluster {
        ids: Vec<u64>,
    },
    ManagerManagerRequest {
        target_id: u64,
        request: ManagerManagerRequest,
    },
}
use PeerTrackerRequest as Request;

use super::ConnectionToTracker as FromConnection;
use super::TrackerToConnection as ToConnection;

// TODO: Error handling

#[derive(Debug, Clone)]
pub struct PeerTrackerHandle {
    request_sender: mpsc::Sender<(Request, Option<oneshot::Sender<ManagerManagerResponse>>)>,
    sdr_sender: mpsc::Sender<()>,
}

impl PeerTrackerHandle {
    pub async fn shutdown(&self) -> Result<()> {
        self.sdr_sender.send(()).await.unwrap();

        Ok(())
    }

    pub async fn add_peer(
        &self,
        id: u64,
        public_addr: SocketAddr,
        connection: TlsConnection<TcpStream>,
    ) -> Result<()> {
        let (resp_send, resp_recv) = oneshot::channel::<ManagerManagerResponse>();
        self.request_sender
            .send((
                Request::NewPeer {
                    id,
                    socket_addr: public_addr,
                    connection,
                },
                Some(resp_send),
            ))
            .await.unwrap();

        let _ = resp_recv.await.unwrap();

        Ok(())
    }

    pub async fn initialize(&self, ids: Vec<u64>) -> Result<()> {
        self.request_sender
            .send((Request::InitializeCluster { ids }, None))
            .await.unwrap();

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
                Request::ManagerManagerRequest {
                    target_id: target,
                    request: rpc.into(),
                },
                Some(resp_send),
            ))
            .await.unwrap();

        resp_recv
            .await
            .context("Error receiving response for AppendEntriesRequest")
            .map(|resp| Ok(resp.try_into().unwrap())).unwrap()
    }

    async fn install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        let (resp_send, resp_recv) = oneshot::channel::<ManagerManagerResponse>();
        self.request_sender
            .send((
                Request::ManagerManagerRequest {
                    target_id: target,
                    request: rpc.into(),
                },
                Some(resp_send),
            ))
            .await.unwrap();

        resp_recv
            .await
            .context("Error receiving response for InstallSnapshotRequest")
            .map(|resp| Ok(resp.try_into().unwrap())).unwrap()
    }

    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
        let (resp_send, resp_recv) = oneshot::channel::<ManagerManagerResponse>();
        self.request_sender
            .send((
                Request::ManagerManagerRequest {
                    target_id: target,
                    request: rpc.into(),
                },
                Some(resp_send),
            ))
            .await.unwrap();

        resp_recv
            .await
            .context("Error receiving response for InstallSnapshotRequest")
            .map(|resp| Ok(resp.try_into().unwrap())).unwrap()
    }
}

pub struct PeerTacker {
    // Used to make comparisons with incoming peers
    core_config: Arc<CoreConfig>,

    // Used to interface with the Raft cluster logic
    raft: Option<Arc<ImRaft>>,
    self_raft_id: u64,

    // Whether the request to initialize the Raft formation has been received -- if Some, it will
    // only actually execute after all the peers are confirmed
    init_request_received: Option<Vec<u64>>,

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
    pub fn new(core_config: Arc<CoreConfig>, self_raft_id: u64) -> PeerTacker {
        let raft = None;
        let init_request_received = None;
        let (request_sender, request_receiver) = mpsc::channel(128);
        let (sdr_sender, shutdown_request_receiver) = mpsc::channel(1);
        let peer_handles = HashMap::new();
        let unconfirmed_peer_handles = HashMap::new();
        let (to_tracker_master, from_peers) = mpsc::channel(128);

        PeerTacker {
            core_config,
            raft,
            self_raft_id,
            init_request_received,
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
        self.raft = Some(raft);
        loop {
            tokio::select! {
                // Upon receiving a shutdown request, forward the request to every connection
                // tracked, then break from the loop
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

                // Upon receiving a request from a PeerTrackerHandle
                msg = self.request_receiver.recv() => {
                    if let Some(msg) = msg {
                        self.handle_peer_handle_request(msg).await.unwrap();
                    } else {
                        tracing::error!("Main thread channel closed");
                        anyhow::bail!("Main thread channel to peer tracker closed");
                    }
                },

                // Upon receiving a message from a Peer connection
                msg = self.from_peers.recv() => {
                    if let Some(msg) = msg {
                        self.handle_peer_message(msg).await.unwrap();
                    } else {
                        tracing::error!("All connection channels closed");
                        anyhow::bail!("All connection channels to peer tracker closed");
                    }

                },

                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {},
            }

            // If the init request has been received, then check if all IDs given have been
            // confirmed -- if they have been initialize the Raft cluster, then set the init
            // request to None to prevent initialization from occuring multiple times

            // TODO: Some kind of error if any of the `CoreConfig`s don't match
            let mut nullify_init_request = false;
            if let Some(ids) = &self.init_request_received {
                let mut all_ids_in = true;

                for id in ids {
                    if !self.peer_handles.contains_key(id) && id != &self.self_raft_id {
                        all_ids_in = false;
                    }
                }

                if all_ids_in {
                    nullify_init_request = true;
                    tracing::info!("Initializing cluster");
                    let _ = self.raft
                        .as_ref()
                        .unwrap()
                        .initialize(ids.iter().map(|e| *e).collect())
                        .await;
                }
            }
            if nullify_init_request {
                self.init_request_received = None;
            }
        }

        Ok(())
    }

    async fn handle_peer_handle_request(
        &mut self,
        msg: (Request, Option<oneshot::Sender<ManagerManagerResponse>>),
    ) -> Result<()> {
        tracing::trace!("Received request from handle: {:?}", msg.0);
        match msg {
            (
                Request::NewPeer {
                    id,
                    socket_addr,
                    connection,
                },
                tx,
            ) => {
                let (sender, receiver) = mpsc::channel::<ToConnection>(128);

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

                if let Some((_peer_handle, sender)) = self.unconfirmed_peer_handles.get(&id) {
                    let id = rand::thread_rng().gen::<u64>();
                    let mmr = InstanceManagerMessage {
                        id,
                        payload: ManagerManagerRequest::CompareCoreConfig(
                            self.core_config.as_ref().clone(),
                        ),
                    };
                    sender.send(ToConnection::Request(mmr, tx.unwrap())).await.unwrap();
                }
            }

            (Request::InitializeCluster { ids }, _no_tx) => {
                self.init_request_received = Some(ids);
            }

            (Request::ManagerManagerRequest { target_id, request }, tx) => {
                if let Some((_join_handle, sender)) = self.peer_handles.get(&target_id) {
                    let id = rand::thread_rng().gen::<u64>();
                    let mmr = InstanceManagerMessage {
                        id,
                        payload: request,
                    };

                    sender.send(ToConnection::Request(mmr, tx.unwrap())).await.unwrap();
                }
            }
        }

        Ok(())
    }

    async fn handle_peer_message(&mut self, msg: (u64, FromConnection)) -> Result<()> {
        tracing::trace!("Message from connection: {:?}", msg);
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
                self.handle_compare_core_config(peer_id, msg_id, cc).await.unwrap();
            }

            // When given a Raft related request, forward it to the Raft object, await a response,
            // and send it back to the peer
            // TODO: Less tuples, more structs
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
            ) => {
                let resp = self
                    .raft
                    .as_ref()
                    .unwrap()
                    .append_entries(AppendEntriesRequest {
                        term,
                        leader_id,
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit,
                    })
                    .await.unwrap();

                // TODO: Some kind of error if the peer_id isn't in the peer_handles
                if let Some((_join_handle, sender)) = self.peer_handles.get(&peer_id) {
                    sender
                        .send(ToConnection::Response(InstanceManagerMessage {
                            id: msg_id,
                            payload: ManagerManagerResponse::AppendEntriesResponse {
                                term: resp.term,
                                success: resp.success,
                                conflict_opt: resp.conflict_opt.map(|co| ((co.term, co.index))),
                            },
                        }))
                        .await.unwrap();
                }
            }
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
            ) => {
                let resp = self
                    .raft
                    .as_ref()
                    .unwrap()
                    .install_snapshot(InstallSnapshotRequest {
                        term,
                        leader_id,
                        last_included_index,
                        last_included_term,
                        offset,
                        data,
                        done,
                    })
                    .await.unwrap();

                if let Some((_join_handle, sender)) = self.peer_handles.get(&peer_id) {
                    sender
                        .send(ToConnection::Response(InstanceManagerMessage {
                            id: msg_id,
                            payload: ManagerManagerResponse::InstallSnapshotResponse(resp.term),
                        }))
                        .await.unwrap();
                }
            }
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
            ) => {
                let VoteResponse { term, vote_granted } = self
                    .raft
                    .as_ref()
                    .unwrap()
                    .vote(VoteRequest {
                        term,
                        candidate_id,
                        last_log_index,
                        last_log_term,
                    })
                    .await.unwrap();

                if let Some((_join_handle, sender)) = self.peer_handles.get(&peer_id) {
                    sender
                        .send(ToConnection::Response(InstanceManagerMessage {
                            id: msg_id,
                            payload: ManagerManagerResponse::VoteResponse { term, vote_granted },
                        }))
                        .await.unwrap();
                }
            }
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
                sender.send(ToConnection::CoreConfigMatch(msg_id)).await.unwrap();
                tracing::debug!("CoreConfig matches");
            } else {
                sender.send(ToConnection::CoreConfigNoMatch(msg_id)).await.unwrap();
                tracing::error!("CoreConfig doesn't match");
            }
        }

        if delete_from_unconfirmed {
            let (join_handle, sender) = self.unconfirmed_peer_handles.remove(&peer_id).unwrap();
            self.peer_handles.insert(peer_id, (join_handle, sender));
        }

        Ok(())
    }
}
