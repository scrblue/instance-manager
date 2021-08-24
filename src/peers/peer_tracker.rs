use super::peer_connection;
use crate::{
    messages::{ManagerManagerRequest, ManagerManagerResponse, RaftRequest},
    state::handle::StateHandle,
};

use anyhow::{Context, Result};
use async_raft::{async_trait::async_trait, network::RaftNetwork, raft::*, NodeId};
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
// TODO: Compare match Request model to separate senders and receivers per request

pub struct PeerTrackerHandle {
    request_sender: mpsc::Sender<(Request, oneshot::Sender<Response>)>,
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
        let (tx, rx) = oneshot::channel();
        self.request_sender
            .send((Request::NewPeer(id, public_addr, connection), tx))
            .await?;

        if let Ok(Response::Ok) = rx.await {
            Ok(())
        } else {
            anyhow::bail!("Error adding peer");
        }
    }
}

#[async_trait]
impl RaftNetwork<RaftRequest> for PeerTrackerHandle {
    async fn append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<RaftRequest>,
    ) -> Result<AppendEntriesResponse> {
        let (resp_send, resp_recv) = oneshot::channel::<Response>();
        self.request_sender
            .send((
                Request::ManagerManagerRequest(target, rpc.into()),
                resp_send,
            ))
            .await?;

        resp_recv
            .await
            .context("Error receiving response for AppendEntriesRequest")
            .map(|resp| {
                if let Response::ManagerManagerResponse(mmr) = resp {
                    Ok(mmr.try_into()?)
                } else {
                    anyhow::bail!("Wrong response type for Raft AppendEntriesRequest")
                }
            })?
    }

    async fn install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        let (resp_send, resp_recv) = oneshot::channel::<Response>();
        self.request_sender
            .send((
                Request::ManagerManagerRequest(target, rpc.into()),
                resp_send,
            ))
            .await?;

        resp_recv
            .await
            .context("Error receiving response for InstallSnapshotRequest")
            .map(|resp| {
                if let Response::ManagerManagerResponse(mmr) = resp {
                    Ok(mmr.try_into()?)
                } else {
                    anyhow::bail!("Wrong response type for Raft InstallSnapshotRequest")
                }
            })?
    }

    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
        let (resp_send, resp_recv) = oneshot::channel::<Response>();
        self.request_sender
            .send((
                Request::ManagerManagerRequest(target, rpc.into()),
                resp_send,
            ))
            .await?;

        resp_recv
            .await
            .context("Error receiving response for InstallSnapshotRequest")
            .map(|resp| {
                if let Response::ManagerManagerResponse(mmr) = resp {
                    Ok(mmr.try_into()?)
                } else {
                    anyhow::bail!("Wrong response type for Raft VoteRequest")
                }
            })?
    }
}

pub struct PeerTacker {
    // Requests to the PeerTracker
    request_receiver: mpsc::Receiver<(Request, oneshot::Sender<Response>)>,
    shutdown_request_receiver: mpsc::Receiver<()>,

    // The state handle is needed for inserting peers into to state
    state_handle: StateHandle,

    // TODO: raft_handle: RaftHandle,

    // Peer connections message Senders and JoinHandles
    peer_handles: HashMap<u64, (tokio::task::JoinHandle<()>, mpsc::Sender<ToConnection>)>,

    // Master peer connection sender and receiver
    to_tracker_master: mpsc::Sender<FromConnection>,
    from_peers: mpsc::Receiver<FromConnection>,

    // To be cloned when spawning a PeerTrackerHandle
    request_sender: mpsc::Sender<(Request, oneshot::Sender<Response>)>,
    sdr_sender: mpsc::Sender<()>,
}

impl PeerTacker {
    #[tracing::instrument(skip(state_handle))]
    pub fn new(state_handle: StateHandle) -> PeerTacker {
        let (request_sender, request_receiver) = mpsc::channel(128);
        let (sdr_sender, shutdown_request_receiver) = mpsc::channel(1);
        let peer_handles = HashMap::new();
        let (to_tracker_master, from_peers) = mpsc::channel(128);

        PeerTacker {
            request_receiver,
            shutdown_request_receiver,
            state_handle,
            peer_handles,
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

    #[tracing::instrument(skip(self))]
    pub async fn run(mut self) -> Result<()> {
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
                    match msg {
                        Some((Request::NewPeer(id, socket_addr, connection), tx)) => {
                            let (sender, receiver) = mpsc::channel::<ToConnection>(128);

                            if let Err(e) = self.state_handle.add_peer(id, socket_addr).await {
                                tracing::error!("For NewPeer message with raft_id {}: {}", id, e);
                                continue
                            };

                            self.peer_handles.insert(id, (
                                tokio::spawn(peer_connection::handle_peer_connection(
                                    self.to_tracker_master.clone(),
                                    receiver,
                                    connection,
                                    id
                                )),
                                sender
                            ));

                            // TODO: Notify RaftHandle

                            tx.send(Response::Ok);
                        }

                        Some(other) => {
                            // TODO: ManagerManagerRequest message handling
                            tracing::error!("Unimplemented request: {:?}", other);
                        }

                        None => {
                            tracing::error!("Main thread channel closed");
                            break;
                        }

                    }
                },

                msg = self.from_peers.recv() => {
                    match msg {
                        Some(msg) => {
                            tracing::info!("Message from connection: {:?}", msg);
                        }
                        None => {
                            tracing::error!("All connection channels closed");
                            break;
                        }
                    }
                },
            }
        }

        Ok(())
    }
}
