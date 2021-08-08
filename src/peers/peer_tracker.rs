use super::peer_connection;
use crate::{
    messages::{ManagerManagerRequest, ManagerManagerResponse},
    state_manager::StateHandle,
};

use anyhow::Result;
use async_raft::raft::AppendEntriesResponse;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
};
use tokio_tls::TlsConnection;

#[derive(Clone, Debug, PartialEq)]
pub enum PeerTrackerToMain {
    PeerNotification(ManagerManagerRequest),
}
use PeerTrackerToMain as ToMain;

#[derive(Debug)]
pub enum MainToPeerTracker {
    NewPeer(u64, SocketAddr, TlsConnection<TcpStream>),
    ManagerManagerRequest(
        u64,
        ManagerManagerRequest,
        oneshot::Sender<ManagerManagerResponse>,
    ),
    Shutdown,
}
use MainToPeerTracker as FromMain;

use super::ConnectionToTracker as FromConnection;
use super::TrackerToConnection as ToConnection;

// TODO: Error handling

#[tracing::instrument(skip(to_main, from_main, state_handle))]
pub async fn track_peers(
    to_main: mpsc::Sender<ToMain>,
    mut from_main: mpsc::Receiver<FromMain>,
    state_handle: StateHandle,
) -> Result<()> {
    let (to_tracker, mut from_connections) = mpsc::channel::<FromConnection>(128);

    let mut peer_handles: HashMap<u64, (tokio::task::JoinHandle<()>, mpsc::Sender<ToConnection>)> =
        HashMap::new();

    loop {
        tokio::select! {
            msg = from_main.recv() => {
                   match msg {
                    Some(FromMain::Shutdown) => {
                        tracing::debug!("Shutting down peer tracker gracefully");
                        for (id, (join_handle, sender)) in peer_handles.drain() {
                            sender.send(ToConnection::Shutdown).await.unwrap();
                            if let Err(e) = join_handle.await {
                                tracing::error!("Error shutting down peer session ID {}: {}", id, e);
                            };
                        }
                        break;
                    }

                    Some(FromMain::NewPeer(id, socket_addr, connection)) => {
                        let (sender, receiver) = mpsc::channel::<ToConnection>(128);

                        if let Err(e) = state_handle.add_peer(id, socket_addr).await {
                            tracing::error!("For NewPeer message with raft_id {}: {}", id, e);
                            continue
                        };

                        peer_handles.insert(id, (
                            tokio::spawn(peer_connection::handle_peer_connection(
                                to_tracker.clone(),
                                receiver,
                                connection,
                                id
                            )),
                            sender
                        ));
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

            msg = from_connections.recv() => {
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
