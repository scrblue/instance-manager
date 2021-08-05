use super::peer_connection;
use crate::messages::ManagerManagerRequest;

use anyhow::Result;
use indradb::{Datastore, Transaction};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::{net::TcpStream, sync::mpsc};
use tokio_tls::TlsConnection;

#[derive(Clone, Debug, PartialEq)]
pub enum PeerTrackerToMain {
    PeerNotification(ManagerManagerRequest),
}
use PeerTrackerToMain as ToMain;

#[derive(Debug)]
pub enum MainToPeerTracker {
    NewPeer(usize, SocketAddr, TlsConnection<TcpStream>),
    Shutdown,
}
use MainToPeerTracker as FromMain;

use super::ConnectionToTracker as FromConnection;
use super::TrackerToConnection as ToConnection;

#[tracing::instrument(skip(to_main, from_main, state_handle))]
pub async fn track_peers(
    to_main: mpsc::Sender<ToMain>,
    mut from_main: mpsc::Receiver<FromMain>,
    state_handle: Arc<indradb::MemoryDatastore>,
) -> Result<()> {
    let (to_tracker, mut from_connections) = mpsc::channel::<FromConnection>(128);

    let mut peer_handles: HashMap<
        usize,
        (tokio::task::JoinHandle<()>, mpsc::Sender<ToConnection>),
    > = HashMap::new();

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

                        match state_handle.transaction() {
                            Ok(transaction) => {
                                let existing_instance_managers = transaction.get_vertices(
                                    indradb::RangeVertexQuery::new().t(indradb::Type::new(
                                        "InstanceManager"
                                    ).unwrap())).unwrap();

                                let existing_instance_manager_ids = transaction.get_vertex_properties(
                                    indradb::VertexPropertyQuery::new(
                                        indradb::SpecificVertexQuery::new(
                                            existing_instance_managers.iter().map(|im| im.id).collect()
                                        ).into(),
                                        "instance_id",
                                    )
                                ).unwrap();

								// Skip adding this InstanceManager if it already appears in the graph
								if existing_instance_manager_ids.iter().find(|&existing_id| {
									existing_id.value == serde_json::Value::Number(
    									serde_json::Number::from_f64(id as f64).unwrap()
									)
								}).is_some() {
									continue;
								}

                                let new_instance_manager = indradb::Vertex::new(
                                     indradb::Type::new("InstanceManager"
                                ).unwrap());
                                let _ = transaction.create_vertex(&new_instance_manager);
                                let _ = transaction.set_vertex_properties(
                                    indradb::VertexPropertyQuery::new(
                                        indradb::SpecificVertexQuery::single(
                                              new_instance_manager.id).into(),
                                              "instance_id"),
                                          &serde_json::Value::Number(
                                              serde_json::Number::from_f64(id as f64).unwrap()
                                      ));

                                let _ = transaction.set_vertex_properties(
                                    indradb::VertexPropertyQuery::new(
                                        indradb::SpecificVertexQuery::single(
                                              new_instance_manager.id).into(),
                                              "socket_addr"),
                                          &serde_json::Value::String(
                                              format!("{}", socket_addr)
                                          )
                                      );

                                for existing_instance_manager in existing_instance_managers {
                                    let key = indradb::EdgeKey::new(existing_instance_manager.id, indradb::Type::new("peers_with").unwrap(), new_instance_manager.id);
                                    let key_rev = key.reversed();

                                    let _ = transaction.create_edge(&key);
                                    let _ = transaction.create_edge(&key_rev);
                                }

                            },

                            Err(e) => {
                                tracing::error!("Error creating transaction from state handle: {}", e);
                                break;
                            },
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
