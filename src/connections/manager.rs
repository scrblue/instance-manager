use crate::{messages::*, peers::peer_tracker::PeerTrackerHandle, ImRaft};

use anyhow::{Context, Result};
use futures::{
    stream::{futures_unordered::FuturesUnordered, StreamExt},
    Future,
};
use rand::{distributions::Uniform, seq::SliceRandom, thread_rng, Rng};
use rustls::{ClientConfig, ServerConfig};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_tls::TlsConnection;

#[derive(Debug)]
pub enum Response {
    ConnectedPeer(u64, SocketAddr),
    FailedToConnect(SocketAddr),

    Ok,
}

#[derive(Clone, Debug, Copy, PartialEq)]
pub enum Request {
    FormConnectionWith(SocketAddr),
    Shutdown,
}

enum LoopEnd {
    Break,
    Continue,
}

/// Handles incoming connections and directs them to the PeerTracker or InstanceTracker or
/// ConsoleTracker depending on the initial message. Additionally initiates connections with all
/// configured peers with a randomized algorithm
pub struct ConnectionManager {
    client_config: Arc<ClientConfig>,
    tcp_listener: TcpListener,
    tls_acceptor: TlsAcceptor,
    listener_addr: SocketAddr,

    form_connections_with: Vec<(u64, SocketAddr)>,
    await_connections_from: Vec<(u64, SocketAddr)>,

    unmanaged_connections: FuturesUnordered<
        std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(Greeting, TlsConnection<TcpStream>)>>>,
        >,
    >,

    from_handle: mpsc::Receiver<(Request, oneshot::Sender<Response>)>,
    handle_sender_master: mpsc::Sender<(Request, oneshot::Sender<Response>)>,

    peer_tracker_handle: Option<PeerTrackerHandle>,
    // TODO: InstanceTrackerHandle and ConsoleTrackerHandles
}

impl ConnectionManager {
    pub async fn new(
        client_config: Arc<ClientConfig>,
        server_config: Arc<ServerConfig>,
        listener_addr: SocketAddr,
        initial_connections: Vec<(u64, SocketAddr)>,
    ) -> Result<ConnectionManager> {
        let tcp_listener = TcpListener::bind(listener_addr).await?;
        let tls_acceptor = TlsAcceptor::from(server_config);

        let form_connections_with = initial_connections;
        let await_connections_from = Vec::new();
        let unmanaged_connections = FuturesUnordered::new();

        let (handle_sender_master, from_handle) = mpsc::channel(32);

        let peer_tracker_handle = None;

        Ok(ConnectionManager {
            client_config,
            tcp_listener,
            tls_acceptor,
            listener_addr,
            form_connections_with,
            await_connections_from,
            unmanaged_connections,
            from_handle,
            handle_sender_master,
            peer_tracker_handle,
        })
    }

    #[tracing::instrument(skip(self, peer_tracker_handle))]
    pub async fn run(mut self, peer_tracker_handle: PeerTrackerHandle) -> Result<()> {
        self.peer_tracker_handle = Some(peer_tracker_handle);

        loop {
            let loop_result;
            match (
                // If there are no agents to form connections with, don't include the randomized
                // timer while `tokio::select!`ing
                self.form_connections_with.is_empty(),
                // Similarly, don't poll from the FuturesUnordered if it is empty
                self.unmanaged_connections.is_empty(),
            ) {
                (false, false) => {
                    let wait_interval = Uniform::new_inclusive(150u64, 500u64);
                    tokio::select! {
                        msg = self.from_handle.recv() => {
                            match self.handle_request(msg).await {
                                LoopEnd::Continue => continue,
                                LoopEnd::Break => break,
                            }
                        },

                        msg = self.unmanaged_connections.next() => {
                            loop_result = self.handle_unmanaged_connection(msg).await;
                        }

                        connection = self.tcp_listener.accept() => {
                            loop_result = self.handle_connection(connection).await;
                        }

                        _ = tokio::time::sleep(Duration::from_millis(thread_rng().sample(wait_interval))) => {
                            loop_result = self.initiate_connection().await;
                        }
                    }
                }
                (false, true) => {
                    let wait_interval = Uniform::new_inclusive(150u64, 500u64);
                    tokio::select! {
                        msg = self.from_handle.recv() => {
                            match self.handle_request(msg).await {
                                LoopEnd::Continue => continue,
                                LoopEnd::Break => break,
                            }
                        },

                        connection = self.tcp_listener.accept() => {
                            loop_result = self.handle_connection(connection).await;
                        }

                        _ = tokio::time::sleep(Duration::from_millis(thread_rng().sample(wait_interval))) => {
                            loop_result = self.initiate_connection().await;
                        }
                    }

                }
                (true, false) => {
                    tokio::select! {
                        msg = self.from_handle.recv() => {
                            match self.handle_request(msg).await {
                                LoopEnd::Continue => continue,
                                LoopEnd::Break => break,
                            }
                        },

                        msg = self.unmanaged_connections.next() => {
                            loop_result = self.handle_unmanaged_connection(msg).await;
                        }

                        connection = self.tcp_listener.accept() => {
                            loop_result = self.handle_connection(connection).await;
                        }

                    }
                }
                (true, true) => {
                    tokio::select! {
                        msg = self.from_handle.recv() => {
                            match self.handle_request(msg).await {
                                LoopEnd::Continue => continue,
                                LoopEnd::Break => break,
                            }
                        },

                        connection = self.tcp_listener.accept() => {
                            loop_result = self.handle_connection(connection).await;
                        }

                    }
                }
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn handle_request(
        &mut self,
        msg: Option<(Request, oneshot::Sender<Response>)>,
    ) -> LoopEnd {
        match msg {
            Some((Request::Shutdown, tx)) => {
                tx.send(Response::Ok);
                tracing::debug!("ConnectionManager shutting down gracefully");
                LoopEnd::Break
            }

            Some((unhandled, tx)) => {
                tracing::error!("Unimplemented request: {:?}", unhandled);
                LoopEnd::Continue
            }

            None => {
                tracing::error!("All requests senders dropped");
                LoopEnd::Break
            }
        }
    }

    async fn handle_unmanaged_connection(
        &mut self,
        msg: Option<Result<(Greeting, TlsConnection<TcpStream>)>>,
    ) -> Result<()> {
        match msg {
            Some(Ok((Greeting::Peer(addr), connection))) => {
                tracing::info!(
                    "Received connection from client peer who listens on {}",
                    addr
                );

                let mut id = None;
                let mut listener_addr = None;

                let mut index =
                    self.form_connections_with
                        .iter()
                        .position(|&(raft_id, element_addr)| {
                            if element_addr == addr {
                                id = Some(raft_id);
                                listener_addr = Some(element_addr);
                                true
                            } else {
                                false
                            }
                        });

                if index.is_none() {
                    index =
                        self.await_connections_from
                            .iter()
                            .position(|&(raft_id, element_addr)| {
                                if element_addr == addr {
                                    id = Some(raft_id);
                                    listener_addr = Some(element_addr);
                                    true
                                } else {
                                    false
                                }
                            });
                }

                if let Some(index) = index {
                    self.peer_tracker_handle
                        .as_ref()
                        .unwrap()
                        .add_peer(id.unwrap(), listener_addr.unwrap(), connection)
                        .await?;
                }
            }

            Some(Ok(other)) => {
                tracing::error!("Unimplemented request {:?}", other);
            }

            Some(Err(e)) => {
                tracing::error!("{}", e);
            }

            None => {}
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn handle_connection(
        &mut self,
        connection: std::io::Result<(TcpStream, SocketAddr)>,
    ) -> Result<()> {
        let (stream, socket) = connection.context("Error handling incoming connection")?;
        tracing::info!("Connection from {}", socket);

        let stream = TlsConnection::new(
            self.tls_acceptor
                .accept(stream)
                .await
                .context("Error forming TLS connection")?,
        );

        self.unmanaged_connections
            .push(Box::pin(stream.read_message_and_take_self()));

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn initiate_connection(&mut self) -> Result<()> {
        let (id, addr) = self
            .form_connections_with
            .choose(&mut rand::thread_rng())
            .unwrap();

        let mut connected = false;
        for _attempts in 0u8..3u8 {
            let stream = match TcpStream::connect(addr).await {
                Ok(stream) => stream,
                Err(e) => {
                    tracing::error!("Error connecting to peer {}: {}", addr, e);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };

            // TODO: Reuse TlsConnector
            let tls_connector = TlsConnector::from(self.client_config.clone());

            // FIXME: Don't hardcode DNS name
            let stream = match tls_connector
                .connect(
                    webpki::DNSNameRef::try_from_ascii_str("root.pp.lyne.dev")?,
                    stream,
                )
                .await
            {
                Ok(stream) => stream,
                Err(e) => {
                    tracing::error!("Error forming TLS connection to peer {}: {}", addr, e);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };

            let mut stream = TlsConnection::new(stream);

            stream
                .send_message(&Greeting::Peer(self.listener_addr))
                .await?;

            self.peer_tracker_handle
                .as_ref()
                .unwrap()
                .add_peer(*id, *addr, stream)
                .await?;

            tracing::info!("Connected to peer with socket address {}", addr);
            connected = true;
            break;
        }

        let index = self.form_connections_with
            .iter()
            .position(|&(raft_id, _)| raft_id == *id);
        if let Some(index) = index {
			let removed = self.form_connections_with.remove(index);

			if !connected {
				self.await_connections_from.push(removed);
			}
        }

        Ok(())
    }
}
