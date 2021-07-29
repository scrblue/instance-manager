use crate::messages::*;

use anyhow::{Context, Result};
use futures::stream::{futures_unordered::FuturesUnordered, StreamExt};
use rand::{distributions::Uniform, seq::SliceRandom, thread_rng, Rng};
use rustls::{ClientConfig, ServerConfig};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_tls::TlsConnection;

#[derive(Debug)]
pub enum ConnectionManagerToMain {
    ConnectedPeer(usize, TlsConnection<TcpStream>),
    ConnectedInstance(SocketAddr),
    FailedToConnect(SocketAddr),
}
use ConnectionManagerToMain as ToMain;

#[derive(Clone, Debug, Copy, PartialEq)]
pub enum MainToConnectionManager {
    FormConnectionWith(SocketAddr),
    Shutdown,
}
use MainToConnectionManager as FromMain;

enum LoopEnd {
    Break,
    Continue,
}

#[tracing::instrument(skip(to_main, from_main, server_config, client_config))]
pub async fn handle_connections(
    to_main: mpsc::Sender<ToMain>,
    mut from_main: mpsc::Receiver<FromMain>,
    listener_socket_addr: SocketAddr,
    server_config: Arc<ServerConfig>,
    client_config: Arc<ClientConfig>,
    initial_connections: Vec<SocketAddr>,
) -> Result<()> {
    // Start listening
    let tcp_lisenter = TcpListener::bind(listener_socket_addr).await?;
    let acceptor = TlsAcceptor::from(server_config);

    // Add initial_connections to randomized connection queue
    let mut form_connections_with = initial_connections;

    // The connections that have yet to be sent to a tracker -- waiting on an initial message
    let mut unmanaged_connections = FuturesUnordered::new();

    // Main loop
    loop {
        match (
            // If there are no agents to form connections with, don't include the randomized timer
            // in the tokio::select!
            form_connections_with.is_empty(),
            // If there are no unmanaged_connections, don't poll from that FuturesUnordered
            unmanaged_connections.is_empty(),
        ) {
            (false, false) => {
                let wait_interval = Uniform::new_inclusive(150u64, 500u64);
                tokio::select! {
                    msg = from_main.recv() => {
                        match handle_from_main(msg).await {
                            LoopEnd::Continue => continue,
                            LoopEnd::Break => break,
                        }
                    },

                    result = unmanaged_connections.next() => {
                        handle_unmanaged_connection(result, &mut form_connections_with, &to_main).await;
                    }

                    connection = tcp_lisenter.accept() => {
                        match handle_connection(acceptor.clone(), connection,).await {
                            Ok(stream) => unmanaged_connections.push(stream.read_message_and_take_self()),
                            Err(e) => tracing::error!("{}", e),
                        };
                    },

                    _ = tokio::time::sleep(Duration::from_millis(thread_rng().sample(wait_interval))) => {
                       if let Err(e) = initiate_connection(
                           &mut form_connections_with,
                           client_config.clone(),
                           &to_main
                       ).await.context("Error initiating a connection") {
                            tracing::error!("{}", e);
                       }
                   }
                }
            }

            (false, true) => {
                let wait_interval = Uniform::new_inclusive(150u64, 500u64);
                tokio::select! {
                    msg = from_main.recv() => {
                        match handle_from_main(msg).await {
                            LoopEnd::Continue => continue,
                            LoopEnd::Break => break,
                        }
                    },

                    connection = tcp_lisenter.accept() => {
                        match handle_connection(acceptor.clone(), connection,).await {
                            Ok(stream) => unmanaged_connections.push(stream.read_message_and_take_self()),
                            Err(e) => tracing::error!("{}", e),
                        };
                    },

                    _ = tokio::time::sleep(Duration::from_millis(thread_rng().sample(wait_interval))) => {
                       if let Err(e) = initiate_connection(
                           &mut form_connections_with,
                           client_config.clone(),
                           &to_main
                       ).await.context("Error initiating a connection") {
                            tracing::error!("{}", e);
                       }
                   }
                }
            }
            (true, false) => {
                tokio::select! {
                    msg = from_main.recv() => {
                        match handle_from_main(msg).await {
                            LoopEnd::Continue => continue,
                            LoopEnd::Break => break,
                        }
                    },

                    result = unmanaged_connections.next() => {
                        handle_unmanaged_connection(result,&mut form_connections_with,&to_main).await
                    }

                    connection = tcp_lisenter.accept() => {
                        match handle_connection(acceptor.clone(), connection,).await {
                            Ok(stream) => unmanaged_connections.push(stream.read_message_and_take_self()),
                            Err(e) => tracing::error!("{}", e),
                        };
                        continue;
                    },
                }
            }
            (true, true) => {
                tokio::select! {
                    msg = from_main.recv() => {
                        match handle_from_main(msg).await {
                            LoopEnd::Continue => continue,
                            LoopEnd::Break => break,
                        }
                    },

                    connection = tcp_lisenter.accept() => {
                        match handle_connection(acceptor.clone(), connection,).await {
                            Ok(stream) => unmanaged_connections.push(stream.read_message_and_take_self()),
                            Err(e) => tracing::error!("{}", e),
                        };
                        continue;
                    },
                }
            }
        };
    }

    Ok(())
}

#[tracing::instrument(skip(client_config, to_main))]
async fn initiate_connection(
    form_connections_with: &mut Vec<SocketAddr>,
    client_config: Arc<ClientConfig>,
    to_main: &mpsc::Sender<ToMain>,
) -> Result<()> {
    let addr = *form_connections_with
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

        let tls_connector = TlsConnector::from(client_config.clone());
        let stream = match tls_connector
            .connect(
                // FIXME: Don't hardcode
                webpki::DNSNameRef::try_from_ascii_str("root.pp.lyne.dev")?,
                stream,
            )
            .await
        {
            Ok(stream) => stream,
            Err(e) => {
                tracing::error!("Error forming TLS connectionto peer {}: {}", addr, e);
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        let stream = TlsConnection::new(stream);

        // TODO: Negotiate ID
        to_main.send(ToMain::ConnectedPeer(0, stream)).await?;
        tracing::info!("Connected to peer with socket address {}", addr);
        connected = true;
        break;
    }

    let index = form_connections_with
        .iter()
        .position(|&element| element == addr);
    if let Some(index) = index {
        let _ = form_connections_with.remove(index);
    }

    if !connected {
        to_main.send(ToMain::FailedToConnect(addr)).await?;
    }

    Ok(())
}

#[tracing::instrument(skip(acceptor))]
async fn handle_connection(
    acceptor: TlsAcceptor,
    connection: std::io::Result<(TcpStream, SocketAddr)>,
) -> Result<TlsConnection<TcpStream>> {
    let (stream, socket) = connection.context("Error handling incoming connection")?;
    tracing::info!("Connection from {}", socket);

    let stream = acceptor
        .accept(stream)
        .await
        .context("Error forming TLS connection")?;

    Ok(TlsConnection::new(stream))
}

#[tracing::instrument]
async fn handle_from_main(from_main: Option<FromMain>) -> LoopEnd {
    match from_main {
        Some(FromMain::Shutdown) => {
            tracing::debug!("Connection manager shutting down gracefully");
            LoopEnd::Break
        }

        None => {
            tracing::error!("Main thread channel closed");
            LoopEnd::Break
        }

        msg => {
            tracing::error!("Unimplemented message: {:?}", msg);
            LoopEnd::Continue
        }
    }
}

#[tracing::instrument]
async fn handle_unmanaged_connection(
    msg: Option<Result<(IncomingMessage, TlsConnection<TcpStream>)>>,
    form_connections_with: &mut Vec<SocketAddr>,
    to_main: &mpsc::Sender<ToMain>,
) {
    match msg {
        Some(Ok((
            IncomingMessage::ManagerManagerRequest(ManagerManagerRequest::Greeting(addr)),
            connection,
        ))) => {
            tracing::info!(
                "Received connection from client peer who listens on {}",
                addr
            );
            if let Err(e) = to_main.send(ToMain::ConnectedPeer(0, connection)).await {
                tracing::error!("Error while notifying main of ConnectedPeer");
            };

            let index = form_connections_with
                .iter()
                .position(|&element| element == addr);
            if let Some(index) = index {
                let _ = form_connections_with.remove(index);
            }
        }

        Some(Err(e)) => {
            tracing::error!("{}", e);
        }

        _ => {}
    };
}
