use anyhow::Result;
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

    // Main loop
    'outer: loop {
        if form_connections_with.is_empty() {
            tokio::select! {
                connection = tcp_lisenter.accept() => {
                     match connection {
                         Err(e) => {
                             tracing::error!("Failure accepting connection: {}", e);
                             continue;
                         },
                         Ok((stream, socket)) => {
                             tracing::info!("Connection from {}", socket);

                             let stream = match acceptor.accept(stream).await {
                                 Ok(stream) => stream,
                                 Err(e) => {
                                     tracing::error!("Error forming TLS stream with incoming connection on {}: {}", socket, e);
                                     continue;
                                 }
                             };

                             let stream = TlsConnection::new(stream);

                         }
                     }
                },
                msg = from_main.recv() => {
                    match msg {
                        Some(FromMain::Shutdown) => {
                            tracing::debug!("Connection manager shutting down gracefully");
                            break;
                        },

                        None => {
                            tracing::error!("Main thread channel closed");
                            break;
                        }

                        _ => {},
                    }
                },
            }
        } else {
            let wait_interval = Uniform::new_inclusive(75u64, 200u64);
            tokio::select! {
               connection = tcp_lisenter.accept() => {
                    match connection {
                        Err(e) => {
                            tracing::error!("Failure accepting connection: {}", e);
                            continue;
                        },
                        Ok((stream, socket)) => {
                            tracing::info!("Connection from {}", socket);

                            let stream = match acceptor.accept(stream).await {
                                Ok(stream) => stream,
                                Err(e) => {
                                    tracing::error!("Error forming TLS stream with incoming connection on {}: {}", socket, e);
                                    continue;
                                }
                            };

                            let stream = TlsConnection::new(stream);

                        }
                    }
               },

               msg = from_main.recv() => {
                   match msg {
                       Some(FromMain::Shutdown) => {
                           tracing::debug!("Connection manager shutting down gracefully");
                           break;
                       },

                       None => {
                           tracing::error!("Main thread channel closed");
                           break;
                       }

                       _ => {},
                   }
               },

               _ = tokio::time::sleep(Duration::from_millis(thread_rng().sample(wait_interval))) => {
                   // TODO: Error handling
                   let addr = *form_connections_with.choose(&mut rand::thread_rng()).unwrap();
                   let mut connected = false;
                   'inner: for _attempts in 0u8..3u8 {
                       let stream = match TcpStream::connect(addr).await {
                           Ok(stream) => stream,
                           Err(e) => {
                               tracing::error!("Error connecting to peer {}: {}", addr, e);
                               tokio::time::sleep(Duration::from_millis(100)).await;
                               continue 'inner;
                           }
                       };

                       let tls_connector = TlsConnector::from(client_config.clone());
                       let stream = match tls_connector.connect(
                           // FIXME: Don't hardcode
                           webpki::DNSNameRef::try_from_ascii_str("root.pp.lyne.dev")?,
                           stream
                       ).await {
                           Ok(stream) => stream,
                           Err(e) => {
                               tracing::error!("Error forming TLS connectionto peer {}: {}", addr, e);
                               tokio::time::sleep(Duration::from_millis(100)).await;
                               continue 'inner;
                           }
                       };

                       let stream = TlsConnection::new(stream);

                       // TODO: Negotiate ID
                       to_main.send(ToMain::ConnectedPeer(0, stream)).await?;
                       tracing::info!("Connected to peer with socket address {}", addr);
                       connected = true;
                       break 'inner;
                   }

                   let index = form_connections_with.iter().position(|&element| element == addr).unwrap();
                   let _ = form_connections_with.remove(index);

                   if !connected {
                      to_main.send(ToMain::FailedToConnect(addr)).await?;
                   }
                }
            }
        }
    }

    Ok(())
}
