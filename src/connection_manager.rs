use anyhow::Result;
use rustls::{ClientConfig, ServerConfig};
use std::{
    net::SocketAddr,
    sync::Arc
};
use tokio::{
    net::TcpListener,
    sync::mpsc,
};
use tokio_rustls::TlsAcceptor;
use tokio_tls::TlsConnection;

#[derive(Clone, Debug, Copy, PartialEq)]
pub enum ConnectionManagerToMain {
	ConnectedPeer(usize, SocketAddr),
	ConnectedInstance(SocketAddr),
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
	loop {
		tokio::select! {
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
	}

	Ok(())
}
