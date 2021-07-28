use rustls::{ClientConfig, ServerConfig};
use std::{
    net::SocketAddr,
    sync::Arc
};
use tokio::sync::mpsc;

pub enum ConnectionManagerToMain {
	IncomingPeer(usize, SocketAddr),
	IncomingInstance(SocketAddr),
}
use ConnectionManagerToMain as ToMain;

pub enum MainToConnectionManager {
    Connect(SocketAddr),
}
use MainToConnectionManager as FromMain;

pub fn handle_connections(
    to_main: mpsc::Sender<ToMain>,
    from_main: mpsc::Receiver<FromMain>,
    listener_socket_addr: SocketAddr,
    server_config: Arc<ServerConfig>,
    client_config: Arc<ClientConfig>,
    initial_connections: Vec<SocketAddr>,
) {
	// Start listening
    let tcp_lisenter = TcpListener::bind(listener_socket_addr).await?;
    let acceptor = TlsAcceptor::from(server_conf);


	// Add initial_connections to randomized connection queue

	// Main loop
}
