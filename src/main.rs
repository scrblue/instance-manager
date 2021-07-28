use common_model::{instance_management::ServerHealth, InstancePath};

use anyhow::Result;
use std::{collections::HashMap, net::SocketAddr, str::FromStr, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
};
use tokio_rustls::TlsAcceptor;
use tokio_tls::TlsConnection;

mod configuration;
mod connection_manager;
mod io;
mod messages;
mod tls;

use connection_manager::ConnectionManagerToMain as FromConnectionManager;
use connection_manager::MainToConnectionManager as ToConnectionManager;
use io::IoToMain as FromIo;

#[tokio::main]
async fn main() -> Result<()> {
    let (local_conf, core_conf) = configuration::setup().await?;

    let log_level = tracing::Level::from_str(local_conf.log_level.as_deref().unwrap_or("INFO"))?;
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(log_level)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    tracing::info!("Loaded configuration and logging enabled");

    let (server_conf, client_conf) = tls::set_up_config(
        &core_conf.root_ca_cert,
        Some(&core_conf.node_ca_cert),
        Some(&core_conf.admin_ca_cert),
        &local_conf.server_public_key,
        &local_conf.server_private_key,
    )
    .await?;

    let server_conf = Arc::new(server_conf);
    let client_conf = Arc::new(client_conf);

    let distributed_conf = Arc::new(
        indradb::RocksdbDatastore::new("TODO.db", None).map_err(|e| anyhow::anyhow!("{:?}", e))?,
    );
    let state = Arc::new(indradb::MemoryDatastore::create("TODO.tmp")?);

    let (mut io_s, mut io_r) = mpsc::channel::<FromIo>(128);

    // Spawn IO thread
    tokio::spawn(io::handle_io(io_s));

    // Spawn ConnectionManager
    let (mut connection_manager_to_main_s, connection_manager_r) =
        mpsc::channel::<FromConnectionManager>(128);
    let (mut connection_manager_s, main_to_connection_manager_r) =
        mpsc::channel::<FromConnectionManager>(128);

    let listener_socket_addr = SocketAddr::from((local_conf.public_ip, local_conf.port_bound));

    // Filter the global list of peers so that the address of this manager is not included
    let mut peers = core_conf
        .peers
        .iter()
        .filter(|addr| **addr != listener_socket_addr)
        .collect::<Vec<_>>();

    tokio::spawn(connection_manager::handle_connections(
        connection_manager_to_main_s,
        main_to_connection_manager_r,
        listener_socket_addr,
        server_conf,
        client_conf,
        peers,
    ));

    // Spawn RequestManager

    // Spawn PeerTracker
    // Spawn InstanceTracker

    loop {
        tokio::select! {
            msg = io_r.recv() => {
                match msg {
                    Some(FromIo::NetworkRequest(messages::ConsoleNetworkRequest::Shutdown(_))) => {
                        break;
                    },
                    None => {
                        tracing::error!("IO thread channel closed");
                        break;
                    }
                    _ => {},
                }
            }
        }
    }

    Ok(())
}
