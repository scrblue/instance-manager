use common_model::{instance_management::ServerHealth, InstancePath};

use anyhow::Result;
use indradb::{Datastore, Transaction};
use std::{collections::HashMap, net::SocketAddr, str::FromStr, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc,
};

mod configuration;
mod connection_manager;
mod io;
mod messages;
mod peers;
use peers::peer_tracker;
mod state_manager;
mod tls;

use connection_manager::ConnectionManagerToMain as FromConnectionManager;
use connection_manager::MainToConnectionManager as ToConnectionManager;

use io::IoToMain as FromIo;

use peer_tracker::MainToPeerTracker as ToPeerTracker;
use peer_tracker::PeerTrackerToMain as FromPeerTracker;

#[tokio::main]
async fn main() -> Result<()> {
    // The local configuration and shared configuration are loaded from a file specified by the
    // argument given to the program on launch
    let (local_conf, core_conf) = configuration::setup().await?;

    // Setup logging
    let log_level = tracing::Level::from_str(local_conf.log_level.as_deref().unwrap_or("INFO"))?;
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(log_level)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    tracing::info!("Loaded configuration and logging enabled");

    // The ConnectionManager requires a server and client configuration for TLS
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

    // The state manager controls the databases and the handle is an abstraction for queries to it
    let state_manager = state_manager::StateManager::new(
        local_conf.shared_conf_db_path,
        local_conf.cache_file_path,
    )?;
    let state_handle = state_manager.handle();
    state_manager.run();

    // Spawn IO thread
    let (mut io_s, mut io_r) = mpsc::channel::<FromIo>(128);
    tokio::spawn(io::handle_io(io_s));

    // Spawn connection managment thread

    // First create the channels
    let (connection_manager_to_main_s, mut connection_manager_r) =
        mpsc::channel::<FromConnectionManager>(128);
    let (mut connection_manager_s, main_to_connection_manager_r) =
        mpsc::channel::<ToConnectionManager>(128);

    // Create a SocketAddr type from the local configuration
    let listener_socket_addr = SocketAddr::from((local_conf.public_ip, local_conf.port_bound));

    // Filter the global list of peers so that the address of this manager is not included
    let mut self_id = None;
    let peers = core_conf
        .peers
        .iter()
        .filter(|(id, addr)| {
            if *addr != listener_socket_addr {
                true
            } else {
                self_id = Some(*id);
                false
            }
        })
        .map(|id_and_addr| *id_and_addr)
        .collect::<Vec<_>>();

    // Then actually spawn it
    tokio::spawn(connection_manager::handle_connections(
        connection_manager_to_main_s,
        main_to_connection_manager_r,
        listener_socket_addr,
        server_conf,
        client_conf,
        peers,
    ));

    // Insert self into graph of running instances
    let self_uuid = state_handle
        .add_peer(
            self_id.ok_or(anyhow::anyhow!(
                "This InstanceManager is not a part of the shared configuration"
            ))?,
            listener_socket_addr,
        )
        .await?;

    // Spawn peer tracking thread
    let (mut peer_tracker_to_main_s, mut peer_tracker_r) = mpsc::channel::<FromPeerTracker>(128);
    let (mut peer_tracker_s, mut main_to_peer_tracker_r) = mpsc::channel::<ToPeerTracker>(128);

    tokio::spawn(peer_tracker::track_peers(
        peer_tracker_to_main_s,
        main_to_peer_tracker_r,
        state_handle.clone(),
    ));

    // Spawn instance tracking thread

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

            msg = connection_manager_r.recv() => {
                match msg {
                    Some(FromConnectionManager::ConnectedPeer(id, listener_addr, stream)) => {
                        peer_tracker_s.send(ToPeerTracker::NewPeer(id, listener_addr, stream)).await?;
                    },
                    Some(msg) => tracing::info!("CM msg: {:?}", msg),
                    None => {
                        // TODO: Restart on failuer
                        tracing::error!("Connection manager channel closed");
                        break;
                    },
                }
            }

            msg = peer_tracker_r.recv() => {
                match msg {
                   Some(msg) => tracing::info!("PT msg: {:?}", msg),
                   None => {
                       tracing::error!("Peer tracker channel closed");
                       break;
                   }
                }
            }
        }
    }

    connection_manager_s
        .send(ToConnectionManager::Shutdown)
        .await?;

    peer_tracker_s.send(ToPeerTracker::Shutdown).await?;

    // FIXME: IO shutdown

    Ok(())
}
