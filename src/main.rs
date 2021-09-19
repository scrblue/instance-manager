use common_model::{instance_management::ServerHealth, InstancePath};

use anyhow::Result;
use async_raft::raft::Raft;
use indradb::{Datastore, Transaction};
use std::{collections::HashMap, net::SocketAddr, str::FromStr, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc,
};

mod configuration;
mod connections;
mod io;
mod messages;
mod peers;
mod state;
mod tls;

use peers::peer_tracker::*;
use state::StateManager;

use io::IoToMain as FromIo;

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

    // Create a SocketAddr type from the local configuration
    let listener_socket_addr = SocketAddr::from((local_conf.public_ip, local_conf.port_bound));

    // Filter the global list of peers so that the address of this manager is not included and to
    // find the Raft ID of this instacne
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

    // The state manager controls the databases and the handle is an abstraction for queries to it
    let state_manager = Arc::new(state::StateManager::new(
        self_id.unwrap(),
        local_conf.shared_conf_db_path,
        local_conf.log_file_path,
        local_conf.snapshot_save_dir,
    )?);

    // Spawn IO thread
    let (io_s, mut io_r) = mpsc::channel::<FromIo>(128);
    tokio::spawn(io::handle_io(io_s));

    // Prepare to spawn peer tracking thread
    let peer_tracker = PeerTacker::new(Arc::new(core_conf));
    let peer_tracker_handle = peer_tracker.handle();
    let raft_peer_handle = Arc::new(peer_tracker.handle());

    // Spawn instance tracking thread
    // Spawn console tracking thread

    // Spawn connection managment thread
    let connection_manager = connections::manager::ConnectionManager::new(
        client_conf,
        server_conf,
        listener_socket_addr,
        local_conf.is_core_member,
        peers,
    )
    .await?;
    let connection_manager_handle = connection_manager.handle();
    tokio::spawn(connection_manager.run(peer_tracker_handle.clone()));

    // Raft
    // TODO: Don't hardcode name or anything
    let raft_config = Arc::new(async_raft::Config::build("instance-manager".into()).validate()?);
    let raft = Arc::new(ImRaft::new(
        self_id.unwrap(),
        raft_config,
        raft_peer_handle,
        state_manager,
    ));

    // And actually spawn the PeerTacker, passing it a clone of the Arc<ImRaft>
    tokio::spawn(peer_tracker.run(raft.clone()));

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

    connection_manager_handle.shutdown().await?;
    peer_tracker_handle.shutdown().await?;

    // FIXME: IO shutdown

    Ok(())
}

pub enum AppState {
    Discovery,
    Initiation,
    ShareDistributedConf,
    RequestResponseLoop,
}

pub type ImRaft =
    Raft<messages::RaftRequest, messages::RaftResponse, PeerTrackerHandle, StateManager>;
