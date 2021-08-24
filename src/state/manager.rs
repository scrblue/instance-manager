use super::handle::StateHandle;
use crate::messages::{
    ConsoleNetworkRequest, ConsoleNetworkResponse, InstanceNetworkRequest, InstanceNetworkResponse,
    ManagerNetworkRequest, RaftRequest, RaftResponse,
};

use anyhow::Result;
use async_raft::{async_trait::async_trait, raft::*, storage::*, RaftStorage};
use indradb::{
    Datastore, EdgeKey, EdgeQuery, EdgeQueryExt, MemoryDatastore, MemoryTransaction,
    RangeVertexQuery, RocksdbDatastore, RocksdbTransaction, SpecificVertexQuery, Transaction, Type,
    Vertex, VertexPropertyQuery, VertexQuery, VertexQueryExt,
};
use rocksdb::DB;
use serde_json::{value, Value};
use std::{
    collections::{btree_map::BTreeMap, hash_map::HashMap},
    error::Error,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::{mpsc, oneshot, RwLock};
use uuid::Uuid;

pub struct StateManager {
    self_raft_id: u64,

    raft_log_db: Arc<RwLock<DB>>,
    raft_log_db_request_receiver: mpsc::Receiver<oneshot::Sender<Arc<RwLock<DB>>>>,

    distributed_state: RocksdbDatastore,
    distributed_state_request_receiver: mpsc::Receiver<oneshot::Sender<RocksdbTransaction>>,
    distributed_state_sync_request_receiver: mpsc::Receiver<()>,

    shutdown_request_receiver: mpsc::Receiver<()>,

    // The senders to be cloned when spawning a StateHandle
    rlr_sender: mpsc::Sender<oneshot::Sender<Arc<RwLock<DB>>>>,
    dsr_sender: mpsc::Sender<oneshot::Sender<RocksdbTransaction>>,
    dss_sender: mpsc::Sender<()>,
    sdr_sender: mpsc::Sender<()>,
    snapshot_dir: PathBuf,
}

impl StateManager {
    #[tracing::instrument]
    pub fn new(
        self_raft_id: u64,
        distributed_path: PathBuf,
        log_path: PathBuf,
        snapshot_dir: PathBuf,
    ) -> Result<StateManager> {
        let raft_log_db = Arc::new(RwLock::new(DB::open_default(log_path)?));
        let distributed_state = indradb::RocksdbDatastore::new(distributed_path, None)
            .map_err(|e| anyhow::anyhow!("{:?}", e))?;

        let (rlr_sender, raft_log_db_request_receiver) = mpsc::channel(128);
        let (dsr_sender, distributed_state_request_receiver) = mpsc::channel(128);
        let (dss_sender, distributed_state_sync_request_receiver) = mpsc::channel(128);
        let (sdr_sender, shutdown_request_receiver) = mpsc::channel(128);

        Ok(StateManager {
            self_raft_id,
            raft_log_db,
            raft_log_db_request_receiver,
            distributed_state,
            distributed_state_request_receiver,
            distributed_state_sync_request_receiver,
            shutdown_request_receiver,
            rlr_sender,
            dsr_sender,
            dss_sender,
            sdr_sender,
            snapshot_dir,
        })
    }

    /// Returns a StateHandle to be used to interact with the StateManager after it has been spawned
    pub fn handle(&self) -> StateHandle {
        StateHandle {
            self_raft_id: self.self_raft_id,
            dsr_sender: self.dsr_sender.clone(),
            dsr_sync_sender: self.dss_sender.clone(),
            sdr_sender: self.sdr_sender.clone(),
            rlr_sender: self.rlr_sender.clone(),
            snapshot_dir: self.snapshot_dir.clone(),
        }
    }

    /// Runs the main loop of the StateManager consuming the structure. Interact with the thread it
    /// creates using a StateHandle
    /// FIXME: Support the other receivers
    #[tracing::instrument(skip(self))]
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                _ = self.shutdown_request_receiver.recv() => break,
                option_tx = self.distributed_state_request_receiver.recv() => match option_tx {
                    Some(tx) => tx.send(self.distributed_state.transaction().unwrap()).unwrap(),
                    None => {
                        tracing::error!("StateManager has no means to receive distributed configuration requests");
                        break
                    }
                },
            }
        }

        tracing::info!("Shutting down StateManager");
        if let Err(e) = self.distributed_state.sync() {
            tracing::error!("Error syncing distributed_conf: {}", e);
        };
    }
}
