use crate::messages::{
    ConsoleNetworkRequest, ConsoleNetworkResponse, InstanceNetworkRequest, InstanceNetworkResponse,
    ManagerNetworkRequest, RaftRequest, RaftResponse,
};
use super::handle::StateHandle;

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

    distributed_conf: RocksdbDatastore,
    distributed_conf_request_receiver: mpsc::Receiver<oneshot::Sender<RocksdbTransaction>>,
    distributed_conf_sync_request_receiver: mpsc::Receiver<()>,

    temporary_state: MemoryDatastore,
    temporary_state_request_receiver: mpsc::Receiver<oneshot::Sender<MemoryTransaction>>,

    shutdown_request_receiver: mpsc::Receiver<()>,

    // The senders to be cloned when spawning a StateHandle
    rlr_sender: mpsc::Sender<oneshot::Sender<Arc<RwLock<DB>>>>,
    dcr_sender: mpsc::Sender<oneshot::Sender<RocksdbTransaction>>,
    dcs_sender: mpsc::Sender<()>,
    tsr_sender: mpsc::Sender<oneshot::Sender<MemoryTransaction>>,
    sdr_sender: mpsc::Sender<()>,
    snapshot_dir: PathBuf,
}

impl StateManager {
    #[tracing::instrument]
    pub fn new(
        self_raft_id: u64,
        distributed_path: PathBuf,
        temporary_path: PathBuf,
        log_path: PathBuf,
        snapshot_dir: PathBuf,
    ) -> Result<StateManager> {
		let raft_log_db = Arc::new(RwLock::new(DB::open_default(log_path)?));
        let distributed_conf = indradb::RocksdbDatastore::new(distributed_path, None)
            .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        let temporary_state = indradb::MemoryDatastore::create(temporary_path)?;

        let (rlr_sender, raft_log_db_request_receiver) = mpsc::channel(128);
        let (dcr_sender, distributed_conf_request_receiver) = mpsc::channel(128);
        let (dcs_sender, distributed_conf_sync_request_receiver) = mpsc::channel(128);
        let (tsr_sender, temporary_state_request_receiver) = mpsc::channel(128);
        let (sdr_sender, shutdown_request_receiver) = mpsc::channel(128);

        Ok(StateManager {
            self_raft_id,
            raft_log_db,
            raft_log_db_request_receiver,
            distributed_conf,
            distributed_conf_request_receiver,
            distributed_conf_sync_request_receiver,
            temporary_state,
            temporary_state_request_receiver,
            shutdown_request_receiver,
            rlr_sender,
            dcr_sender,
            dcs_sender,
            tsr_sender,
            sdr_sender,
            snapshot_dir,
        })
    }

    /// Returns a StateHandle to be used to interact with the StateManager after it has been spawned
    pub fn handle(&self) -> StateHandle {
        StateHandle {
            self_raft_id: self.self_raft_id,
            dcr_sender: self.dcr_sender.clone(),
            dcr_sync_sender: self.dcs_sender.clone(),
            tsr_sender: self.tsr_sender.clone(),
            sdr_sender: self.sdr_sender.clone(),
            rlr_sender: self.rlr_sender.clone(),
            snapshot_dir: self.snapshot_dir.clone(),
        }
    }

    /// Runs the main loop of the StateManager consuming the structure. Interact with the thread it
    /// creates using a StateHandle
    #[tracing::instrument(skip(self))]
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                _ = self.shutdown_request_receiver.recv() => break,
                option_tx = self.distributed_conf_request_receiver.recv() => match option_tx {
                    Some(tx) => tx.send(self.distributed_conf.transaction().unwrap()).unwrap(),
                    None => {
                        tracing::error!("StateManager has no means to receive distributed configuration requests");
                        break
                    }
                },
                option_tx = self.temporary_state_request_receiver.recv() => match option_tx {
                    Some(tx) => tx.send(self.temporary_state.transaction().unwrap()).unwrap(),
                    None => {
                        tracing::error!("StateManager has no means to receive temporary state requests");
                        break
                    }
                }
            }
        }

        tracing::info!("Shutting down StateManager");
        if let Err(e) = self.distributed_conf.sync() {
            tracing::error!("Error syncing distributed_conf: {}", e);
        };
    }
}
