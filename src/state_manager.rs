use crate::messages::{RaftRequest, RaftResponse};

use anyhow::Result;
use async_raft::{async_trait::async_trait, raft::*, storage::*, RaftStorage};
use indradb::{
    Datastore, EdgeKey, EdgeQuery, EdgeQueryExt, MemoryDatastore, MemoryTransaction,
    RangeVertexQuery, RocksdbDatastore, RocksdbTransaction, SpecificVertexQuery, Transaction, Type,
    Vertex, VertexPropertyQuery, VertexQuery, VertexQueryExt,
};
use serde_json::{value, Value};
use std::{
    collections::btree_map::BTreeMap, error::Error, net::SocketAddr, path::PathBuf, sync::Arc,
};
use tokio::sync::{mpsc, oneshot, RwLock};
use uuid::Uuid;

#[derive(Clone)]
pub struct StateHandle {
    tlr_sender: mpsc::Sender<oneshot::Sender<Arc<RwLock<BTreeMap<u64, Entry<RaftRequest>>>>>>,

    dcr_sender: mpsc::Sender<oneshot::Sender<RocksdbTransaction>>,
    dcr_sync_sender: mpsc::Sender<()>,

    tsr_sender: mpsc::Sender<oneshot::Sender<MemoryTransaction>>,

    sdr_sender: mpsc::Sender<()>,

    self_raft_id: u64,
}

impl StateHandle {
    pub async fn shutdown(&self) -> Result<()> {
        self.sdr_sender.send(()).await?;

        Ok(())
    }

    pub async fn add_peer(&self, raft_id: u64, public_socket_addr: SocketAddr) -> Result<Uuid> {
        // To be cloned instead of making a new one every time
        let vertex_type = Type::new("InstanceManager")?;

        // Fetch a MemoryTransaction
        let (send, recv) = oneshot::channel();
        self.tsr_sender.send(send).await?;
        let transaction = recv.await?;

        // The existing InstanceManager instances must be fetched so that they can be made each
        // other's peers
        let existing_instance_managers = transaction
            .get_vertices(RangeVertexQuery::new().t(vertex_type.clone()))
            .map_err(|e| {
                anyhow::anyhow!("Error fetching existing instance managers in state: {}", e)
            })?;

        let existing_instance_manager_ids = transaction
            .get_vertex_properties(VertexPropertyQuery::new(
                SpecificVertexQuery::new(
                    existing_instance_managers.iter().map(|im| im.id).collect(),
                )
                .into(),
                "raft_id",
            ))
            .map_err(|e| {
                anyhow::anyhow!(
                    "Error fetching `raft_id`s of existing instance managers for state: {}",
                    e
                )
            })?;

        // But if the InstanceManager with this ID already appears in the graph, return an error
        if existing_instance_manager_ids
            .iter()
            .find(|&existing_id| existing_id.value == Value::from(raft_id))
            .is_some()
        {
            anyhow::bail!("The InstanceManager with this ID already exists in state");
        }

        // Actually create a new vertex
        let new_instance_manager_uuid = transaction
            .create_vertex_from_type(vertex_type.clone())
            .map_err(|e| anyhow::anyhow!("Error creating new InstanceManager in state: {}", e))?;

        // Set its raft_id and socket_addr
        transaction
            .set_vertex_properties(
                VertexPropertyQuery::new(
                    SpecificVertexQuery::single(new_instance_manager_uuid).into(),
                    "raft_id",
                ),
                &Value::from(raft_id),
            )
            .map_err(|e| anyhow::anyhow!("Error seting `raft_id` of new InstanceManager: {}", e))?;
        transaction
            .set_vertex_properties(
                VertexPropertyQuery::new(
                    SpecificVertexQuery::single(new_instance_manager_uuid).into(),
                    "socket_addr",
                ),
                &value::to_value(public_socket_addr)?,
            )
            .map_err(|e| {
                anyhow::anyhow!("Error seting `socket_addr` of new InstanceManager: {}", e)
            })?;

        // Finally create the `peers_with` relationship between this new InstanceManager and the
        // existing ones
        for existing_instance_manager in existing_instance_managers {
            let key = EdgeKey::new(
                existing_instance_manager.id,
                Type::new("peers_with")?,
                new_instance_manager_uuid,
            );
            let key_rev = key.reversed();

            transaction
                .create_edge(&key)
                .map_err(|e| anyhow::anyhow!("Error creating `peers_with` relationship: {}", e))?;
            transaction.create_edge(&key_rev).map_err(|e| {
                anyhow::anyhow!("Error creating reversed `peers_with` relationship: {}", e)
            })?;
        }

        Ok(new_instance_manager_uuid)
    }

    #[tracing::instrument(skip(self))]
    async fn get_dcr_transaction(&self) -> Result<RocksdbTransaction> {
        let (tx, rx) = oneshot::channel();
        self.dcr_sender.send(tx).await?;

        Ok(rx.await?)
    }

    #[tracing::instrument(skip(self))]
    async fn get_tlr_handle(&self) -> Result<Arc<RwLock<BTreeMap<u64, Entry<RaftRequest>>>>> {
        let (tx, rx) = oneshot::channel();
        self.tlr_sender.send(tx).await?;

        Ok(rx.await?)
    }

    #[tracing::instrument(skip(self))]
    async fn get_hard_state(&self) -> Result<Option<HardState>> {
        let transaction = self.get_dcr_transaction().await?;

        let json = transaction
            .get_vertex_properties(VertexPropertyQuery::new(
                RangeVertexQuery::new()
                    .limit(1)
                    .t(Type::new("HardState")?)
                    .into(),
                "hard_state",
            ))
            .map_err(|e| anyhow::anyhow!("Error fetching HardState: {}", e))?
            .pop();

        if let Some(json) = json {
            let parsed = serde_json::from_value(json.value)?;
            Ok(parsed)
        } else {
            Ok(None)
        }
    }

    #[tracing::instrument(skip(self))]
    async fn get_last_applied_entry(&self) -> Result<Option<u64>> {
        let transaction = self.get_dcr_transaction().await?;

        let json = transaction
            .get_vertex_properties(VertexPropertyQuery::new(
                RangeVertexQuery::new()
                    .limit(1)
                    .t(Type::new("StateMachine")?)
                    .into(),
                "last_applied",
            ))
            .map_err(|e| anyhow::anyhow!("Error fetching HardState: {}", e))?
            .pop();

        if let Some(json) = json {
            let parsed = serde_json::from_value(json.value)?;
            Ok(parsed)
        } else {
            Ok(None)
        }
    }

    #[tracing::instrument(skip(self))]
    async fn get_last_log_segment(&self) -> Result<Option<Vertex>> {
        let transaction = self.get_dcr_transaction().await?;
        Ok(transaction
            .get_vertices(
                RangeVertexQuery::new()
                    .limit(1)
                    .t(indradb::Type::new("LastLogTracker")?)
                    .outbound()
                    .outbound(),
            )
            .map_err(|e| anyhow::anyhow!("Error fetching pointer to last log entry"))?
            .pop())
    }

    #[tracing::instrument(skip(self))]
    async fn get_entries_for_log_segment(&self, log: &Vertex) -> Result<Vec<Entry<RaftRequest>>> {
        let transaction = self.get_dcr_transaction().await?;

        let prop = transaction
            .get_vertex_properties(VertexPropertyQuery::new(
                SpecificVertexQuery::single(log.id).into(),
                "log_entries",
            ))
            .map_err(|e| anyhow::anyhow!("Error fetching entries for vertex: {}", e))?
            .pop()
            .ok_or_else(|| anyhow::anyhow!("Vertex given does not have log_entries property"))?;

        Ok(serde_json::from_value(prop.value)?)
    }

    #[tracing::instrument(skip(self))]
    async fn get_previous_log_segment(&self, log: &Vertex) -> Result<Option<Vertex>> {
        let transaction = self.get_dcr_transaction().await?;

        Ok(transaction
            .get_vertices(
                SpecificVertexQuery::single(log.id)
                    .outbound()
                    .t(Type::new("following")?)
                    .outbound(),
            )
            .map_err(|e| anyhow::anyhow!("Error fetching previous log segment"))?
            .pop())
    }

    #[tracing::instrument(skip(self))]
    async fn get_next_log_segment(&self, log: &Vertex) -> Result<Option<Vertex>> {
        let transaction = self.get_dcr_transaction().await?;

        Ok(transaction
            .get_vertices(
                SpecificVertexQuery::single(log.id)
                    .outbound()
                    .t(Type::new("followed_by")?)
                    .outbound(),
            )
            .map_err(|e| anyhow::anyhow!("Error fetching next log segment"))?
            .pop())
    }
}

#[derive(Debug, Copy, Clone)]
pub enum RaftStorageError {
    Unknown,
}

impl std::fmt::Display for RaftStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Unknown Raft storage error")
    }
}

impl Error for RaftStorageError {}

// FIXME: First time adding of essential vertices like HardState, LastLogTracker, and StateMachine
// TODO: Cache the Uuids of these essential vertices
#[async_trait]
impl RaftStorage<RaftRequest, RaftResponse> for StateHandle {
    type Snapshot = tokio::fs::File;
    type ShutdownError = RaftStorageError;

    #[tracing::instrument(skip(self))]
    async fn get_membership_config(&self) -> Result<MembershipConfig> {
        // First check the in memory log
        let log = self.get_tlr_handle().await?;
        let log = log.read().await;

        let membership_config = if let Some(membership_config) =
            log.values().rev().find_map(|entry| match &entry.payload {
                EntryPayload::ConfigChange(cc) => Some(cc.membership.clone()),
                EntryPayload::SnapshotPointer(sp) => Some(sp.membership.clone()),
                _ => None,
            }) {
            membership_config
        } else {
            // Otherwise check the logs stored in the graph DB
            let last = self.get_last_log_segment().await?;

            // If there is a last log segment, search in reverse to find the most recent membership
            // config
            // A snapshot pointer will also contain a membership config
            if let Some(mut last) = last {
                let mut membership_config = None;

                while membership_config.is_none() {
                    let data = self.get_entries_for_log_segment(&last).await?;
                    membership_config = data.iter().rev().find_map(|entry| match &entry.payload {
                        EntryPayload::ConfigChange(cc) => Some(cc.membership.clone()),
                        EntryPayload::SnapshotPointer(sp) => Some(sp.membership.clone()),
                        _ => None,
                    });

                    // TODO: Error handling
                    last = self.get_previous_log_segment(&last).await?.unwrap();
                }

                membership_config.unwrap()
            }
            // If there is no last log entry, return an initiall MembershipConfig
            else {
                MembershipConfig::new_initial(self.self_raft_id)
            }
        };

        Ok(membership_config)
    }

    #[tracing::instrument(skip(self))]
    async fn get_initial_state(&self) -> Result<InitialState> {
        let last = self.get_last_log_segment().await?;

        if let Some(mut last) = last {
            let mut membership = None;
            let mut last_log_index = None;
            let mut last_log_term = None;

            while membership.is_none() {
                let entry = self.get_entries_for_log_segment(&last).await?;

                // TODO: Error handling
                if last_log_index.is_none() {
                    last_log_index = Some(entry.last().unwrap().index);
                    last_log_term = Some(entry.last().unwrap().term);
                }

                membership = entry.iter().rev().find_map(|entry| match entry.payload {
                    EntryPayload::ConfigChange(cc) => Some(cc.membership.clone()),
                    EntryPayload::SnapshotPointer(sp) => Some(sp.membership.clone()),
                    _ => None,
                });

                last = self
                    .get_previous_log_segment(&last)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("No previous log entry"))?;
            }

            let last_applied_log = self.get_last_applied_entry().await?;
            let hard_state = self.get_hard_state().await?;

            Ok(InitialState {
                last_log_index: last_log_index.unwrap(),
                last_log_term: last_log_term.unwrap(),
                last_applied_log: last_applied_log.unwrap(),
                hard_state: hard_state.unwrap(),
                membership: membership.unwrap(),
            })
        } else {
            Ok(InitialState::new_initial(self.self_raft_id))
        }
    }

    #[tracing::instrument(skip(self))]
    async fn save_hard_state(&self, hs: &HardState) -> Result<()> {
        let transaction = self.get_dcr_transaction().await?;

        transaction
            .set_vertex_properties(
                VertexPropertyQuery::new(
                    RangeVertexQuery::new()
                        .limit(1)
                        .t(Type::new("HardState")?)
                        .into(),
                    "hard_state",
                ),
                &serde_json::json!(hs),
            )
            .map_err(|e| anyhow::anyhow!("Error saving HardState: {}", e))?;

        self.dcr_sync_sender.send(()).await.map_err(|e| {
            anyhow::anyhow!(
                "Error sending sync state request while saving HardState: {}",
                e
            )
        })
    }

    #[tracing::instrument(skip(self))]
    async fn get_log_entries(&self, start: u64, stop: u64) -> Result<Vec<Entry<RaftRequest>>> {
        // First check the cache
        let log = self.get_tlr_handle().await?;
        let log = log.read().await;

        // Note the min and max
        let log_min = log.keys().next();

        Ok(match log_min {
            Some(min) if min <= &start => log
                .iter()
                .filter_map(|(k, v)| {
                    if k >= &start && k < &stop {
                        Some(v.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>(),

            _ => {
                let transaction = self.get_dcr_transaction().await?;
                let mut log_segment = None;

                let mut segment_min: Option<u64> = None;

                while segment_min.is_none() || segment_min.unwrap() > start {
                    log_segment = if let Some(log_segment) = log_segment {
                        self.get_previous_log_segment(&log_segment).await?
                    } else {
                        self.get_last_log_segment().await?
                    };

                    let segment_data = self
                        .get_entries_for_log_segment(&log_segment.unwrap())
                        .await?;
                    let segment_min = segment_data.first();
                }

                // log_segment now contains the start point
                let mut segment_max: Option<u64> = None;
                let mut out = Vec::new();

                while segment_max.is_none() || segment_max.unwrap() < stop {
                    let segment_data = self
                        .get_entries_for_log_segment(&log_segment.unwrap())
                        .await?;
                    segment_max = segment_data.last().map(|e| e.index);
                    for data in segment_data {
                        if data.index >= start && data.index < stop {
                            out.push(data.clone())
                        }
                    }

                    if segment_max.unwrap() < stop {
                        log_segment = self.get_next_log_segment(&log_segment.unwrap()).await?;

                        // If the log_segment is None, then the mx must be in the cache
                        if log_segment.is_none() {
                            for (k, v) in log.iter() {
                                if k >= &start && k < &stop {
                                    out.push(v.clone())
                                }
                            }

                            break;
                        }
                    }
                }

                out
            }
        })
    }

    #[tracing::instrument(skip(self))]
    async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> Result<()> {
        // TODO: Error handling
        let log = self.get_tlr_handle().await?;

        let log = log.read().await;
        let log_max = log.keys().max();
        let log_min = log.keys().min();

        match log_min {
            Some(log_min) if log_min <= &start => {
                std::mem::drop(log);
                let log = self.get_tlr_handle().await?;
                let log = log.write().await;

                if let Some(stop) = stop {
                    for key in start..stop {
                        log.remove(&key);
                    }
                } else {
                    for key in start..*log_max.unwrap() {
                        log.remove(&key);
                    }
                }

                Ok(())
            }
            _ => {
                let transaction = self.get_dcr_transaction().await?;

                let segment_min = u64::MAX;
                let mut segment = None;

                while segment_min > start {
                    segment = if let Some(segment) = segment {
                        self.get_previous_log_segment(&segment).await?
                    } else {
                        self.get_last_log_segment().await?
                    };

                    let values = self.get_entries_for_log_segment(&segment.unwrap()).await?;
                    segment_min = values.last().unwrap().index;
                }

                let stop = if let Some(stop) = stop {
                    stop
                } else {
                    u64::MAX
                };
                let mut segment_max = 0;

                while segment_max < stop {
                    let values = self.get_entries_for_log_segment(&segment.unwrap()).await?;
                    segment_max = values.last().unwrap().index;

                    if segment_max < stop {
                        // Clear the whole data
                        transaction
                            .set_vertex_properties(
                                VertexPropertyQuery::new(
                                    SpecificVertexQuery::single(segment.unwrap().id).into(),
                                    "log_entries",
                                ),
                                &serde_json::json! {()},
                            )
                            .unwrap();
                    } else {
                        let filtered = values
                            .iter()
                            .filter(|value| value.index < stop)
                            .collect::<Vec<_>>();

                        transaction
                            .set_vertex_properties(
                                VertexPropertyQuery::new(
                                    SpecificVertexQuery::single(segment.unwrap().id).into(),
                                    "log_entries",
                                ),
                                &serde_json::json! {filtered},
                            )
                            .unwrap();
                        break;
                    }

                    segment = self.get_next_log_segment(&segment.unwrap()).await?;

                    // Then the max must be in the cache
                    if segment.is_none() {
                        std::mem::drop(log);
                        let log = self.get_tlr_handle().await?;
                        let log = log.write().await;

                        for key in start..*log_max.unwrap() {
                            log.remove(&key);
                        }

                        break;
                    }
                }

                Ok(())
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn append_entry_to_log(&self, entry: &Entry<RaftRequest>) -> Result<()> {
        // Must change StateMachine and LastLogTracker
    }

    #[tracing::instrument(skip(self))]
    async fn replicate_to_log(&self, entries: &[Entry<RaftRequest>]) -> Result<()> {}

    #[tracing::instrument(skip(self))]
    async fn apply_entry_to_state_machine(
        &self,
        index: &u64,
        data: &RaftRequest,
    ) -> Result<RaftResponse> {
    }

    #[tracing::instrument(skip(self))]
    async fn replicate_to_state_machine(&self, entries: &[(&u64, &RaftRequest)]) -> Result<()> {}

    #[tracing::instrument(skip(self))]
    async fn do_log_compaction(&self) -> Result<CurrentSnapshotData<Self::Snapshot>> {}

    #[tracing::instrument(skip(self))]
    async fn create_snapshot(&self) -> Result<(String, Box<Self::Snapshot>)> {}

    #[tracing::instrument(skip(self))]
    async fn finalize_snapshot_installation(
        &self,
        index: u64,
        term: u64,
        delete_through: Option<u64>,
        id: String,
        snapshot: Box<Self::Snapshot>,
    ) -> Result<()> {
    }

    #[tracing::instrument(skip(self))]
    async fn get_current_snapshot(&self) -> Result<Option<CurrentSnapshotData<Self::Snapshot>>> {}
}

pub struct StateManager {
    self_raft_id: u64,

    temporary_log: Arc<RwLock<BTreeMap<u64, Entry<RaftRequest>>>>,
    temporary_log_request_receiver:
        mpsc::Receiver<oneshot::Sender<Arc<RwLock<BTreeMap<u64, Entry<RaftRequest>>>>>>,

    distributed_conf: RocksdbDatastore,
    distributed_conf_request_receiver: mpsc::Receiver<oneshot::Sender<RocksdbTransaction>>,
    distributed_conf_sync_request_receiver: mpsc::Receiver<()>,

    temporary_state: MemoryDatastore,
    temporary_state_request_receiver: mpsc::Receiver<oneshot::Sender<MemoryTransaction>>,

    shutdown_request_receiver: mpsc::Receiver<()>,

    // The senders to be cloned when spawning a StateHandle
    tlr_sender: mpsc::Sender<oneshot::Sender<Arc<RwLock<BTreeMap<u64, Entry<RaftRequest>>>>>>,
    dcr_sender: mpsc::Sender<oneshot::Sender<RocksdbTransaction>>,
    dcs_sender: mpsc::Sender<()>,
    tsr_sender: mpsc::Sender<oneshot::Sender<MemoryTransaction>>,
    sdr_sender: mpsc::Sender<()>,
}

impl StateManager {
    #[tracing::instrument]
    pub fn new(
        self_raft_id: u64,
        distributed_path: PathBuf,
        temporary_path: PathBuf,
    ) -> Result<StateManager> {
        let temporary_log = Arc::new(RwLock::from(BTreeMap::new()));
        let distributed_conf = indradb::RocksdbDatastore::new(distributed_path, None)
            .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        let temporary_state = indradb::MemoryDatastore::create(temporary_path)?;

        let (tlr_sender, temporary_log_request_receiver) = mpsc::channel(128);
        let (dcr_sender, distributed_conf_request_receiver) = mpsc::channel(128);
        let (dcs_sender, distributed_conf_sync_request_receiver) = mpsc::channel(128);
        let (tsr_sender, temporary_state_request_receiver) = mpsc::channel(128);
        let (sdr_sender, shutdown_request_receiver) = mpsc::channel(128);

        Ok(StateManager {
            self_raft_id,
            temporary_log,
            temporary_log_request_receiver,
            distributed_conf,
            distributed_conf_request_receiver,
            distributed_conf_sync_request_receiver,
            temporary_state,
            temporary_state_request_receiver,
            shutdown_request_receiver,
            tlr_sender,
            dcr_sender,
            dcs_sender,
            tsr_sender,
            sdr_sender,
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
            tlr_sender: self.tlr_sender.clone(),
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
