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
use std::{collections::HashMap, error::Error, net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{mpsc, oneshot, RwLock},
};
use uuid::Uuid;

#[derive(Clone)]
pub struct StateHandle {
    // Raft log DB handle request
    rlr_sender: mpsc::Sender<oneshot::Sender<Arc<RwLock<DB>>>>,

    // Distributed state Transaction request
    dsr_sender: mpsc::Sender<oneshot::Sender<RocksdbTransaction>>,
    // Distributed configuration sync request
    dsr_sync_sender: mpsc::Sender<()>,

    // Shutdown request
    sdr_sender: mpsc::Sender<()>,

    self_raft_id: u64,
    snapshot_dir: PathBuf,
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
        self.dsr_sender.send(send).await?;
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
    async fn get_dsr_transaction(&self) -> Result<RocksdbTransaction> {
        let (tx, rx) = oneshot::channel();
        self.dsr_sender.send(tx).await?;

        Ok(rx.await?)
    }

    #[tracing::instrument(skip(self))]
    async fn get_rlr_handle(&self) -> Result<Arc<RwLock<DB>>> {
        let (tx, rx) = oneshot::channel();
        self.rlr_sender.send(tx).await?;

        Ok(rx.await?)
    }

    #[tracing::instrument(skip(self))]
    async fn get_hard_state(&self) -> Result<Option<HardState>> {
        let transaction = self.get_dsr_transaction().await?;

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
        let transaction = self.get_dsr_transaction().await?;

        let json = transaction
            .get_vertex_properties(VertexPropertyQuery::new(
                RangeVertexQuery::new()
                    .limit(1)
                    .t(Type::new("StateMachine")?)
                    .into(),
                "last_applied_entry",
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
        let handle = self.get_rlr_handle().await?;
        let log = handle.read().await;

        let membership = log
            .iterator(rocksdb::IteratorMode::End)
            .find_map(|(key, value)| {
                let entry = bincode::deserialize::<Entry<RaftRequest>>(value.as_ref())
                    .map(|entry| Some(entry))
                    .unwrap_or(None);

                if let Some(entry) = entry {
                    match entry.payload {
                        EntryPayload::ConfigChange(cc) => Some(cc.membership),
                        EntryPayload::SnapshotPointer(sp) => Some(sp.membership),
                        _ => None,
                    }
                } else {
                    None
                }
            });

        Ok(membership.unwrap_or(MembershipConfig::new_initial(self.self_raft_id)))
    }

    #[tracing::instrument(skip(self))]
    async fn get_initial_state(&self) -> Result<InitialState> {
        let handle = self.get_rlr_handle().await?;
        let log = handle.read().await;

        let mut membership = None;
        let mut last_log_index = None;
        let mut last_log_term = None;

        for (key, value) in log.iterator(rocksdb::IteratorMode::End) {
            let entry = bincode::deserialize::<Entry<RaftRequest>>(value.as_ref())
                .map(|entry| Some(entry))
                .unwrap_or(None);

            if let Some(entry) = entry {
                if last_log_index.is_none() {
                    last_log_index = Some(entry.index);
                    last_log_term = Some(entry.term);
                }

                membership = match entry.payload {
                    EntryPayload::ConfigChange(cc) => Some(cc.membership.clone()),
                    EntryPayload::SnapshotPointer(sp) => Some(sp.membership.clone()),
                    _ => None,
                };

                if membership.is_some() {
                    break;
                }
            }
        }

        if membership.is_none() {
            Ok(InitialState::new_initial(self.self_raft_id))
        } else {
            // FIXME: UPDATE THESE METHODS
            let last_applied_log = self.get_last_applied_entry().await?;
            let hard_state = self.get_hard_state().await?;

            Ok(InitialState {
                last_log_index: last_log_index.unwrap(),
                last_log_term: last_log_term.unwrap(),
                last_applied_log: last_applied_log.unwrap(),
                hard_state: hard_state.unwrap(),
                membership: membership.unwrap(),
            })
        }
    }

    #[tracing::instrument(skip(self))]
    async fn save_hard_state(&self, hs: &HardState) -> Result<()> {
        let transaction = self.get_dsr_transaction().await?;

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

        self.dsr_sync_sender.send(()).await.map_err(|e| {
            anyhow::anyhow!(
                "Error sending sync state request while saving HardState: {}",
                e
            )
        })
    }

    #[tracing::instrument(skip(self))]
    async fn get_log_entries(&self, start: u64, stop: u64) -> Result<Vec<Entry<RaftRequest>>> {
        let handle = self.get_rlr_handle().await?;
        let log = handle.read().await;

        log.iterator(rocksdb::IteratorMode::From(
            &start.to_le_bytes(),
            rocksdb::Direction::Forward,
        ))
        .map(|(key, value)| {
            bincode::deserialize::<Entry<RaftRequest>>(value.as_ref()).map_err(|e| {
                anyhow::anyhow!("Error deserializing log DB for key {:x?}: {}", key, e)
            })
        })
        .collect::<Result<Vec<Entry<RaftRequest>>>>()
    }

    #[tracing::instrument(skip(self))]
    async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> Result<()> {
        let handle = self.get_rlr_handle().await?;
        let log = handle.read().await;

        log.delete_range_cf(
            log.cf_handle("default").unwrap(),
            &start.to_le_bytes(),
            &stop.unwrap_or(u64::MAX).to_le_bytes(),
        )?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn append_entry_to_log(&self, entry: &Entry<RaftRequest>) -> Result<()> {
        let handle = self.get_rlr_handle().await?;
        let log = handle.read().await;

        log.put(entry.index.to_le_bytes(), bincode::serialize(entry)?)?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn replicate_to_log(&self, entries: &[Entry<RaftRequest>]) -> Result<()> {
        for entry in entries {
            self.append_entry_to_log(entry).await?;
        }

        Ok(())
    }

    // FIXME: Update last_applied_entry of StateMachine
    #[tracing::instrument(skip(self))]
    async fn apply_entry_to_state_machine(
        &self,
        index: &u64,
        data: &RaftRequest,
    ) -> Result<RaftResponse> {
        match data {
            RaftRequest::ConsoleNetworkRequest(cnr) => match cnr {
                ConsoleNetworkRequest::NewCoreConfiguration(configuration) => {
                    anyhow::bail!("Unimplemented")
                }
                ConsoleNetworkRequest::UpdateSharedConfig {
                    name,
                    configuration,
                } => {
                    anyhow::bail!("Unimplemented")
                }
                ConsoleNetworkRequest::AddPeer {
                    address,
                    add_to_core,
                } => {
                    anyhow::bail!("Unimplemented")
                }
                ConsoleNetworkRequest::Bootstrap => {
                    anyhow::bail!("Unimplemented")
                }
                ConsoleNetworkRequest::Shutdown(id) => {
                    anyhow::bail!("Unimplemented")
                }
                ConsoleNetworkRequest::SoftShutdown(id) => {
                    anyhow::bail!("Unimplemented")
                }
                ConsoleNetworkRequest::UpdateInstance {
                    manager,
                    instance_name,
                } => {
                    anyhow::bail!("Unimplemented")
                }
            },
            RaftRequest::InstanceNetworkRequest(inr) => match inr {
                InstanceNetworkRequest::SpawnChild {
                    request_id,
                    instance_name,
                    with_args,
                    notify,
                } => {
                    anyhow::bail!("Unimplemented")
                }
                InstanceNetworkRequest::DropSelf(uuid) => {
                    anyhow::bail!("Unimplemented")
                }
                InstanceNetworkRequest::UpdateRelationships(()) => {
                    anyhow::bail!("Unimplemented")
                }
            },
            RaftRequest::ManagerNetworkRequest(mnr) => match mnr {
                ManagerNetworkRequest::UpdateServerHealth {
                    manager_id,
                    ports_free,
                    cpu_use_as_decimal_fraction,
                    ram_use_as_decimal_fraction,
                    ram_free_in_mb,
                } => {
                    anyhow::bail!("Unimplemented")
                }
                ManagerNetworkRequest::ShareInstanceStore(map) => {
                    anyhow::bail!("Unimplemented")
                }
            },
        }
    }

    #[tracing::instrument(skip(self))]
    async fn replicate_to_state_machine(&self, entries: &[(&u64, &RaftRequest)]) -> Result<()> {
        // TODO: More efficient to compact first?
        for (index, request) in entries {
            match self.apply_entry_to_state_machine(index, request).await {
                Ok(_) => {}
                Err(e) if e.is::<RaftStorageError>() => anyhow::bail!(
                    "The error replicating entry index {} to state maching",
                    index
                ),
                Err(_) => {}
            };
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn do_log_compaction(&self) -> Result<CurrentSnapshotData<Self::Snapshot>> {
        let handle = self.get_rlr_handle().await?;
        let log = handle.read().await;

        let index = self.get_last_applied_entry().await?.unwrap_or(0);
        let membership = self.get_membership_config().await?;
        let entry = log
            .get(&index.to_le_bytes())
            .map_err(|e| anyhow::anyhow!("Error fetching last applied entry: {}", e))?
            .ok_or_else(|| anyhow::anyhow!("No key with last applied entry's index"))?;
        let entry = bincode::deserialize::<Entry<RaftRequest>>(entry.as_ref())?;
        let term = entry.term;

        let snapshot = {
            // Read from temporary state and distributed conf, write to file
            // Insert snapshot pointer into distributed conf
            let mut file = tokio::fs::File::create(format!(
                "{}--{}.complog",
                self.self_raft_id,
                chrono::Utc::now().format("%Y%m%d%H%M%S"),
            ))
            .await?;

            let transaction = self.get_dsr_transaction().await?;
            let all_vertices_query = RangeVertexQuery::new().limit(u32::MAX);
            let all_vertices_and_properties = transaction
                .get_all_vertex_properties(all_vertices_query.clone())
                .map_err(|e| anyhow::anyhow!("Error fetching all verticies for log compaction"))?;

            let mut snapshot_vertices = Vec::new();
            let mut snapshot_edges = Vec::new();

            for vertex_and_properties in all_vertices_and_properties {
                snapshot_vertices.push(SnapshotVertex {
                    id: vertex_and_properties.vertex.id,
                    t: vertex_and_properties.vertex.t,
                    properties: vertex_and_properties
                        .props
                        .iter()
                        .map(|np| (np.name, np.value))
                        .collect(),
                });

                let outbound_edges_query =
                    SpecificVertexQuery::single(vertex_and_properties.vertex.id).outbound();

                let outbound_edges_and_properties = transaction
                    .get_all_edge_properties(outbound_edges_query)
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "Error fetching outbound edges for {}: {}",
                            vertex_and_properties.vertex.id,
                            e
                        )
                    })?;

                for edge_and_properties in outbound_edges_and_properties {
                    snapshot_edges.push(SnapshotEdge {
                        key: edge_and_properties.edge.key,
                        created_datetime: edge_and_properties.edge.created_datetime,
                        properties: edge_and_properties
                            .props
                            .iter()
                            .map(|np| (np.name, np.value))
                            .collect(),
                    });
                }
            }

            file.write_all(&bincode::serialize(&SnapshotFile {
                vertices: snapshot_vertices,
                edges: snapshot_edges,
            })?);

            Box::new(file)
        };

        Ok(CurrentSnapshotData {
            term,
            index,
            membership,
            snapshot,
        })
    }

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

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct SnapshotFile {
    vertices: Vec<SnapshotVertex>,
    edges: Vec<SnapshotEdge>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct SnapshotVertex {
    id: Uuid,
    t: Type,
    properties: HashMap<String, serde_json::Value>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct SnapshotEdge {
    key: EdgeKey,
    created_datetime: chrono::DateTime<chrono::Utc>,
    properties: HashMap<String, serde_json::Value>,
}
