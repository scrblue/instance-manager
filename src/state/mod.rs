#[cfg(test)]
mod tests;

use crate::messages::{
    ConsoleNetworkRequest, ConsoleNetworkResponse, InstanceNetworkRequest, InstanceNetworkResponse,
    ManagerNetworkRequest, RaftRequest, RaftRequestPayload, RaftResponse,
};

use anyhow::{Context, Result};
use async_raft::{async_trait::async_trait, raft::*, storage::*, RaftStorage};
use indradb::{
    BulkInsertItem, Datastore, EdgeKey, EdgeQuery, EdgeQueryExt, MemoryDatastore,
    MemoryTransaction, RangeVertexQuery, RocksdbDatastore, RocksdbTransaction, SpecificVertexQuery,
    Transaction, Type, Vertex, VertexPropertyQuery, VertexQuery, VertexQueryExt,
};
use rocksdb::DB;
use serde_json::{value, Value};
use std::{
    collections::hash_map::HashMap, convert::TryInto, error::Error, net::SocketAddr, path::PathBuf,
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, oneshot},
};
use uuid::Uuid;

#[derive(Debug, Copy, Clone)]
pub struct RaftStorageError;

impl std::fmt::Display for RaftStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Raft storage error")
    }
}
impl Error for RaftStorageError {}

pub struct StateManager {
    self_raft_id: u64,
    snapshot_dir: PathBuf,
    raft_log_db: DB,
    distributed_state: RocksdbDatastore,
    state_machine_uuid: Uuid,
}

impl StateManager {
    #[tracing::instrument]
    pub fn new(
        self_raft_id: u64,
        distributed_path: PathBuf,
        log_path: PathBuf,
        snapshot_dir: PathBuf,
    ) -> Result<StateManager> {
        let mut raft_log_db = DB::open_default(log_path)?;

        let distributed_state = indradb::RocksdbDatastore::new(distributed_path, None)
            .map_err(|e| anyhow::anyhow!("{:?}", e))?;

        let transaction = distributed_state
            .transaction()
            .map_err(|e| anyhow::anyhow!("Error getting Transaction: {}", e))?;

        let sm_type = Type::new("StateMachine")?;

        let mut vertex = transaction
            .get_vertices(RangeVertexQuery::new().limit(1).t(sm_type.clone()))
            .map_err(|e| anyhow::anyhow!("Error checking for StateMachine: {}", e))?;

        let state_machine_uuid = if let Some(vertex) = vertex.pop() {
            vertex.id
        } else {
            transaction
                .create_vertex_from_type(sm_type)
                .map_err(|e| anyhow::anyhow!("Error creating StateMachine in DB: {}", e))?
        };

        distributed_state
            .sync()
            .map_err(|e| anyhow::anyhow!("Error syncing DB while creating StateManager: {}", e))?;

        Ok(StateManager {
            self_raft_id,
            raft_log_db,
            distributed_state,
            snapshot_dir,
            state_machine_uuid,
        })
    }

    #[tracing::instrument(skip(self))]
    async fn get_hard_state(&self) -> Result<HardState> {
        let transaction = self
            .distributed_state
            .transaction()
            .map_err(|e| anyhow::anyhow!("Error fetching hard state: {}", e))?;

        let json = transaction
            .get_vertex_properties(VertexPropertyQuery::new(
                RangeVertexQuery::new()
                    .limit(1)
                    .t(Type::new("StateMachine")?)
                    .into(),
                "hard_state",
            ))
            .map_err(|e| anyhow::anyhow!("Error fetching StateMachine: {}", e))?
            .pop();

        if let Some(json) = json {
            let parsed = serde_json::from_value(json.value)?;
            Ok(parsed)
        } else {
            Ok(HardState {
                current_term: 0,
                voted_for: None,
            })
        }
    }

    #[tracing::instrument(skip(self))]
    async fn get_last_applied_entry(&self) -> Result<u64> {
        let transaction = self
            .distributed_state
            .transaction()
            .map_err(|e| anyhow::anyhow!("Error fetching last applied entry: {}", e))?;

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
            Ok(0)
        }
    }
}

#[async_trait]
impl RaftStorage<RaftRequest, RaftResponse> for StateManager {
    type Snapshot = File;
    type ShutdownError = RaftStorageError;

    #[tracing::instrument(skip(self))]
    async fn get_membership_config(&self) -> Result<MembershipConfig> {
        tracing::trace!("Entering get_membership_config");
        let log = &self.raft_log_db;

        let membership = log
            .iterator(rocksdb::IteratorMode::End)
            .find_map(|(key, value)| {
                // FIXME: Remove these once you know how sorting works
                tracing::trace!("Iterated through entry with key {:?}", key);
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
                    tracing::trace!("Invalid value for key {:?} in Raft log DB", key);
                    None
                }
            });

        Ok(membership.unwrap_or(MembershipConfig::new_initial(self.self_raft_id)))
    }

    // FIXME: Initialize distributed DB in here if it doesn't exist
    #[tracing::instrument(skip(self))]
    async fn get_initial_state(&self) -> Result<InitialState> {
        tracing::trace!("Entering get_initial_state");
        let log = &self.raft_log_db;

        let mut membership = None;
        let mut last_log_index = None;
        let mut last_log_term = None;

        for (_key, value) in log.iterator(rocksdb::IteratorMode::End) {
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
                last_applied_log,
                hard_state,
                membership: membership.unwrap(),
            })
        }
    }

    #[tracing::instrument(skip(self))]
    async fn save_hard_state(&self, hs: &HardState) -> Result<()> {
        tracing::trace!("Entering save_hard_state");
        let transaction = self
            .distributed_state
            .transaction()
            .map_err(|e| anyhow::anyhow!("Could not fetch Transaction to save HardState: {}", e))?;
        tracing::trace!("Transaction for distributed state acquired");

        transaction
            .set_vertex_properties(
                VertexPropertyQuery::new(
                    RangeVertexQuery::new()
                        .limit(1)
                        .t(Type::new("StateMachine")?)
                        .into(),
                    "hard_state",
                ),
                &serde_json::json!(hs),
            )
            .map_err(|e| anyhow::anyhow!("Error saving HardState: {}", e))?;

        self.distributed_state
            .sync()
            .map_err(|e| anyhow::anyhow!("Could not save hard state: {}", e))?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn get_log_entries(&self, start: u64, stop: u64) -> Result<Vec<Entry<RaftRequest>>> {
        tracing::trace!("Entering get_log_entries");
        let log = &self.raft_log_db;

        let mut requests = Vec::new();

        for (key, value) in log.iterator(rocksdb::IteratorMode::From(
            &start.to_le_bytes(),
            rocksdb::Direction::Forward,
        )) {
            requests.push(
                bincode::deserialize::<Entry<RaftRequest>>(value.as_ref()).map_err(|e| {
                    anyhow::anyhow!("Error deserializing log DB for key {:x?}: {}", key, e)
                })?,
            );

            // TODO: Find a more elegant way to do this :)
            if let Some(request) = requests.last() {
                if request.index == stop - 1 {
                    break;
                }
            } else {
                break;
            }
        }

        Ok(requests)
    }

    #[tracing::instrument(skip(self))]
    async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> Result<()> {
        tracing::trace!("Entering delete_logs_from");
        let log = &self.raft_log_db;

        // Find last entry
        let stop = if let Some(stop) = stop {
            stop
        } else if let Some((key, _entry)) = log.iterator(rocksdb::IteratorMode::End).next() {
            let key: [u8; 8] = key[0..8].try_into()?;
            u64::from_le_bytes(key) + 1
        } else {
            0
        };

        // Find first entry to reduce unnecessary iterations
        let start = if let Some((key, _entry)) = log.iterator(rocksdb::IteratorMode::Start).next() {
            let key: [u8; 8] = key[0..8].try_into()?;
            let key = u64::from_le_bytes(key);
            std::cmp::max(key, start)
        } else {
            start
        };

        for index in start..stop {
            log.delete(index.to_le_bytes())?;
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn append_entry_to_log(&self, entry: &Entry<RaftRequest>) -> Result<()> {
        tracing::trace!("Entering append_entry_to_log");
        let log = &self.raft_log_db;

        log.put(entry.index.to_le_bytes(), bincode::serialize(entry)?)?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn replicate_to_log(&self, entries: &[Entry<RaftRequest>]) -> Result<()> {
        tracing::trace!("Entering replicate_to_log");
        let log = &self.raft_log_db;

        for entry in entries {
            log.put(entry.index.to_le_bytes(), bincode::serialize(entry)?)?;
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
        tracing::trace!("StateHandle received request: {:?}", data);
        let ret = match &data.payload {
            RaftRequestPayload::ConsoleNetworkRequest(cnr) => match cnr {
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
                    // FIXME: Only reason this isn't the same is so the testing of entry application works
                    RaftResponse::ConsoleNetworkResponse(ConsoleNetworkResponse::Success)
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
            RaftRequestPayload::InstanceNetworkRequest(inr) => match inr {
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
            RaftRequestPayload::ManagerNetworkRequest(mnr) => match mnr {
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
        };

        let transaction = self
            .distributed_state
            .transaction()
            .map_err(|e| anyhow::anyhow!("Could not fetch Transaction applying entry: {}", e))?;

        transaction
            .set_vertex_properties(
                VertexPropertyQuery::new(
                    SpecificVertexQuery::single(self.state_machine_uuid).into(),
                    "last_applied_entry",
                ),
                &serde_json::json!(index),
            )
            .map_err(|e| {
                anyhow::anyhow!(
                    "Could not change last_applied_entry while applying entry: {}",
                    e
                )
            })?;

        Ok(ret)
    }

    #[tracing::instrument(skip(self))]
    async fn replicate_to_state_machine(&self, entries: &[(&u64, &RaftRequest)]) -> Result<()> {
        tracing::trace!("Entering replicate_to_state_machine");

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
        tracing::trace!("Entering do_log_compaction");
        let log = &self.raft_log_db;

        let mut file_name = self.snapshot_dir.clone();
        file_name.push(format!(
            "{}--{}.complog",
            self.self_raft_id,
            chrono::Utc::now().format("%Y%m%d%H%M%S")
        ));

        let index = self.get_last_applied_entry().await?;

        let membership = log
            .iterator(rocksdb::IteratorMode::From(
                &index.to_le_bytes(),
                rocksdb::Direction::Reverse,
            ))
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
            })
            .ok_or_else(|| anyhow::anyhow!("No membership before last applied entry"))?;

        let entry = log
            .get(&index.to_le_bytes())
            .map_err(|e| anyhow::anyhow!("Error fetching last applied entry: {}", e))?
            .ok_or_else(|| anyhow::anyhow!("No key with last applied entry's index"))?;
        let entry = bincode::deserialize::<Entry<RaftRequest>>(entry.as_ref())?;
        let term = entry.term;

        let snapshot = {
            // Read from temporary state and distributed conf, write to file
            // FIXME: Use directory specified in config
            let mut file = tokio::fs::File::create(file_name.clone()).await?;

            let transaction = self.distributed_state.transaction().map_err(|e| {
                anyhow::anyhow!("Error fetching Transaction in do_log_compaction: {}", e)
            })?;
            let all_vertices_query = RangeVertexQuery::new().limit(u32::MAX);

            let all_vertices_and_properties = transaction
                .get_all_vertex_properties(all_vertices_query)
                .map_err(|e| {
                    anyhow::anyhow!("Error fetching all verticies for log compaction: {}", e)
                })?;

            let mut snapshot_vertices = Vec::new();
            let mut snapshot_edges = Vec::new();

            for vertex_and_properties in all_vertices_and_properties {
                snapshot_vertices.push(SnapshotVertex {
                    id: vertex_and_properties.vertex.id,
                    t: vertex_and_properties.vertex.t.clone(),
                    properties: vertex_and_properties
                        .props
                        .iter()
                        .map(|np| (np.name.clone(), np.value.clone()))
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
                            .map(|np| (np.name.clone(), np.value.clone()))
                            .collect(),
                    });
                }
            }

            file.write_all(&serde_json::ser::to_vec(&SnapshotFile {
                vertices: snapshot_vertices,
                edges: snapshot_edges,
                membership: membership.clone(),
            })?)
            .await?;

            file.flush().await?;
            file.sync_all().await?;
            std::mem::drop(file);

            // Open in read mode instead of write mode
            let file = File::open(file_name.clone()).await?;
            Box::new(file)
        };

        self.append_entry_to_log(&Entry::new_snapshot_pointer(
            index,
            term,
            file_name.as_os_str().to_str().unwrap().to_owned(),
            membership.clone(),
        ))
        .await?;

        self.delete_logs_from(0, Some(index)).await?;

        Ok(CurrentSnapshotData {
            term,
            index,
            membership,
            snapshot,
        })
    }

    #[tracing::instrument(skip(self))]
    async fn create_snapshot(&self) -> Result<(String, Box<Self::Snapshot>)> {
        tracing::trace!("Entering create_snapshot");
        let file_name = format!(
            "{}--{}.complog",
            self.self_raft_id,
            chrono::Utc::now().format("%Y%m%d%H%M%S"),
        );

        let mut file_path = self.snapshot_dir.clone();
        file_path.push(&file_name);

        let file = tokio::fs::File::create(file_path).await?;
        file.sync_all().await?;

        Ok((file_name, Box::new(file)))
    }

    #[tracing::instrument(skip(self))]
    async fn finalize_snapshot_installation(
        &self,
        index: u64,
        term: u64,
        delete_through: Option<u64>,
        id: String,
        mut snapshot: Box<Self::Snapshot>,
    ) -> Result<()> {
        tracing::trace!("Entering finalize_snapshot_installation");

        // FIXME: Clear DB here
        snapshot.flush().await?;
        snapshot.sync_all().await?;
        std::mem::drop(snapshot);

        // let mut snapshot = File::open(id.clone()).await?;

        // let mut buffer = Vec::new();
        // snapshot.read_to_end(&mut buffer).await.context("Error reading snapshot")?;

        let mut file_path = self.snapshot_dir.clone();
        file_path.push(&id);
        let buffer = tokio::fs::read(file_path)
            .await
            .context("Error reading snapshot")?;
        let file: SnapshotFile =
            serde_json::de::from_slice(&buffer).context("Error deserializing snapshot")?;

        let mut insertion = Vec::new();

        for snapshot_vertex in file.vertices {
            let id = snapshot_vertex.id;
            insertion.push(BulkInsertItem::Vertex(Vertex::with_id(
                id,
                snapshot_vertex.t,
            )));

            for (name, value) in snapshot_vertex.properties {
                insertion.push(BulkInsertItem::VertexProperty(id, name, value));
            }
        }

        for snapshot_edge in file.edges {
            let edge_key = EdgeKey::new(
                snapshot_edge.key.outbound_id,
                snapshot_edge.key.t,
                snapshot_edge.key.inbound_id,
            );

            insertion.push(BulkInsertItem::Edge(edge_key.clone()));

            for (name, value) in snapshot_edge.properties {
                insertion.push(BulkInsertItem::EdgeProperty(edge_key.clone(), name, value));
            }
        }

        self.distributed_state
            .bulk_insert(insertion.drain(..))
            .map_err(|e| {
                anyhow::anyhow!(
                    "Error with bulk insert in finalize_snapshot_installation: {}",
                    e
                )
            })?;
        self.distributed_state.sync().map_err(|e| {
            anyhow::anyhow!(
                "Error syncing database in finalize_snapshot_installation: {}",
                e
            )
        })?;

        self.delete_logs_from(0, delete_through).await?;

        // Insert SnapshotPointer
        self.append_entry_to_log(&Entry {
            index,
            term,
            payload: EntryPayload::SnapshotPointer(EntrySnapshotPointer {
                id,
                membership: file.membership,
            }),
        })
        .await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn get_current_snapshot(&self) -> Result<Option<CurrentSnapshotData<Self::Snapshot>>> {
        tracing::trace!("Entering get_current_snapshot");
        let log = &self.raft_log_db;

        let option_term_index_pointer =
            log.iterator(rocksdb::IteratorMode::End)
                .find_map(|(_key, value)| {
                    let entry = bincode::deserialize::<Entry<RaftRequest>>(value.as_ref())
                        .map(|entry| Some(entry))
                        .unwrap_or(None);

                    if let Some(entry) = entry {
                        match entry.payload {
                            EntryPayload::SnapshotPointer(sp) => {
                                Some((entry.term, entry.index, sp))
                            }
                            _ => None,
                        }
                    } else {
                        None
                    }
                });

        if let Some((term, index, pointer)) = option_term_index_pointer {
            let snapshot = Box::new(tokio::fs::File::open(pointer.id).await?);
            Ok(Some(CurrentSnapshotData {
                term,
                index,
                membership: pointer.membership,
                snapshot,
            }))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct SnapshotFile {
    vertices: Vec<SnapshotVertex>,
    edges: Vec<SnapshotEdge>,
    membership: MembershipConfig,
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
