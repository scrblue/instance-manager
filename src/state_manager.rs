use anyhow::Result;
use indradb::{
    Datastore, EdgeKey, MemoryDatastore, MemoryTransaction, RangeVertexQuery, RocksdbDatastore,
    RocksdbTransaction, SpecificVertexQuery, Transaction, Type, Vertex, VertexPropertyQuery,
};
use serde_json::{value, Value};
use std::{net::SocketAddr, path::PathBuf};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

#[derive(Clone)]
pub struct StateHandle {
    dcr_sender: mpsc::Sender<oneshot::Sender<RocksdbTransaction>>,
    tsr_sender: mpsc::Sender<oneshot::Sender<MemoryTransaction>>,

    sdr_sender: mpsc::Sender<()>,
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
}

pub struct StateManager {
    distributed_conf: RocksdbDatastore,
    distributed_conf_request_receiver: mpsc::Receiver<oneshot::Sender<RocksdbTransaction>>,
    temporary_state: MemoryDatastore,
    temporary_state_request_receiver: mpsc::Receiver<oneshot::Sender<MemoryTransaction>>,

    shutdown_request_receiver: mpsc::Receiver<()>,

    // The senders to be cloned when spawning a StateHandle
    dcr_sender: mpsc::Sender<oneshot::Sender<RocksdbTransaction>>,
    tsr_sender: mpsc::Sender<oneshot::Sender<MemoryTransaction>>,
    sdr_sender: mpsc::Sender<()>,
}

impl StateManager {
    #[tracing::instrument]
    pub fn new(distributed_path: PathBuf, temporary_path: PathBuf) -> Result<StateManager> {
        let distributed_conf = indradb::RocksdbDatastore::new(distributed_path, None)
            .map_err(|e| anyhow::anyhow!("{:?}", e))?;

        let temporary_state = indradb::MemoryDatastore::create(temporary_path)?;

        let (dcr_sender, distributed_conf_request_receiver) = mpsc::channel(128);
        let (tsr_sender, temporary_state_request_receiver) = mpsc::channel(128);
        let (sdr_sender, shutdown_request_receiver) = mpsc::channel(128);

        Ok(StateManager {
            distributed_conf,
            distributed_conf_request_receiver,
            temporary_state,
            temporary_state_request_receiver,
            shutdown_request_receiver,
            dcr_sender,
            tsr_sender,
            sdr_sender,
        })
    }

    /// Returns a StateHandle to be used to interact with the StateManager after it has been spawned
    pub fn handle(&self) -> StateHandle {
        StateHandle {
            dcr_sender: self.dcr_sender.clone(),
            tsr_sender: self.tsr_sender.clone(),
            sdr_sender: self.sdr_sender.clone(),
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
