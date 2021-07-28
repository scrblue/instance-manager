use crate::messages::ManagerManagerRequest;

use anyhow::Result;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::mpsc;

#[derive(Clone, Debug, PartialEq)]
pub enum PeerTrackerToMain {
    PeerNotification(ManagerManagerRequest),
}
use PeerTrackerToMain as ToMain;

#[derive(Clone, Debug, PartialEq)]
pub enum MainToPeerTracker {
    NewPeer(usize, SocketAddr),
    Shutdown,
}
use MainToPeerTracker as FromMain;

#[tracing::instrument(skip(to_main, from_main, state_handle))]
pub async fn track_peers(
    to_main: mpsc::Sender<ToMain>,
    mut from_main: mpsc::Receiver<FromMain>,
    state_handle: Arc<indradb::MemoryDatastore>,
) -> Result<()> {
    loop {
        match from_main.recv().await {
            Some(FromMain::Shutdown) => {
                tracing::debug!("Shutting down peer tracker gracefully");
                break;
            }

            None => {
                tracing::error!("Main thread channel closed");
                break;
            }

            _ => {}
        }
    }

    Ok(())
}
