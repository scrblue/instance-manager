use tokio::{net::TcpStream, sync::mpsc};
use tokio_tls::TlsConnection;

use super::ConnectionToTracker as ToTracker;
use super::TrackerToConnection as FromTracker;
use crate::messages::ManagerManagerRequest;

#[tracing::instrument(skip(to_tracker, from_tracker, connection))]
pub async fn handle_peer_connection(
    to_tracker: mpsc::Sender<ToTracker>,
    mut from_tracker: mpsc::Receiver<FromTracker>,
    mut connection: TlsConnection<TcpStream>,
    id: u64,
) {
    loop {
        tokio::select! {
            msg = from_tracker.recv() => {
                match msg {
                    Some(FromTracker::Shutdown) => break,
                    Some(FromTracker::Request(mmr)) => {
                        tracing::debug!("Sending message {:?}", mmr);
                        connection.send_message(&mmr).await.unwrap();
                    }
                    None => {
                        tracing::error!("Channel from tracker is closed");
                        break;
                    },
                };
            },
            msg = connection.read_message::<ManagerManagerRequest>() => {
                match msg {
                    Ok(msg) =>  {
                        tracing::info!("Received from connection: {:?}", msg);
                        to_tracker.send(ToTracker::Request(msg)).await.unwrap();
                    }
                    Err(e) => {
                        tracing::error!("Error receiving message from peer: {}", e);
                        break;
                    }
                }
            },
        }
    }
}
