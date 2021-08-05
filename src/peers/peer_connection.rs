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
    id: usize,
) {
    loop {
        tokio::select! {
            msg = from_tracker.recv() => {
                match msg {
                    Some(FromTracker::Shutdown) => break,
                    msg => tracing::info!("Received message from peer tracker: {:?}", msg),
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
                    }
                }
            },
        }
    }
}
