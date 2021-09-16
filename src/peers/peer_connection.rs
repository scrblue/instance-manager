use tokio::{net::TcpStream, sync::mpsc};
use tokio_tls::TlsConnection;

use super::ConnectionToTracker as ToTracker;
use super::TrackerToConnection as FromTracker;
use crate::messages::{ManagerManagerRequest, ManagerManagerResponse};

#[tracing::instrument(skip(to_tracker, from_tracker, connection))]
pub async fn handle_peer_connection(
    to_tracker: mpsc::Sender<(u64, ToTracker)>,
    mut from_tracker: mpsc::Receiver<FromTracker>,
    mut connection: TlsConnection<TcpStream>,
    id: u64,
    mut core_config_matches: Option<bool>,
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
                    Some(FromTracker::Response(mmr)) => {
                        tracing::debug!("Sending message {:?}", mmr);
                        connection.send_message(&mmr).await.unwrap();
                    }
                    Some(FromTracker::CoreConfigMatch) => {
                        core_config_matches = Some(true);
                        connection.send_message(&ManagerManagerResponse::ConnectionAccepted).await.unwrap();
                    }
                    Some(FromTracker::CoreConfigNoMatch) => {
                        core_config_matches = Some(false);
                        connection.send_message(&ManagerManagerResponse::ConnectionDenied).await.unwrap();
                        // TODO: break here?
                    }
                    None => {
                        tracing::error!("Channel from tracker is closed");
                        break;
                    },
                };
            },
            msg = connection.read_message::<ManagerManagerRequest>() => {
                match core_config_matches {
                    Some(true) => match msg {
                        Ok(msg) =>  {
                            tracing::info!("Received from connection: {:?}", msg);
                            to_tracker.send((id, ToTracker::Request(msg))).await.unwrap();
                        }
                        Err(e) => {
                            tracing::error!("Error receiving message from peer: {}", e);
                            break;
                        }
                    },

                    Some(false) => {
                        connection.send_message(&ManagerManagerResponse::ConnectionDenied).await.unwrap();
                    },

                    None => {
                        if let Ok(ManagerManagerRequest::CompareCoreConfig(cc)) = msg {
                            to_tracker.send((
                                id,
                                ToTracker::Request(
                                    ManagerManagerRequest::CompareCoreConfig(cc)
                                ))).await.unwrap();
                        } else {
                            connection.send_message(
                                &ManagerManagerResponse::RequireCoreConfig
                            ).await.unwrap();
                        }
                    }
                }
            },
        }
    }
}
