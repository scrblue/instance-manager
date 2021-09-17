use std::collections::HashMap;
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
};
use tokio_tls::TlsConnection;

use super::ConnectionToTracker as ToTracker;
use super::TrackerToConnection as FromTracker;
use crate::messages::{
    InstanceManagerMessage, ManagerManagerPayload, ManagerManagerRequest, ManagerManagerResponse,
};

#[tracing::instrument(skip(to_tracker, from_tracker, connection))]
pub async fn handle_peer_connection(
    to_tracker: mpsc::Sender<(u64, ToTracker)>,
    mut from_tracker: mpsc::Receiver<FromTracker>,
    mut connection: TlsConnection<TcpStream>,
    id: u64,
    mut core_config_matches: Option<bool>,
) {
    let mut unfulfilled_requests: HashMap<u64, oneshot::Sender<ManagerManagerResponse>> =
        HashMap::new();

    // TODO: From/Into for ManagerManagerRequest -> ManagerManagerPayload and ManagerManagerResponse
    // -> ManagerManagerPayload
    loop {
        tokio::select! {
            msg = from_tracker.recv() => {
                match msg {
                    Some(FromTracker::Shutdown) => break,
                    Some(FromTracker::Request(mmr, tx)) => {
                        unfulfilled_requests.insert(mmr.id, tx);

                        tracing::debug!("Sending message {:?}", mmr);
                        let mmr = InstanceManagerMessage {
                            id: mmr.id,
                            payload: ManagerManagerPayload::Request(mmr.payload),
                        };
                        connection.send_message(&mmr).await.unwrap();
                    }
                    Some(FromTracker::Response(mmr)) => {
                        tracing::debug!("Sending message {:?}", mmr);
                        let mmr = InstanceManagerMessage {
                            id: mmr.id,
                            payload: ManagerManagerPayload::Response(mmr.payload),
                        };
                        connection.send_message(&mmr).await.unwrap();
                    }
                    Some(FromTracker::CoreConfigMatch(id)) => {
                        core_config_matches = Some(true);
                        connection.send_message(&InstanceManagerMessage{
                            id,
                            payload: ManagerManagerPayload::Response(
                                ManagerManagerResponse::ConnectionAccepted
                            )
                        }).await.unwrap();
                    }
                    Some(FromTracker::CoreConfigNoMatch(id)) => {
                        core_config_matches = Some(false);
                        connection.send_message(&InstanceManagerMessage{
                            id,
                            payload: ManagerManagerPayload::Response(
                                ManagerManagerResponse::ConnectionDenied
                            )
                        }).await.unwrap();
                        // TODO: break here?
                    }
                    None => {
                        tracing::error!("Channel from tracker is closed");
                        break;
                    },
                };
            },
            msg = connection.read_message::<InstanceManagerMessage<ManagerManagerPayload>>() => {
                match core_config_matches {
                    Some(true) => match msg {
                        Ok(InstanceManagerMessage {
                            id,
                            payload: ManagerManagerPayload::Response(resp),
                        }) =>  {
                            tracing::info!(
                                "Received response from connection for request {}: {:?}",
                                id,
                                resp,
                            );

                            if let Some(tx) = unfulfilled_requests.remove(&id) {
                                tx.send(resp).unwrap();
                            } else {
                                to_tracker.send((id, ToTracker::Response(
                                    InstanceManagerMessage {
                                        id,
                                        payload: resp,
                                    }
                                ))).await.unwrap();
                            }
                        }

                        Ok(InstanceManagerMessage {
                            id,
                            payload: ManagerManagerPayload::Request(req),
                        }) => {
                            tracing::info!("Received from connection: {:?}", req);
                            to_tracker.send(
                                (id, ToTracker::Request(
                                    InstanceManagerMessage {id, payload: req}))).await.unwrap();
                        }

                        Err(e) => {
                            tracing::error!("Error receiving message from peer: {}", e);
                            break;
                        }
                    },

                    Some(false) => {
                        connection.send_message(&ManagerManagerResponse::ConnectionDenied).await.unwrap();
                        // TODO: Let retry with new CoreConfig?
                    },

                    None => {
                        if let Ok(InstanceManagerMessage {
                            id,
                            payload: ManagerManagerPayload::Request(
                                ManagerManagerRequest::CompareCoreConfig(cc))
                        }) = msg {
                            to_tracker.send((
                                id,
                                ToTracker::Request(
                                    InstanceManagerMessage {
                                        id,
                                        payload:
                                            ManagerManagerRequest::CompareCoreConfig(cc)
                                    }
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
