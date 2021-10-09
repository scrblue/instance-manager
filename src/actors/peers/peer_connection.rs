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
    self_id: u64,
    mut core_config_matches: Option<bool>,
) {
    // With every request, a oneshot::Sender is sent through which the response is to be sent;
    // this maps the request ID to the transmitter so when the response arrives, it can be directed
    // to the right location
    let mut unfulfilled_requests: HashMap<u64, oneshot::Sender<ManagerManagerResponse>> =
        HashMap::new();

    // TODO: From/Into for ManagerManagerRequest -> ManagerManagerPayload and ManagerManagerResponse
    // -> ManagerManagerPayload
    let from_tracker = &mut from_tracker;
    let connection = &mut connection;
    loop {
        tokio::select! {
            msg = from_tracker.recv() => {
                match msg {
                    Some(FromTracker::Shutdown) => break,

                    // Upon receiving a request from the PeerTracker, insert the Sender into the
                    // unfulfilled_requests map, then send the message to the peer that this thread
                    // manages
                    Some(FromTracker::Request(mmr, tx)) => {
                        unfulfilled_requests.insert(mmr.id, tx);

                        tracing::trace!("Sending message {:?}", mmr);
                        let mmr = InstanceManagerMessage {
                            id: mmr.id,
                            payload: ManagerManagerPayload::Request(mmr.payload),
                        };
                        connection.send_message(&mmr).await.unwrap();
                    }

                    // Upon receiving a response to the PeerTracker, simply send the message to the
                    // peer
                    Some(FromTracker::Response(mmr)) => {
                        tracing::trace!("Sending message {:?}", mmr);
                        let mmr = InstanceManagerMessage {
                            id: mmr.id,
                            payload: ManagerManagerPayload::Response(mmr.payload),
                        };
                        connection.send_message(&mmr).await.unwrap();
                    }

                    // The following two messages from the PeerTracker indicate whether or not this
                    // connection is from a valid peer
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
                    // If the CoreConfig does match, requests and responses can be handled as
                    // expected
                    Some(true) => match msg {

                        // Upon receiving a response from the peer, fetch the Sender from the
                        // unfulfilled_requests map corresponding to the request ID, and send
                        // the response through that
                        Ok(InstanceManagerMessage {
                            id,
                            payload: ManagerManagerPayload::Response(resp),
                        }) =>  {
                            tracing::trace!(
                                "Received response from connection for request {}: {:?}",
                                id,
                                resp,
                            );

                            if let Some(tx) = unfulfilled_requests.remove(&id) {
                                tx.send(resp).unwrap();
                            }
                        }

                        // Upon receiving a request, simply forward it to the PeerTracker
                        Ok(InstanceManagerMessage {
                            id,
                            payload: ManagerManagerPayload::Request(req),
                        }) => {
                            tracing::trace!("Received from connection: {:?}", req);
                            to_tracker.send(
                                (self_id, ToTracker::Request(
                                    InstanceManagerMessage {id, payload: req}))).await.unwrap();
                        }

                        Err(e) => {
                            tracing::error!("Error receiving message from peer: {}", e);
                            break;
                        }
                    },

                    // If the CoreConfig does not match, then the peer is not allowed to enter the
                    // cluster and thus no requests or responses should be accepted
                    Some(false) => {
                        connection.send_message(&ManagerManagerResponse::ConnectionDenied).await.unwrap();
                        // TODO: Let retry with new CoreConfig?
                    },

                    // If the CoreConfig has not been compared yet, only forward the request if it
                    // is a CompareCoreConfig request, otherwise send a response stating that the
                    // CoreConfig must be compared
                    None => {
                        if let Ok(InstanceManagerMessage {
                            id: msg_id,
                            payload: ManagerManagerPayload::Request(
                                ManagerManagerRequest::CompareCoreConfig(cc))
                        }) = msg {
                            tracing::trace!("Received CompareCoreConfig from peer");
                            to_tracker.send((
                                self_id,
                                ToTracker::Request(
                                    InstanceManagerMessage {
                                        id: msg_id,
                                        payload:
                                            ManagerManagerRequest::CompareCoreConfig(cc)
                                    }
                                ))).await.unwrap();
                        } else {
                             //connection.send_message(
                               	//&InstanceManagerMessage {
                                    // id: 0,
                                    // payload: ManagerManagerPayload::Response(
                                    // 	ManagerManagerResponse::RequireCoreConfig
                                    // )
                               //	}
                             //).await.unwrap();
                        }
                    }
                }
            },
        }
    }
}
