use crate::messages::{ConsoleManagerRequest, ConsoleNetworkRequest};

use std::io::{stdin, Read};
use tokio::sync::mpsc;

#[derive(Debug, PartialEq)]
pub enum IoToMain {
    ProcessStarted,
    NetworkRequest(ConsoleNetworkRequest),
    ManagerRequest(ConsoleManagerRequest),
}

#[tracing::instrument]
pub async fn handle_io(to_main: mpsc::Sender<IoToMain>) {
    to_main.send(IoToMain::ProcessStarted).await.unwrap();

    let mut command = String::new();
    loop {
        match stdin().read_line(&mut command) {
            Ok(_) => match command.to_lowercase().as_str().trim() {
                "quit" | "stop" => {
                    to_main
                        .send(IoToMain::NetworkRequest(ConsoleNetworkRequest::Shutdown(1)))
                        .await
                        .unwrap();
                    break;
                }

                other => {
                    tracing::error!("Unrecognized command: {}", other);
                }
            },
            Err(e) => {
                tracing::error!("Error reading standard in: {:?}", e);
            }
        }
    }
}
