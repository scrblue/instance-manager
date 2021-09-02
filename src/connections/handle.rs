use super::manager::*;

use anyhow::Result;
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Debug)]
pub struct ConnectionManagerHandle {
    pub(super) sender: mpsc::Sender<(Request, oneshot::Sender<Response>)>,
}

impl ConnectionManagerHandle {
    pub async fn shutdown(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender.send((Request::Shutdown, tx)).await?;

        match rx.await? {
            Response::Ok => Ok(()),
            r => anyhow::bail!("Unexepeted response: {:?}", r),
        }
    }
}
