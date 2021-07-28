use anyhow::{anyhow, bail, Context, Result};
use rustls::{
    AllowAnyAuthenticatedClient, Certificate, ClientConfig, PrivateKey, RootCertStore,
    ServerConfig, TLSError,
};
use std::{
    io::{Read, Write},
    path::PathBuf,
    sync::Arc,
};
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
};
use tracing::instrument;

#[instrument]
pub async fn set_up_config(
    root_cert_location: &PathBuf,
    node_cert_location: Option<&PathBuf>,
    admn_cert_location: Option<&PathBuf>,
    server_certificate: &PathBuf,
    server_key: &PathBuf,
) -> Result<(ServerConfig, ClientConfig)> {
    tracing::trace!("Reading root CA certificate");
    let root_cert = read_certificate(root_cert_location).await?;

    let node_cert = if let Some(node_cert_location) = node_cert_location {
        tracing::trace!("Reading node CA certificate");
        Some(read_certificate(node_cert_location).await?)
    } else {
        None
    };

    let admn_cert = if let Some(admn_cert_location) = admn_cert_location {
        tracing::trace!("Reading admin CA certificate");
        Some(read_certificate(admn_cert_location).await?)
    } else {
        None
    };

    tracing::trace!("Creating RootCertStore");
    let mut cert_store = RootCertStore::empty();
    cert_store
        .add(&root_cert)
        .context("Error while parsing root CA certificate")?;

    if let Some(node_cert) = node_cert {
        cert_store
            .add(&node_cert)
            .context("Error while parsing node CA certificate")?;
    }

    if let Some(admn_cert) = admn_cert {
        cert_store
            .add(&admn_cert)
            .context("Error while parsing admin CA certificate")?;
    }

    let cert_verifier = AllowAnyAuthenticatedClient::new(cert_store.clone());

    let mut server = ServerConfig::new(cert_verifier);

    tracing::trace!("Reading root server certificate and key");
    let server_cert = read_certificate(server_certificate).await?;
    let server_key = read_key(server_key).await?;
    tracing::trace!("Configuring root server certificate and key");
    server.set_single_cert(vec![server_cert.clone()], server_key.clone())?;

    let mut client = ClientConfig::new();
    client.root_store = cert_store;
    client.set_single_client_cert(vec![server_cert], server_key)?;

    Ok((server, client))
}

#[instrument]
async fn read_certificate(path: &PathBuf) -> Result<Certificate> {
    // FIXME: Return error if path cannot be converted to &str
    let mut file = File::open(path).await.context(format!(
        "Error while opening key file {}",
        path.to_str().unwrap()
    ))?;

    let mut out = Vec::new();

    let _ = file.read_to_end(&mut out).await.context(format!(
        "Error while reading key file {}",
        path.to_str().unwrap(),
    ))?;

    Ok(Certificate(out))
}

#[instrument]
async fn read_key(path: &PathBuf) -> Result<PrivateKey> {
    // FIXME: Return error if path cannot be converted to &str
    let mut file = File::open(path).await.context(format!(
        "Error while opening key file {}",
        path.to_str().unwrap()
    ))?;

    let mut out = Vec::new();

    let _ = file.read_to_end(&mut out).await.context(format!(
        "Error while reading key file {}",
        path.to_str().unwrap(),
    ))?;

    Ok(PrivateKey(out))
}
