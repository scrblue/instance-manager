use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{net::{IpAddr, SocketAddr}, path::PathBuf};
use tokio::fs;
use tracing::instrument;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct CoreConfig {
    pub root_ca_cert: PathBuf,
    pub node_ca_cert: PathBuf,
    pub admin_ca_cert: PathBuf,
    pub peers: Vec<(u64, SocketAddr)>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalConfig {
    pub log_level: Option<String>,

    pub public_ip: IpAddr,
    pub port_bound: u16,

    pub server_public_key: PathBuf,
    pub server_private_key: PathBuf,
    pub server_key_password: Option<String>,

    pub shared_conf_db_path: PathBuf,
    pub log_file_path: PathBuf,
    pub snapshot_save_dir: PathBuf,

    pub is_core_member: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SettingsFile {
    #[serde(flatten)]
    local_config: LocalConfig,
    #[serde(flatten)]
    core_config: CoreConfig,
}

/// Read the configuration file specified as a command line argument
#[instrument]
pub async fn setup() -> Result<(LocalConfig, CoreConfig)> {
    let mut args = std::env::args().collect::<Vec<_>>();
    // Fetch
    let path = if args.len() != 2 {
        bail!("Must launch with exactly one argument specifying the configuration file to use")
    } else {
        match std::env::current_dir() {
            Ok(mut path) => {
                path.push(std::mem::take(&mut args[1]));
                path
            }
            Err(e) => {
                bail!("Error fetching current directory: {}", e)
            }
        }
    };

    let file_contents = fs::read_to_string(path)
        .await
        .context("Error reading configuration file")?;
    let settings: SettingsFile =
        ron::de::from_str(&file_contents).context("Error parsing configuration file")?;

    // TODO:

    Ok((settings.local_config, settings.core_config))
}
