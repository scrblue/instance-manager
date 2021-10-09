// use super::manager_configuration::InstanceConf;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ManagerNetworkRequest {
    UpdateServerHealth {
        manager_id: u64,
        ports_free: u8,
        cpu_use_as_decimal_fraction: f32,
        ram_use_as_decimal_fraction: f32,
        ram_free_in_mb: u32,
    },
    ShareInstanceStore(HashMap<String, String>),
}
