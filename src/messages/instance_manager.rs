use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum InstanceManagerRequest {
    InstanceSocketAddr(uuid::Uuid),
    GetRelatedInstances(uuid::Uuid),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum InstanceManagerResponse {
    InstanceSocketAddr(uuid::Uuid, SocketAddr),
    RelatedInstances(HashMap<uuid::Uuid, String>),
}
