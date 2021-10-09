use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
/// A Map of Method names and Methods
pub struct InstanceConfig(pub HashMap<String, Method>);

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Method {
    /// How many servers to run the method on
	pub launch_on: LaunchOn,

	/// Default variable values -- not all variables that appear in the run instructions must
	/// appear here
	pub default_vars: HashMap<String, String>,
	
	/// List of Run instructions
	pub run: Vec<Run>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum LaunchOn {
	Single,
	Range(u8, u8),
	Specific
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Run {
    /// Call another method associated with this instance
    CallMethod(String, Vec<Val>),
   	/// Execute something on the server with the given args
    Exec(Val, Vec<Arg>),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Val {
	Literal(String),
	Variable(String),

	SelfId,
	SelfIp,
	SelfPort,

	SpawnerId,
	SpawnerIp,
	SpawnerPort,

	// Only valid in an EachPeer arg
	PeerId,
	PeerIp,
	PeerPort,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Arg {
    /// An argument passed once
    Standard(String, Vec<Val>),

    /// An argument passed once per peer
    EachPeer(String, Vec<Val>),
}
