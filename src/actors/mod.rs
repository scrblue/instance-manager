/// Handles incoming connections and passes them to the actor for the connection type
pub mod connections;

// pub mod console;
// pub mod instances;

/// Handles connections with other instance managers that act as peers in the Raft cluster
pub mod peers;

/// Simple actor that handles stdin to act as a console and handle graceful shutdown
pub mod io;
