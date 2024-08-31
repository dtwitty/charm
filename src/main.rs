use std::future::pending;
use std::time::Duration;

use crate::raft::core::config::RaftConfig;
use crate::raft::run_raft;
use clap::Parser;
use tonic::async_trait;

mod raft;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The address to bind to/
    #[arg(long)]
    bind_address: String,

    /// The minimum amount of time to wait before starting a new election.
    #[arg(long)]
    #[clap(value_parser = humantime::parse_duration, default_value = "150ms")]
    min_election_timeout: Duration,

    /// The maximum amount of time to wait before starting a new election.
    /// This should be greater than `min_election_timeout`.
    #[arg(long)]
    #[clap(value_parser = humantime::parse_duration, default_value = "300ms")]
    max_election_timeout: Duration,

    /// The amount of time to wait between sending heartbeats to followers.
    #[arg(long)]
    #[clap(value_parser = humantime::parse_duration, default_value = "50ms")]
    heartbeat_interval: Duration,

    /// The addresses of the other nodes in the cluster.
    #[arg(long)]
    peer_addresses: Vec<String>,
}

struct StateMachine {}

#[async_trait]
impl raft::state_machine::StateMachine for StateMachine {
    type Request = String;

    async fn apply(&mut self, request: Self::Request) {
        println!("Applying request: {}", request);
    }
}


#[tokio::main]
async fn main() {
    let args = Args::parse();
    let config = RaftConfig {
        node_id: raft::types::NodeId(args.bind_address.clone()),
        other_nodes: args.peer_addresses.into_iter().map(raft::types::NodeId).collect(),
        election_timeout_min: args.min_election_timeout,
        election_timeout_max: args.max_election_timeout,
        heartbeat_interval: args.heartbeat_interval,
    };

    let sm = StateMachine {};
    let raft_handle = run_raft(config, sm);
    pending::<()>().await;
}
