use std::time::Duration;

use clap::Parser;

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

    /// The addresses of the other nodes in the cluster (can include this one for convenience).
    #[arg(long)]
    peer_addresses: Vec<String>,
}

/*
TODO:
  - Implement core -> state machine
  - Run some madsim tests!
 */


#[tokio::main]
async fn main() {
    todo!()
}

