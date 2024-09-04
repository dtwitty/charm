use std::collections::HashMap;
use std::time::Duration;

use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::oneshot;
use tonic::async_trait;
use charm::raft;
use charm::raft::core::config::RaftConfig;
use charm::raft::run_raft;

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

struct StateMachine {
    map: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
enum StateMachineRequest {
    Get {
        key: String,
        #[serde(skip)]
        response: Option<oneshot::Sender<Option<String>>>,
    },
    Set {
        key: String,
        value: String,
        #[serde(skip)]
        response: Option<oneshot::Sender<Option<String>>>,
    },
    Delete {
        key: String,
        #[serde(skip)]
        response: Option<oneshot::Sender<Option<String>>>,
    }
}

#[async_trait]
impl raft::state_machine::StateMachine for StateMachine {
    type Request = StateMachineRequest;

    async fn apply(&mut self, request: Self::Request) {
        match request {
            StateMachineRequest::Get { key, response: tx } => {
                let value = self.map.get(&key).cloned();
                if let Some(tx) = tx {
                    tx.send(value).unwrap();
                }
            }
            StateMachineRequest::Set { key, value, response: tx } => {
                let old_value = self.map.insert(key, value);
                if let Some(tx) = tx {
                    tx.send(old_value).unwrap();
                }
            }
            StateMachineRequest::Delete { key, response: tx } => {
                let value = self.map.remove(&key);
                if let Some(tx) = tx {
                    tx.send(value).unwrap();
                }
            }
        }
    }
}

/*
TODO:
  - Implement core -> state machine
  - Run some madsim tests!
 */


#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let mut config = RaftConfig {
        node_id: raft::types::NodeId(args.bind_address.clone()),
        other_nodes: args.peer_addresses.into_iter().map(raft::types::NodeId).collect(),
        election_timeout_min: args.min_election_timeout,
        election_timeout_max: args.max_election_timeout,
        heartbeat_interval: args.heartbeat_interval,
    };
    config.other_nodes.retain(|node| node != &config.node_id);

    let sm = StateMachine {
        map: HashMap::new(),
    };
    let raft_handle = run_raft(config, sm);

    let mut input_lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = input_lines.next_line().await.unwrap(){
        let mut parts = line.split_whitespace();
        match parts.next() {

            Some("get") => {
                let key = parts.next().unwrap().to_string();
                let (tx, rx) = oneshot::channel();

                let commit = raft_handle.propose(StateMachineRequest::Get { key, response: Some(tx) }).await;
                if let Err(e) = commit {
                    println!("Error: {:?}", e);
                    continue;
                }

                let value = rx.await.unwrap();
                println!("{:?}", value);
            }

            Some("set") => {
                let key = parts.next().unwrap().to_string();
                let value = parts.next().unwrap().to_string();
                let (tx, rx) = oneshot::channel();

                let commit = raft_handle.propose(StateMachineRequest::Set { key, value, response: Some(tx) }).await;
                if let Err(e) = commit {
                    println!("Error: {:?}", e);
                    continue;
                }

                let old_value = rx.await.unwrap();
                println!("Previous value: {:?}", old_value);
            }

            Some("delete") => {
                let key = parts.next().unwrap().to_string();
                let (tx, rx) = oneshot::channel();

                let commit = raft_handle.propose(StateMachineRequest::Delete { key, response: Some(tx) }).await;
                if let Err(e) = commit {
                    println!("Error: {:?}", e);
                    continue;
                }

                let value = rx.await.unwrap();
                println!("{:?}", value);
            }

            _ => {
                println!("Invalid command");
            }
        }
    }
}
