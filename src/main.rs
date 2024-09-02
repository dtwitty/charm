use std::collections::HashMap;
use std::future::pending;
use std::time::Duration;

use crate::raft::core::config::RaftConfig;
use crate::raft::run_raft;
use clap::Parser;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::oneshot;
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

struct StateMachine {
    map: HashMap<String, String>,
}

enum StateMachineRequest {
    Get(String, oneshot::Sender<Option<String>>),
    Set(String, String, oneshot::Sender<String>),
    Delete(String, oneshot::Sender<Option<String>>),
}

#[async_trait]
impl raft::state_machine::StateMachine for StateMachine {
    type Request = StateMachineRequest;

    async fn apply(&mut self, request: Self::Request) {
        match request {
            StateMachineRequest::Get(key, tx) => {
                let value = self.map.get(&key).cloned();
                tx.send(value).unwrap();
            }
            StateMachineRequest::Set(key, value, tx) => {
                let old_value = self.map.insert(key, value);
                tx.send(old_value.unwrap_or_default()).unwrap();
            }
            StateMachineRequest::Delete(key, tx) => {
                let value = self.map.remove(&key);
                tx.send(value).unwrap();
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
    let args = Args::parse();
    let config = RaftConfig {
        node_id: raft::types::NodeId(args.bind_address.clone()),
        other_nodes: args.peer_addresses.into_iter().map(raft::types::NodeId).collect(),
        election_timeout_min: args.min_election_timeout,
        election_timeout_max: args.max_election_timeout,
        heartbeat_interval: args.heartbeat_interval,
    };

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

                let commit=raft_handle.propose(StateMachineRequest::Get(key, tx)).await;
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

                let commit=raft_handle.propose(StateMachineRequest::Set(key, value, tx)).await;
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

                let commit=raft_handle.propose(StateMachineRequest::Delete(key, tx)).await;
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
