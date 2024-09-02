#![cfg(madsim)]

use charm::raft;
use charm::raft::core::config::RaftConfig;
use charm::raft::run_raft;
use charm::raft::types::NodeId;
use madsim::runtime::Handle;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::oneshot;
use tonic::async_trait;

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
    },
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
#[madsim::test]
async fn test_state_machine() {
    let handle = Handle::current();

    let ports = vec![54321, 54322, 54323];
    let nodes = ports.iter().map(|p| {
        handle.create_node()
    });

    let node0 = handle.create_node().name("server").ip(addr0.ip()).build();
    let node1 = handle.create_node().name("client").ip(addr1.ip()).build();
    let node2 = handle.create_node().name("client").ip(addr2.ip()).build();

    let config = RaftConfig {
        node_id: NodeId("127.0.0.1:54321".to_string()),
        other_nodes: vec![NodeId("127.0.0.1:54322".to_string()), NodeId("127.0.0.1:54323".to_string())],
        election_timeout_min: Duration::from_millis(150),
        election_timeout_max: Duration::from_millis(300),
        heartbeat_interval: Duration::from_millis(50),
    };

    let sm = StateMachine {
        map: HashMap::new(),
    };
    let raft_handle = run_raft(config, sm);
}