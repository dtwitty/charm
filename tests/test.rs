#![cfg(madsim)]

use charm::raft;
use charm::raft::core::config::RaftConfig;
use charm::raft::run_raft;
use charm::raft::types::NodeId;
use madsim::runtime::Handle;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::pending;
use std::net::IpAddr::V4;
use std::net::Ipv4Addr;
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

    let ips = vec![
        V4(Ipv4Addr::new(10, 0, 0, 1)),
        V4(Ipv4Addr::new(10, 0, 0, 2)),
        V4(Ipv4Addr::new(10, 0, 0, 3)),
    ];
    let nodes = ips.iter().map(|ip| {
        handle.create_node().name(format!("node-{}", ip)).ip(ip.clone()).build()
    }).collect::<Vec<_>>();

    let node_ids = nodes.iter().zip(ips.iter()).map(|(node, ip)| NodeId(format!("{}:54321", ip))).collect::<Vec<_>>();

    for (node_id, node) in node_ids.iter().zip(nodes.iter()) {
        let node_id_clones = node_ids.clone();
        let node_id_clone = node_id.clone();
        node.spawn(async move {
            let config = RaftConfig {
                node_id: node_id_clone,
                other_nodes: node_id_clones,
                election_timeout_min: Duration::from_millis(150),
                election_timeout_max: Duration::from_millis(300),
                heartbeat_interval: Duration::from_millis(50),
            };

            let sm = StateMachine {
                map: HashMap::new(),
            };
            let raft_handle = run_raft(config, sm);
            pending::<()>().await
        });
    }
}