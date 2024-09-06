use crate::raft;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tonic::async_trait;

pub struct CharmStateMachine {
    map: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CharmStateMachineRequest {
    Get {
        key: String,
        #[serde(skip)]
        response: Option<oneshot::Sender<Option<String>>>,
    },
    Set {
        key: String,
        value: String,
        #[serde(skip)]
        response: Option<oneshot::Sender<()>>,
    },
    Delete {
        key: String,
        #[serde(skip)]
        response: Option<oneshot::Sender<()>>,
    },
}

#[async_trait]
impl raft::state_machine::StateMachine for CharmStateMachine {
    type Request = CharmStateMachineRequest;

    async fn apply(&mut self, request: Self::Request) {
        match request {
            CharmStateMachineRequest::Get { key, response: tx } => {
                let value = self.map.get(&key).cloned();
                if let Some(tx) = tx {
                    tx.send(value).unwrap();
                }
            }
            CharmStateMachineRequest::Set { key, value, response: tx } => {
                self.map.insert(key, value);
                if let Some(tx) = tx {
                    tx.send(()).unwrap();
                }
            }
            CharmStateMachineRequest::Delete { key, response: tx } => {
                self.map.remove(&key);
                if let Some(tx) = tx {
                    tx.send(()).unwrap();
                }
            }
        }
    }
}
