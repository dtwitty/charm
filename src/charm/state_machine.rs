use crate::raft;
use crate::raft::types::RaftInfo;
use crate::tracing_util::SpanOption;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::oneshot;
use tonic::async_trait;
use tracing::Span;

pub struct CharmStateMachine {
    map: HashMap<String, String>,
}

impl Default for CharmStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl CharmStateMachine {
    #[must_use] pub fn new() -> CharmStateMachine {
        CharmStateMachine {
            map: HashMap::new(),
        }
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct GetResponse {
    pub value: Option<String>,
    pub raft_info: RaftInfo,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SetResponse {
    pub raft_info: RaftInfo,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteResponse {
    pub raft_info: RaftInfo,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CharmStateMachineRequest {
    Get {
        key: String,
        #[serde(skip)]
        response: Option<oneshot::Sender<GetResponse>>,
        #[serde(skip)]
        span: Option<Span>
    },
    Set {
        key: String,
        value: String,
        #[serde(skip)]
        response: Option<oneshot::Sender<SetResponse>>,
        #[serde(skip)]
        span: Option<Span>
    },
    Delete {
        key: String,
        #[serde(skip)]
        response: Option<oneshot::Sender<DeleteResponse>>,
        #[serde(skip)]
        span: Option<Span>
    },
}

#[async_trait]
impl raft::state_machine::StateMachine for CharmStateMachine {
    type Request = CharmStateMachineRequest;

    async fn apply(&mut self, request: Self::Request, raft_info: RaftInfo) {
        match request {
            CharmStateMachineRequest::Get { key, response, span } => {
                let value = self.map.get(&key).cloned();
                span.in_scope(|| {
                    tracing::debug!("Get `{:?}` returns `{:?}`", key, value);
                });
                if let Some(tx) = response {
                    tx.send(GetResponse { value, raft_info }).unwrap();
                }
            }
            CharmStateMachineRequest::Set { key, value, response, span } => {
                span.in_scope(|| {
                    tracing::debug!("Set `{:?}` to `{:?}`", key, value);
                });
                self.map.insert(key, value);
                if let Some(tx) = response {
                    tx.send(SetResponse { raft_info }).unwrap();
                }
            }
            CharmStateMachineRequest::Delete { key, response, span } => {
                span.in_scope(|| {
                    tracing::debug!("Delete `{:?}`", key);
                });
                self.map.remove(&key);
                if let Some(tx) = response {
                    tx.send(DeleteResponse { raft_info }).unwrap();
                }
            }
        }
    }
}
