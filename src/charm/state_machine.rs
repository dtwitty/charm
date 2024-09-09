use crate::raft;
use crate::tracing_util::SpanOption;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::oneshot;
use tonic::async_trait;
use tracing::Span;

pub struct CharmStateMachine {
    map: HashMap<String, String>,
}

impl CharmStateMachine {
    pub fn new() -> CharmStateMachine {
        CharmStateMachine {
            map: HashMap::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CharmStateMachineRequest {
    Get {
        key: String,
        #[serde(skip)]
        response: Option<oneshot::Sender<Option<String>>>,
        #[serde(skip)]
        span: Option<Span>
    },
    Set {
        key: String,
        value: String,
        #[serde(skip)]
        response: Option<oneshot::Sender<()>>,
        #[serde(skip)]
        span: Option<Span>
    },
    Delete {
        key: String,
        #[serde(skip)]
        response: Option<oneshot::Sender<()>>,
        #[serde(skip)]
        span: Option<Span>
    },
}

#[async_trait]
impl raft::state_machine::StateMachine for CharmStateMachine {
    type Request = CharmStateMachineRequest;

    async fn apply(&mut self, request: Self::Request) {
        match request {
            CharmStateMachineRequest::Get { key, response, span } => {
                let value = self.map.get(&key).cloned();
                span.in_scope(|| {
                    tracing::debug!("Get `{:?}` returns `{:?}`", key, value);
                });
                if let Some(tx) = response {
                    tx.send(value).unwrap();
                }
            }
            CharmStateMachineRequest::Set { key, value, response, span } => {
                span.in_scope(|| {
                    tracing::debug!("Set `{:?}` to `{:?}`", key, value);
                });
                self.map.insert(key, value);
                if let Some(tx) = response {
                    tx.send(()).unwrap();
                }
            }
            CharmStateMachineRequest::Delete { key, response, span } => {
                span.in_scope(|| {
                    tracing::debug!("Delete `{:?}`", key);
                });
                self.map.remove(&key);
                if let Some(tx) = response {
                    tx.send(()).unwrap();
                }
            }
        }
    }
}
