use crate::raft;
use crate::raft::types::RaftInfo;
use crate::tracing_util::SpanOption;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::oneshot;
use tonic::async_trait;
use tracing::Span;

struct History {
    current: Option<String>,
    events: Vec<Event>,
}

pub struct CharmStateMachine {
    data: HashMap<String, History>,
}

impl Default for CharmStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl CharmStateMachine {
    #[must_use] pub fn new() -> CharmStateMachine {
        CharmStateMachine {
            data: HashMap::new(),
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Event {
    Put {
        value: String,
        raft_info: RaftInfo,
    },

    Delete {
        raft_info: RaftInfo,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HistoryResponse {
    pub events: Vec<Event>,
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
    History {
        key: String,
        #[serde(skip)]
        response: Option<oneshot::Sender<HistoryResponse>>,
        #[serde(skip)]
        span: Option<Span>,
    },
}

#[async_trait]
impl raft::state_machine::StateMachine for CharmStateMachine {
    type Request = CharmStateMachineRequest;

    async fn apply(&mut self, request: Self::Request, raft_info: RaftInfo) {
        match request {
            CharmStateMachineRequest::Get { key, response, span } => {
                let value = self.data.get(&key).map(|history| history.current.clone()).flatten();
                span.in_scope(|| {
                    tracing::debug!("Get `{:?}` returns `{:?}`", key, value);
                });
                if let Some(tx) = response {
                    let r = tx.send(GetResponse { value, raft_info });
                    if r.is_err() {
                        tracing::warn!("Failed to send response from Get request. The receiver has been dropped.");
                    }
                }
            }
            CharmStateMachineRequest::Set { key, value, response, span } => {
                span.in_scope(|| {
                    tracing::debug!("Set `{:?}` to `{:?}`", key, value);
                });
                let history = self.data.entry(key.clone()).or_insert_with(|| History { current: None, events: Vec::new() });
                history.current = Some(value.clone());
                history.events.push(Event::Put { value, raft_info: raft_info.clone() });
                
                if let Some(tx) = response {
                    let r = tx.send(SetResponse { raft_info });
                    if r.is_err() {
                        tracing::warn!("Failed to send response from Set request. The receiver has been dropped.");
                    }
                }
            }
            CharmStateMachineRequest::Delete { key, response, span } => {
                span.in_scope(|| {
                    tracing::debug!("Delete `{:?}`", key);
                });
                let history = self.data.entry(key.clone()).or_insert_with(|| History { current: None, events: Vec::new() });
                history.current = None;
                history.events.push(Event::Delete { raft_info: raft_info.clone() });
                if let Some(tx) = response {
                    let r = tx.send(DeleteResponse { raft_info });
                    if r.is_err() {
                        tracing::warn!("Failed to send response from Delete request. The receiver has been dropped.");
                    }
                }
            }

            CharmStateMachineRequest::History {
                key,
                response,
                span,
            } => {
                let events = self.data.get(&key).map(|history| history.events.clone()).unwrap_or_default();
                span.in_scope(|| {
                    tracing::debug!("History for `{:?}`: `{:?}`", key, events);
                });
                if let Some(tx) = response {
                    let r = tx.send(HistoryResponse { events, raft_info });
                    if r.is_err() {
                        tracing::warn!("Failed to send response from History request. The receiver has been dropped.");
                    }
                }
            }
        }
    }
}
