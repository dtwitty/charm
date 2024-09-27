use crate::raft;
use crate::raft::types::RaftInfo;
use crate::tracing_util::SpanOption;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::oneshot;
use tonic::async_trait;
use tracing::Span;
use uuid::Uuid;

struct History {
    current: Option<String>,
    events: IndexMap<RequestId, Event>,
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
        request_id: RequestId,
        value: String,
        raft_info: RaftInfo,
    },

    Delete {
        request_id: RequestId,
        raft_info: RaftInfo,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HistoryResponse {
    pub events: Vec<Event>,
    pub raft_info: RaftInfo,
}

#[derive(Debug, Serialize, Deserialize, Hash, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct RequestId {
    pub client_id: Uuid,
    pub request_number: u64,
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
        request_id: RequestId,
        key: String,
        value: String,
        #[serde(skip)]
        response: Option<oneshot::Sender<SetResponse>>,
        #[serde(skip)]
        span: Option<Span>
    },
    Delete {
        request_id: RequestId,
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
            CharmStateMachineRequest::Set { request_id, key, value, response, span } => {
                span.in_scope(|| {
                    tracing::debug!("Set `{:?}` to `{:?}`", key, value);
                });
                let history = self.data.entry(key.clone()).or_insert_with(|| History { current: None, events: IndexMap::new() });

                if let Some(Event::Put { value: old_value, raft_info, .. }) = history.events.get(&request_id) {
                    if let Some(tx) = response {
                        tracing::debug!("Replaying request `{:?}`", request_id);
                        let r = tx.send(SetResponse { raft_info: raft_info.clone() });
                        if r.is_err() {
                            tracing::warn!("Failed to send response from Set request. The receiver has been dropped.");
                        }
                    }
                    return;
                }
                
                history.current = Some(value.clone());
                history.events.insert(request_id.clone(), Event::Put { request_id, value, raft_info: raft_info.clone() });

                if let Some(tx) = response {
                    let r = tx.send(SetResponse { raft_info });
                    if r.is_err() {
                        tracing::warn!("Failed to send response from Set request. The receiver has been dropped.");
                    }
                }
            }
            CharmStateMachineRequest::Delete { request_id, key, response, span } => {
                span.in_scope(|| {
                    tracing::debug!("Delete `{:?}`", key);
                });
                let history = self.data.entry(key.clone()).or_insert_with(|| History { current: None, events: IndexMap::new() });

                if let Some(Event::Delete { raft_info, .. }) = history.events.get(&request_id) {
                    if let Some(tx) = response {
                        tracing::debug!("Replaying request `{:?}`", request_id);
                        let r = tx.send(DeleteResponse { raft_info: raft_info.clone() });
                        if r.is_err() {
                            tracing::warn!("Failed to send response from Delete request. The receiver has been dropped.");
                        }
                    }
                    return;
                }
                
                history.current = None;
                history.events.insert(request_id.clone(), Event::Delete { request_id, raft_info: raft_info.clone() });
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
                let events = self.data.get(&key).map(|history| history.events.values().collect()).unwrap_or_default();
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
