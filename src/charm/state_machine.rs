use crate::charm::pb::{DeleteRequest, DeleteResponse, GetRequest, GetResponse, PutRequest, PutResponse, ResponseHeader};
use crate::charm::CharmNodeInfo;
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

impl CharmStateMachine {
    #[must_use] pub fn new() -> CharmStateMachine {
        CharmStateMachine {
            map: HashMap::new(),
        }
    }

}

#[derive(Debug, Serialize, Deserialize)]
pub enum CharmStateMachineRequest {
    Get {
        req: GetRequest,
        #[serde(skip)]
        response_tx: Option<oneshot::Sender<GetResponse>>,
        #[serde(skip)]
        span: Option<Span>
    },
    Put {
        req: PutRequest,
        #[serde(skip)]
        response_tx: Option<oneshot::Sender<PutResponse>>,
        #[serde(skip)]
        span: Option<Span>
    },
    Delete {
        req: DeleteRequest,
        #[serde(skip)]
        response_tx: Option<oneshot::Sender<DeleteResponse>>,
        #[serde(skip)]
        span: Option<Span>
    },
}

#[async_trait]
impl raft::state_machine::StateMachine<CharmNodeInfo> for CharmStateMachine {
    type Request = CharmStateMachineRequest;

    async fn apply(&mut self, request: Self::Request, raft_info: RaftInfo<CharmNodeInfo>) {
        match request {
            CharmStateMachineRequest::Get { req, response_tx, span } => {
                let key = req.key;
                let value = self.map.get(&key).cloned();
                span.in_scope(|| {
                    tracing::debug!("Get `{:?}` returns `{:?}`", key, value);
                });
                if let Some(tx) = response_tx {
                    let response_header = Some(get_response_header(raft_info));
                    let r = tx.send(GetResponse { value, response_header });
                    if r.is_err() {
                        tracing::warn!("Failed to send response from Get request. The receiver has been dropped.");
                    }
                }
            }
            CharmStateMachineRequest::Put { req, response_tx, span } => {
                let key = req.key;
                let value = req.value;
                span.in_scope(|| {
                    tracing::debug!("Set `{:?}` to `{:?}`", key, value);
                });
                self.map.insert(key, value);
                if let Some(tx) = response_tx {
                    let response_header = Some(get_response_header(raft_info));
                    let r = tx.send(PutResponse { response_header });
                    if r.is_err() {
                        tracing::warn!("Failed to send response from Set request. The receiver has been dropped.");
                    }
                }
            }
            CharmStateMachineRequest::Delete { req, response_tx, span } => {
                let key = req.key;
                span.in_scope(|| {
                    tracing::debug!("Delete `{:?}`", key);
                });
                self.map.remove(&key);
                if let Some(tx) = response_tx {
                    let response_header = Some(get_response_header(raft_info));
                    let r = tx.send(DeleteResponse { response_header });
                    if r.is_err() {
                        tracing::warn!("Failed to send response from Delete request. The receiver has been dropped.");
                    }
                }
            }
        }
    }
}

fn get_response_header(raft_info: RaftInfo<CharmNodeInfo>) -> ResponseHeader {
    ResponseHeader {
        leader_addr: raft_info.leader_info.to_addr(),
        raft_term: raft_info.term.0,
        raft_index: raft_info.index.0,
    }
}
