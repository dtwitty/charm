use crate::charm::pb::{ClientId, DeleteRequest, DeleteResponse, GetRequest, GetResponse, PutRequest, PutResponse, RequestHeader, ResponseHeader};
use crate::charm::CharmNodeInfo;
use crate::raft;
use crate::raft::types::RaftInfo;
use crate::tracing_util::SpanOption;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use tokio::sync::oneshot;
use tonic::async_trait;
use tracing::Span;
use uuid::Uuid;

#[derive(Debug)]
struct PerClientCache {
    put_cache: BTreeMap<u64, PutResponse>,
    delete_cache: BTreeMap<u64, DeleteResponse>,
}

impl PerClientCache {
    #[must_use]
    fn new() -> PerClientCache {
        PerClientCache {
            put_cache: BTreeMap::new(),
            delete_cache: BTreeMap::new(),
        }
    }

    fn insert_put(&mut self, req_id: u64, resp: PutResponse) {
        self.put_cache.insert(req_id, resp);
    }

    fn delete(&mut self, req_id: u64, resp: DeleteResponse) {
        self.delete_cache.insert(req_id, resp);
    }

    fn get_put(&self, req_id: u64) -> Option<&PutResponse> {
        self.put_cache.get(&req_id)
    }

    fn get_delete(&self, req_id: u64) -> Option<&DeleteResponse> {
        self.delete_cache.get(&req_id)
    }

    fn collect_garbage(&mut self, req_id: u64) {
        self.put_cache = self.put_cache.split_off(&req_id);
        self.delete_cache = self.delete_cache.split_off(&req_id);
    }
}


#[derive(Debug)]
pub struct CharmStateMachine {
    data: HashMap<String, String>,
    response_cache: HashMap<Uuid, PerClientCache>,
}

impl CharmStateMachine {
    #[must_use] pub fn new() -> CharmStateMachine {
        CharmStateMachine {
            data: HashMap::new(),
            response_cache: HashMap::new(),
        }
    }

    fn get_response_cache(&mut self, client_id: ClientId) -> &mut PerClientCache {
        let lo = client_id.lo_bits;
        let hi = client_id.hi_bits;
        let uuid = Uuid::from_u64_pair(hi, lo);
        self.response_cache.entry(uuid).or_insert_with(PerClientCache::new)
    }

    fn maybe_get_response_cache(&mut self, client_id: ClientId) -> Option<&mut PerClientCache> {
        let lo = client_id.lo_bits;
        let hi = client_id.hi_bits;
        let uuid = Uuid::from_u64_pair(hi, lo);
        self.response_cache.get_mut(&uuid)
    }

    fn collect_garbage(&mut self, request_header: &RequestHeader) {
        let client_id = request_header.client_id.unwrap();
        let mut cache = self.maybe_get_response_cache(client_id);
        cache.iter_mut().for_each(|c| c.collect_garbage(request_header.first_incomplete_request_number));
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

                span.in_scope(|| {
                    tracing::debug!("Get `{:?}`", key);

                    // Clean up old requests.
                    if let Some(request_header) = req.request_header {
                        self.collect_garbage(&request_header);
                    }

                    let value = self.data.get(&key).cloned();
                    let response_header = Some(get_response_header(raft_info));
                    let response = GetResponse { value, response_header };
                    maybe_respond(response_tx, response);
                });
            }

            CharmStateMachineRequest::Put { req, response_tx, span } => {
                let key = req.key;
                let value = req.value;

                span.in_scope(|| {
                    tracing::debug!("Set `{:?}` to `{:?}`", key, value);

                    if let Some(request_header) = req.request_header {
                        // Clean up old requests.
                        self.collect_garbage(&request_header);

                        // Check for a cached response.
                        if let Some(cache) = self.maybe_get_response_cache(request_header.client_id.unwrap()) {
                            if let Some(resp) = cache.get_put(request_header.request_number) {
                                maybe_respond(response_tx, resp.clone());
                                return;
                            }
                        }
                    }

                    self.data.insert(key, value);
                    let response_header = Some(get_response_header(raft_info));
                    let response = PutResponse { response_header };

                    // Cache the response.
                    if let Some(request_header) = req.request_header {
                        let cache = self.get_response_cache(request_header.client_id.unwrap());
                        cache.insert_put(request_header.request_number, response.clone());
                    }

                    maybe_respond(response_tx, response);
                });
            }

            CharmStateMachineRequest::Delete { req, response_tx, span } => {
                let key = req.key;
                span.in_scope(|| {
                    tracing::debug!("Delete `{:?}`", key);

                    if let Some(request_header) = req.request_header {
                        // Clean up old requests.
                        self.collect_garbage(&request_header);

                        // Check for a cached response.
                        if let Some(cache) = self.maybe_get_response_cache(request_header.client_id.unwrap()) {
                            if let Some(resp) = cache.get_delete(request_header.request_number) {
                                maybe_respond(response_tx, resp.clone());
                                return;
                            }
                        }
                    }

                    self.data.remove(&key);
                    let response_header = Some(get_response_header(raft_info));
                    let response = DeleteResponse { response_header };

                    // Cache the response.
                    if let Some(request_header) = req.request_header {
                        let cache = self.get_response_cache(request_header.client_id.unwrap());
                        cache.delete(request_header.request_number, response.clone());
                    }

                    maybe_respond(response_tx, response);
                });
            }
        }
    }
}

fn maybe_respond<R>(response_tx: Option<oneshot::Sender<R>>, response: R) {
    if let Some(tx) = response_tx {
        let r = tx.send(response);
        if r.is_err() {
            tracing::warn!("Failed to send response. The receiver has been dropped.");
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
