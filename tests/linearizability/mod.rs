/// This module contains the linearizability testing machinery for the Charm service.

use charm::charm::pb::{DeleteResponse, GetResponse, PutResponse, ResponseHeader};
use dashmap::DashMap;
use stateright::semantics::{ConsistencyTester, LinearizabilityTester, SequentialSpec};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};


/// The requests that can be made to the Charm service.
#[derive(Debug, Clone, PartialEq)]
pub enum CharmReq {
    Get(String),
    Put(String, String),
    Delete(String),
}

/// The responses that can be returned by the Charm service.
#[derive(Debug, Clone, PartialEq)]
pub enum CharmResp {
    Get(GetResponse),
    Put(PutResponse),
    Delete(DeleteResponse),
}

/// A specification for the Charm service.
/// In short:
///   Charm is a replicated hashmap
///   Charm has a strong leader
///   Charm has a monotonic term and index.
#[derive(Debug, Clone)]
pub struct CharmSpec {
    data: HashMap<String, String>,
    leader_addr: String,
    term: u64,
    index: u64,
}

impl CharmSpec {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            leader_addr: "".to_string(),
            term: 0,
            index: 0,
        }
    }

    /// Attempts to update the term and index based on the response header. Returns false 
    /// if this transition is impossible.
    pub fn update_term_and_index(&mut self, response_header: &Option<ResponseHeader>) -> bool {
        // There must be a response header in every response.
        if let Some(response_header) = response_header {

            // The term must never decrease.
            if self.term > response_header.raft_term {
                return false;
            }

            // The leader address must be consistent with the term.
            if self.term == response_header.raft_term && self.leader_addr != response_header.leader_addr {
                return false;
            }
            self.term = response_header.raft_term;
            self.leader_addr = response_header.leader_addr.clone();

            // The index must never decrease.
            if self.index >= response_header.raft_index {
                return false;
            }
            self.index = response_header.raft_index;

            true
        } else {
            false
        }
    }
}

impl SequentialSpec for CharmSpec {
    type Op = CharmReq;
    type Ret = CharmResp;

    fn invoke(&mut self, op: &Self::Op) -> Self::Ret {
        match op {
            CharmReq::Get(key) => {
                let value = self.data.get(key).cloned();
                let response_header = Some(ResponseHeader {
                    leader_addr: "".to_string(),
                    raft_term: self.term,
                    raft_index: self.index,
                });
                let response = GetResponse { value, response_header };
                CharmResp::Get(response)
            }

            CharmReq::Put(key, value) => {
                self.index += 1;
                self.data.insert(key.clone(), value.clone());
                let response_header = Some(ResponseHeader {
                    leader_addr: "".to_string(),
                    raft_term: self.term,
                    raft_index: self.index,
                });
                let response = PutResponse { response_header };
                CharmResp::Put(response)
            }

            CharmReq::Delete(key) => {
                self.index += 1;
                self.data.remove(key);
                let response_header = Some(ResponseHeader {
                    leader_addr: "".to_string(),
                    raft_term: self.term,
                    raft_index: self.index,
                });
                let response = DeleteResponse { response_header };
                CharmResp::Delete(response)
            }
        }
    }

    fn is_valid_step(&mut self, op: &Self::Op, ret: &Self::Ret) -> bool {
        match (op, ret) {
            (CharmReq::Get(key), CharmResp::Get(resp)) => {
                if !self.update_term_and_index(&resp.response_header) {
                    return false;
                }

                let value = self.data.get(key).cloned();
                value == resp.value
            }

            (CharmReq::Put(key, value), CharmResp::Put(resp)) => {
                if !self.update_term_and_index(&resp.response_header) {
                    return false;
                }

                self.data.insert(key.clone(), value.clone());
                true
            }

            (CharmReq::Delete(key), CharmResp::Delete(resp)) => {
                if !self.update_term_and_index(&resp.response_header) {
                    return false;
                }

                self.data.remove(key);
                true
            }

            // Response must always match the request.
            _ => false,
        }
    }
}

/// A convenient thread-safe, cloneable wrapper around a linearizability tester.
pub struct CharmHistory {
    client_histories: DashMap<u64, ClientHistory>,
}

impl CharmHistory {
    pub fn new() -> Self {
        Self {
            client_histories: DashMap::new(),
        }
    }

    pub fn for_client(&self, client_num: u64) -> ClientHistory {
        self.client_histories.entry(client_num).or_insert(ClientHistory::new()).clone()
    }

    pub fn linearize(&self) -> Option<Vec<(CharmReq, CharmResp)>> {
        let mut tester = LinearizabilityTester::new(CharmSpec::new());
        self.client_histories.iter().for_each(|r| {
            let client_num = r.key().clone();
            let client_history = r.value();
            client_history.history().iter().for_each(|req_resp| {
                match req_resp {
                    CharmReqResp::Req(req) => tester.on_invoke(client_num, req.clone()).unwrap(),
                    CharmReqResp::Resp(resp) => tester.on_return(client_num, resp.clone()).unwrap(),
                };
            });
        });
        tester.serialized_history()
    }

    pub fn history_by_raft_time(&self) -> Vec<(u64, CharmReq, CharmResp)> {
        let mut v = Vec::new();
        self.client_histories.iter().for_each(|r| {
            let client_num = r.key().clone();
            let client_history = r.value();
            client_history.history().chunks(2).for_each(|chunk| {
                if chunk.len() != 2 {
                    panic!("Got a malformed request-response: {:?}", chunk);
                }

                let req = chunk[0].clone();
                let resp = chunk[1].clone();

                match (req, resp) {
                    (CharmReqResp::Req(req), CharmReqResp::Resp(resp)) => {
                        v.push((client_num, req, resp));
                    }
                    _ => panic!("Got a malformed request-response: {:?}", chunk),
                }
            });
        });

        v.sort_by_key(|(_, _, resp)| {
            let response_header = match resp {
                CharmResp::Get(resp) => resp.response_header.clone().unwrap(),
                CharmResp::Put(resp) => resp.response_header.clone().unwrap(),
                CharmResp::Delete(resp) => resp.response_header.clone().unwrap(),
            };

            (response_header.raft_term, response_header.raft_index)
        });

        v
    }
}
#[derive(Debug, Clone, PartialEq)]
pub enum CharmReqResp {
    Req(CharmReq),
    Resp(CharmResp),
}

#[derive(Clone)]
pub struct ClientHistory {
    local_history: Arc<Mutex<Vec<CharmReqResp>>>,
}

impl ClientHistory {
    pub fn new() -> Self {
        Self {
            local_history: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn on_invoke(&mut self, op: CharmReq) {
        self.local_history.lock().unwrap().push(CharmReqResp::Req(op));
    }

    pub fn on_return(&mut self, ret: CharmResp) {
        self.local_history.lock().unwrap().push(CharmReqResp::Resp(ret));
    }

    pub fn history(&self) -> Vec<CharmReqResp> {
        self.local_history.lock().unwrap().clone()
    }
}



