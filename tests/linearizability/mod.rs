/// This module contains the linearizability testing machinery for the Charm service.

use charm::charm::pb::{DeleteResponse, GetResponse, PutResponse, ResponseHeader};
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
#[derive(Debug, Clone)]
pub struct CharmHistory {
    history: Arc<Mutex<LinearizabilityTester<u64, CharmSpec>>>,
}

impl CharmHistory {
    pub fn new() -> Self {
        Self {
            history: Arc::new(Mutex::new(LinearizabilityTester::new(CharmSpec::new())))
        }
    }

    pub fn client_history(&self, client_num: u64) -> ClientHistory {
        ClientHistory {
            total_history: self.clone(),
            client_num,
        }
    }

    pub fn linearize(&self) -> Option<Vec<(CharmReq, CharmResp)>> {
        self.history.lock().unwrap().serialized_history()
    }
}

pub struct ClientHistory {
    total_history: CharmHistory,
    client_num: u64,
}

impl ClientHistory {
    pub fn on_invoke(&self, op: CharmReq) {
        self.total_history.history.lock().unwrap().on_invoke(self.client_num, op).expect("valid");
    }

    pub fn on_return(&self, ret: CharmResp) {
        self.total_history.history.lock().unwrap().on_return(self.client_num, ret).expect("valid");
    }
}



