use crate::raft::core::handle::RaftCoreHandle;
use crate::raft::messages::*;
use crate::raft::pb::raft_client::RaftClient;
use crate::raft::types::NodeId;
use std::collections::HashMap;
use tokio::spawn;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tonic::transport::Channel;
use tonic::Request;

pub enum RaftRequest {
    AppendEntries(AppendEntriesRequest),
    RequestVote(RequestVoteRequest),
}

pub struct OutboundNetworkHandle {
    tx: UnboundedSender<(NodeId, RaftRequest)>,
}

impl OutboundNetworkHandle {
    pub fn new(tx: UnboundedSender<(NodeId, RaftRequest)>) -> Self {
        Self { tx }
    }

    pub fn append_entries(&self, node_id: NodeId, request: AppendEntriesRequest) {
        self.tx.send((node_id, RaftRequest::AppendEntries(request))).unwrap();
    }

    pub fn request_vote(&self, node_id: NodeId, request: RequestVoteRequest) {
        self.tx.send((node_id, RaftRequest::RequestVote(request))).unwrap();
    }
}

pub fn run_outbound_network<R: Send + 'static>(handle: RaftCoreHandle<R>, mut rx: UnboundedReceiver<(NodeId, RaftRequest)>) {
    spawn(async move {

        // Holds the clients for each node.
        let mut clients: HashMap<NodeId, RaftClient<Channel>> = HashMap::new();

        // Process messages as they come in.
        while let Some((node_id, request)) = rx.recv().await {
            if !clients.contains_key(&node_id) {
                // If we don't have a client for this node yet, create one.
                let client = create_client(node_id.clone()).await;
                clients.insert(node_id.clone(), client);
            }

            let mut client = clients.get_mut(&node_id).unwrap().clone();
            let handle_clone = handle.clone();
            spawn(async move {
                match request {
                    RaftRequest::AppendEntries(request) => {
                        let request_pb = request.to_pb();
                        let request = Request::new(request_pb);
                        if let Ok(response) = client.append_entries(request).await {
                            let response_pb = response.into_inner();
                            let response = AppendEntriesResponse::from_pb(&response_pb);
                            handle_clone.append_entries_response(response);
                        }
                    }

                    RaftRequest::RequestVote(request) => {
                        let request_pb = request.to_pb();
                        let request = Request::new(request_pb);
                        if let Ok(response) = client.request_vote(request).await {
                            let response_pb = response.into_inner();
                            let response = RequestVoteResponse::from_pb(&response_pb);
                            handle_clone.request_vote_response(response);
                        }
                    }
                }
            });
        }
    });
}

async fn create_client(node_id: NodeId) -> RaftClient<Channel> {
    let addr = node_id.0;
    RaftClient::connect(addr).await.unwrap()
}
