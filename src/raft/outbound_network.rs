use crate::raft::messages::*;
use crate::raft::pb::raft_client::RaftClient;
use crate::raft::types::NodeId;
use std::collections::HashMap;
use tokio::spawn;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tonic::transport::Channel;
use tonic::Request;

mod charm {}


pub struct OutboundNetworkHandle {
    /// For sending messages out to the network.
    tx: UnboundedSender<(NodeId, RaftRequest)>,
}

impl OutboundNetworkHandle {
    pub fn send(&self, node_id: NodeId, message: RaftRequest) {
        self.tx.send((node_id, message)).unwrap();
    }
}

pub fn run_outbound_network(core_tx: UnboundedSender<RaftMessage>) -> OutboundNetworkHandle {
    let (tx, mut rx) = unbounded_channel();
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
            let core_tx_clone = core_tx.clone();
            spawn(async move {
                match request {
                    RaftRequest::AppendEntriesRequest(request) => {
                        let request_pb = request.to_pb();
                        let request = Request::new(request_pb);
                        if let Ok(response) = client.append_entries(request).await {
                            let response_pb = response.into_inner();
                            let response = AppendEntriesResponse::from_pb(&response_pb);
                            core_tx_clone.send(RaftMessage::Response(RaftResponse::AppendEntriesResponse(response))).unwrap();
                        }
                    }

                    RaftRequest::RequestVoteRequest(request) => {
                        let request_pb = request.to_pb();
                        let request = Request::new(request_pb);
                        if let Ok(response) = client.request_vote(request).await {
                            let response_pb = response.into_inner();
                            let response = RequestVoteResponse::from_pb(&response_pb);
                            core_tx_clone.send(RaftMessage::Response(RaftResponse::RequestVoteResponse(response))).unwrap();
                        }
                    }
                }
            });
        }
    });
    OutboundNetworkHandle { tx }
}

async fn create_client(node_id: NodeId) -> RaftClient<Channel> {
    let addr = node_id.0;
    RaftClient::connect(addr).await.unwrap()
}
