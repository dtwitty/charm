use crate::raft::messages::*;
use crate::raft::network::charm::raft_client::RaftClient;
use crate::raft::types::NodeId;
use std::collections::HashMap;
use std::thread::spawn;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tonic::transport::Channel;

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
    let (mut tx, mut rx) = unbounded_channel();
    spawn(async move {

        // Holds the clients for each node.
        let mut clients = HashMap::new();

        // Process messages as they come in.
        while let Some((node_id, request)) = rx.recv().await {
            if !clients.contains_key(&node_id) {
                // If we don't have a client for this node yet, create one.
                let client = create_client(&node_id).await;
                clients.insert(node_id.clone(), client);
            }

            let mut client = clients.get_mut(&node_id).unwrap();
            match request {
                RaftRequest::AppendEntriesRequest(request) => {
                    let request_pb = request.to_pb();
                    if let Ok(response_pb) = client.append_entries(request_pb).await {
                        let response = AppendEntriesResponse::from_pb(&response_pb.into());
                        core_tx.send(RaftMessage::Response(response.into())).unwrap();
                    }
                }
                RaftRequest::RequestVoteRequest(request) => {
                    let request_pb = request.to_pb();
                    if let Ok(response_pb) = client.request_vote(request_pb).await {
                        let response = RequestVoteResponse::from_pb(&response_pb.into());
                        core_tx.send(RaftMessage::Response(response.into())).unwrap();
                    }
                }
            }
        }
    });
    OutboundNetworkHandle { tx }
}

async fn create_client(node_id: &NodeId) -> RaftClient<Channel> {
    let addr = &node_id.0;
    RaftClient::connect(addr).await.unwrap()
}

