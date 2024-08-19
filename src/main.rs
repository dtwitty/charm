use std::collections::HashMap;
use std::future::pending;
use std::time::Duration;

use tokio::sync::mpsc::UnboundedSender;
use tracing::Level;

use crate::raft::config::RaftConfig;
use crate::raft::messages::Message;
use crate::raft::network::Network;
use crate::raft::node::RaftNode;
use crate::raft::types::NodeId;

mod raft;

struct TokioNetwork {
    /// Used to send messages to other nodes in the cluster.
    senders: HashMap<NodeId, UnboundedSender<Message>>,
}

impl TokioNetwork {}

impl Network for TokioNetwork {
    fn send(&self, to: &NodeId, message: &Message) {
        self.senders.get(to).unwrap().send(message.clone()).unwrap();
    }
}

#[tokio::main]
async fn main() {

    // construct a subscriber that prints formatted traces to stdout
    tracing_subscriber::FmtSubscriber::builder().with_max_level(Level::DEBUG).init();

    let num_nodes = 3;
    let node_ids = (0..num_nodes).map(|i| NodeId(i.to_string())).collect::<Vec<_>>();

    // Handles to send to a node.
    let mut senders = HashMap::new();
    // Handles nodes use to receive messages.
    let mut receivers = HashMap::new();
    for node_id in &node_ids {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        senders.insert(node_id.clone(), sender);
        receivers.insert(node_id.clone(), receiver);
    }

    // Spawn some tasks.
    for node_id in &node_ids {
        let message_receiver = receivers.remove(node_id).unwrap();
        let other_senders: HashMap<NodeId, UnboundedSender<Message>> = senders
            .iter()
            .filter(|(k, _)| *k != node_id)
            .map(|(a, b)| (a.clone(), b.clone()))
            .collect();
        let other_nodes = other_senders.keys().cloned().collect();
        let network = TokioNetwork {
            senders: other_senders,
        };

        let config = RaftConfig {
            node_id: node_id.clone(),
            other_nodes,
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(100),
        };

        tokio::spawn(RaftNode::run(config, network, message_receiver));
    }

    pending().await
}
