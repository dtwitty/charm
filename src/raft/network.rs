use std::collections::HashMap;

use async_stream::stream;
use futures::Stream;
use tokio::net::ToSocketAddrs;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tonic::Request;

use crate::charm::raft_client::RaftClient;
use crate::raft::messages::{AppendEntriesRequest, Message};
use crate::raft::types::NodeId;

pub mod charm {
    tonic::include_proto!("raft");
}

pub trait Network {
    fn send(&self, to: &NodeId, message: &Message);
}