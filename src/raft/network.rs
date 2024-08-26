use crate::raft::messages::{AppendEntriesRequest, AppendEntriesResponse, Message, RequestVoteRequest, RequestVoteResponse};
use crate::raft::network::charm::raft_server::{Raft, RaftServer};
use crate::raft::network::charm::{AppendEntriesRequestPb, AppendEntriesResponsePb, RequestVoteRequestPb, RequestVoteResponsePb};
use crate::raft::types::NodeId;
use futures::Stream;
use tokio::net::ToSocketAddrs;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tonic::{Request, Response, Status};

pub mod charm {
    tonic::include_proto!("raftpb");
}

pub struct Network {
    /// For sending messages to other nodes.
    send_tx: UnboundedSender<(NodeId, Message)>,

    /// For sending messages to the raft core.
    core_tx: UnboundedSender<(Message, Sender<Message>)>,

    server: RaftServer<Network>,
}

impl Network {}

impl Raft for Network {
    async fn append_entries(&self, request: Request<AppendEntriesRequestPb>) -> Result<Response<AppendEntriesResponsePb>, Status> {
        let request = request.into_inner();
        let request = append_entries_request_pb_to_message(request);
        let (tx, rx) = oneshot::channel();
        self.core_tx.send((Message::AppendEntriesRequest(request), tx)).expect("Core is down!");
        let response = rx.await.expect("Core is down!");

        Ok(Response::new(response))
    }

    async fn request_vote(&self, request: Request<RequestVoteRequestPb>) -> Result<Response<RequestVoteResponsePb>, Status> {
        todo!()
    }
}

fn append_entries_request_pb_to_message(request: AppendEntriesRequestPb) -> AppendEntriesRequest {
    todo!()
}

fn append_entries_request_message_to_pb(request: AppendEntriesRequest) -> AppendEntriesRequestPb {
    todo!()
}

fn append_entries_response_pb_to_message(response: AppendEntriesResponsePb) -> AppendEntriesResponse {
    todo!()
}

fn append_entries_response_message_to_pb(response: AppendEntriesResponse) -> AppendEntriesResponsePb {
    todo!()
}

fn request_vote_request_pb_to_message(request: RequestVoteRequestPb) -> RequestVoteRequest {
    todo!()
}

fn request_vote_request_message_to_pb(request: RequestVoteRequest) -> RequestVoteRequestPb {
    todo!()
}

fn request_vote_response_pb_to_message(response: RequestVoteResponsePb) -> RequestVoteResponse {
    todo!()
}

fn request_vote_response_message_to_pb(response: RequestVoteResponse) -> RequestVoteResponsePb {
    todo!()
}
