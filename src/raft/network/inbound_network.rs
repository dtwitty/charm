use crate::raft::core::handle::RaftCoreHandle;
use crate::raft::messages::{AppendEntriesRequest, RequestVoteRequest};
use crate::raft::pb::raft_server::{Raft, RaftServer};
use crate::raft::pb::{AppendEntriesRequestPb, AppendEntriesResponsePb, RequestVoteRequestPb, RequestVoteResponsePb};
use crate::raft::types::NodeId;
use tonic::{Request, Response, Status};

struct InboundNetwork {
    handle: RaftCoreHandle,
}

impl Raft for InboundNetwork {
    async fn append_entries(&self, request: Request<AppendEntriesRequestPb>) -> Result<Response<AppendEntriesResponsePb>, Status> {
        let request_pb = request.into_inner();
        let request = AppendEntriesRequest::from_pb(&request_pb);
        let rx = self.handle.append_entries_request(request);
        let response = rx.await.unwrap();
        let response_pb = response.to_pb();
        Ok(Response::new(response_pb))
    }

    async fn request_vote(&self, request: Request<RequestVoteRequestPb>) -> Result<Response<RequestVoteResponsePb>, Status> {
        let request_pb = request.into_inner();
        let request = RequestVoteRequest::from_pb(&request_pb);
        let rx = self.handle.request_vote_request(request);
        let response = rx.await.unwrap();
        let response_pb = response.to_pb();
        Ok(Response::new(response_pb))
    }
}

async fn run_inbound_network(node_id: NodeId, handle: RaftCoreHandle) {
    let addr = node_id.0.parse().unwrap();
    let network = InboundNetwork { handle };
    tonic::transport::Server::builder()
        .add_service(RaftServer::new(network))
        .serve(addr)
        .await
        .unwrap();
}