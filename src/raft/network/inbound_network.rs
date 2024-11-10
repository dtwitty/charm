use crate::raft::core::handle::RaftCoreHandle;
use crate::raft::messages::{AppendEntriesRequest, RequestVoteRequest};
use crate::raft::pb::raft_server::{Raft, RaftServer};
use crate::raft::pb::{AppendEntriesRequestPb, AppendEntriesResponsePb, RequestVoteRequestPb, RequestVoteResponsePb};
use std::net::{IpAddr, Ipv4Addr};
use tokio::spawn;
use tonic::{async_trait, Request, Response, Status};
use tracing::debug;

#[derive(Debug)]
struct InboundNetwork<R: Send + 'static> {
    handle: RaftCoreHandle<R>,
}

#[async_trait]
impl<R: Send + 'static> Raft for InboundNetwork<R> {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn append_entries(&self, request: Request<AppendEntriesRequestPb>) -> Result<Response<AppendEntriesResponsePb>, Status> {
        let request_pb = request.into_inner();
        debug!("Received AppendEntriesRequest: {:?}", request_pb);
        let request = AppendEntriesRequest::from_pb(&request_pb);
        let rx = self.handle.append_entries_request(request);
        let response = rx.await.map_err(|_| Status::internal("Raft not ready"))?;
        let response_pb = response.to_pb();
        debug!("Sending AppendEntriesResponse: {:?}", response_pb);
        Ok(Response::new(response_pb))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn request_vote(&self, request: Request<RequestVoteRequestPb>) -> Result<Response<RequestVoteResponsePb>, Status> {
        let request_pb = request.into_inner();
        debug!("Received RequestVoteRequest: {:?}", request_pb);
        let request = RequestVoteRequest::from_pb(&request_pb);
        let rx = self.handle.request_vote_request(request);
        let response = rx.await.map_err(|_| Status::internal("Raft not ready"))?;
        let response_pb = response.to_pb();
        debug!("Sending RequestVoteResponse: {:?}", response_pb);
        Ok(Response::new(response_pb))
    }
}

pub fn run_inbound_network<R: Send + 'static>(port: u16, handle: RaftCoreHandle<R>) {
    let addr = (IpAddr::from(Ipv4Addr::UNSPECIFIED), port);
    let network = InboundNetwork { handle };

    #[cfg(not(feature = "turmoil"))]
    spawn(async move {
        tonic::transport::Server::builder()
            .add_service(RaftServer::new(network))
            .serve(addr.into())
            .await
            .unwrap();
    });

    #[cfg(feature = "turmoil")]
    spawn(async move {
        let server = tonic::transport::Server::builder()
            .add_service(RaftServer::new(network))
            .serve_with_incoming(crate::net::make_incoming(addr))
            .await;
        server.unwrap();
    });
}