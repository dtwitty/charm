use std::fmt::Debug;
use crate::charm::client::make_charm_client;
use crate::charm::config::CharmConfig;
use crate::charm::pb::charm_client::CharmClient;
use crate::charm::pb::charm_server::{Charm, CharmServer};
use crate::charm::pb::{DeleteRequest, DeleteResponse, GetRequest, GetResponse, PutRequest, PutResponse, };
use crate::charm::state_machine::CharmStateMachineRequest;
use crate::raft::core::error::RaftCoreError::{NotLeader, NotReady};
use crate::raft::types::NodeId;
use crate::raft::RaftHandle;
use crate::server::CharmPeer;
use dashmap::DashMap;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr};
use tokio::spawn;
use tokio::sync::oneshot;
use tonic::transport::Channel;
use tonic::{async_trait, Request, Response, Status};
use tracing::{debug, Span};

struct CharmServerImpl {
    config: CharmConfig,
    raft_handle: RaftHandle<CharmStateMachineRequest>,
    clients: DashMap<NodeId, CharmClient<Channel>>,
}

impl CharmServerImpl {
    fn get_client(&self, leader: NodeId) -> anyhow::Result<CharmClient<Channel>> {
        // Get or create a client for the leader.
        let entry = self.clients.entry(leader.clone());
        let client_ref_res = entry.or_try_insert_with(|| {
            let peer = self.get_leader_peer(&leader)?;
            let addr = peer.charm_addr();
            make_charm_client(addr)
        });
        let client= client_ref_res.map_err(|e| Status::internal(e.to_string()))?;
        // Clone and drop the ref to avoid holding the lock.
        Ok(client.clone())
    }

    fn get_leader_peer(&self, leader: &NodeId) -> anyhow::Result<CharmPeer> {
        // Check if this node ID is us.
        if leader.host == self.config.listen.host && leader.port == self.config.listen.raft_port {
            return Ok(CharmPeer {
                host: self.config.listen.host.clone(),
                raft_port: self.config.listen.raft_port,
                charm_port: self.config.listen.charm_port,
            });
        }
        
        // Get the host off the leader node ID.
        // Look in the config for a peer with the same host.
        self.config.peers
            .iter()
            .find(|peer| peer.host == leader.host && peer.raft_port == leader.port)
            .cloned()
            .ok_or(anyhow::anyhow!("no peer found for leader `{:?}`", leader))
    }

    async fn handle_request<RequestPb, ResponsePb, ToStateMachineRequest, Forward, F>(
        &self,
        request_pb: RequestPb,
        to_state_machine_request: ToStateMachineRequest,
        forward: Forward) -> Result<Response<ResponsePb>, Status>
    where
        RequestPb: Clone,
        ResponsePb: Debug,
        ToStateMachineRequest: FnOnce(RequestPb, oneshot::Sender<ResponsePb>, Span) -> CharmStateMachineRequest,
        Forward: FnOnce(CharmClient<Channel>, RequestPb) -> F,
        F: Future<Output=Result<Response<ResponsePb>, Status>>,
    {
        let (tx, rx) = oneshot::channel();
        let proposal = to_state_machine_request(request_pb.clone(), tx, Span::current());
        let commit = self.raft_handle.propose(proposal).await;
        match commit {
            Ok(()) => {
                // We got the commit! Now we wait for the state machine to respond.
                debug!("Proposal accepted! Waiting for state machine response");
                match rx.await {
                    Ok(response_pb) => {
                        debug!("State machine response: {:?}", response_pb);
                        Ok(Response::new(response_pb))
                    }
                    Err(_) => {
                        Err(Status::internal("state machine response channel closed".to_string()))
                    }
                }
            }

            Err(NotLeader { leader_id: Some(leader) }) => {
                debug!("Not the leader, forwarding to `{:?}`", leader);
                let client = self.get_client(leader.clone()).map_err(|e| Status::internal(e.to_string()))?;
                let response = forward(client, request_pb).await?;
                Ok(response)
            }

            Err(NotLeader { leader_id: None }) => {
                debug!("Not the leader and don't know who is");
                Err(Status::unavailable("not the leader and don't know who is".to_string()))
            }

            Err(NotReady) => {
                debug!("Raft not ready");
                Err(Status::unavailable("raft not ready".to_string()))
            }
            
        }
    }
    
}

#[async_trait]
impl Charm for CharmServerImpl {
    #[tracing::instrument(skip_all)]
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let request_pb = request.into_inner();
        debug!("GET: {:?}", request_pb);
        self.handle_request(
            request_pb,
            |request_pb, tx, span| CharmStateMachineRequest::Get { req: request_pb, response_tx: Some(tx), span: Some(span) },
            |mut client, req| async move { client.get(req).await }).await
    }

    #[tracing::instrument(skip_all)]
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let request_pb = request.into_inner();
        debug!("PUT: {:?}", request_pb);
        self.handle_request(
            request_pb,
            |request_pb, tx, span| CharmStateMachineRequest::Put { req: request_pb, response_tx: Some(tx), span: Some(span) },
            |mut client, req| async move { client.put(req).await }).await
    }

    #[tracing::instrument(skip_all)]
    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let request = request.into_inner();
        debug!("DELETE: {:?}", request);
        self.handle_request(
            request,
            |request_pb, tx, span| CharmStateMachineRequest::Delete { req: request_pb, response_tx: Some(tx), span: Some(span) },
            |mut client, req| async move { client.delete(req).await }).await
    }
}

pub fn run_server(config: CharmConfig, raft_handle: RaftHandle<CharmStateMachineRequest>) {
    let port = config.listen.charm_port;
    let addr = (IpAddr::from(Ipv4Addr::UNSPECIFIED), port);
    let clients = DashMap::new();
    let charm_server = CharmServerImpl { config, raft_handle, clients };

    #[cfg(not(feature = "turmoil"))]
    spawn(async move {
        tonic::transport::Server::builder()
            .add_service(CharmServer::new(charm_server))
            .serve(addr.into())
            .await
            .unwrap();
    });

    #[cfg(feature = "turmoil")]
    spawn(async move {
        tonic::transport::Server::builder()
            .add_service(CharmServer::new(charm_server))
            .serve_with_incoming(
                crate::net::make_incoming(addr)
            ).await
            .unwrap();
    });
}