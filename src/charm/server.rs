use crate::charm::client::make_charm_client;
use crate::charm::config::CharmConfig;
use crate::charm::pb;
use crate::charm::pb::charm_client::CharmClient;
use crate::charm::pb::charm_server::{Charm, CharmServer};
use crate::charm::pb::event::Event::{Delete, Put};
use crate::charm::pb::{DeleteEvent, DeleteRequest, DeleteResponse, GetRequest, GetResponse, HistoryRequest, HistoryResponse, PutEvent, PutRequest, PutResponse, ResponseHeader};
use crate::charm::state_machine::{CharmStateMachineRequest, Event};
use crate::raft::core::error::RaftCoreError::NotLeader;
use crate::raft::types::{NodeId, RaftInfo};
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

    fn get_response_header(&self, raft_info: &RaftInfo) -> ResponseHeader {
        let leader_peer = self.get_leader_peer(&raft_info.node_id).unwrap();
        ResponseHeader {
            leader_id: format!("{}:{}", leader_peer.host, leader_peer.charm_port),
            raft_term: raft_info.term.0,
            raft_index: raft_info.index.0,
        }
    }

    fn event_to_pb(&self, event: Event) -> pb::Event {
        match event {
            Event::Put { value, raft_info } => {
                let response_header = Some(self.get_response_header(&raft_info));
                pb::Event { event: Some(Put(PutEvent { request_header: None, response_header, value })) }
            },

            Event::Delete { raft_info } => {
                let response_header = Some(self.get_response_header(&raft_info));
                pb::Event { event: Some(Delete(DeleteEvent { request_header: None, response_header })) }
            }
        }
    }

    fn history_to_pb(&self, history: Vec<Event>) -> Vec<pb::Event> {
        history.into_iter().map(|event| self.event_to_pb(event)).collect()
    }

    async fn handle_request<RequestPb, ResponsePb, ToStateMachineRequest, StateMachineResponse, ToResponsePb, Forward, F>(
        &self,
        request_pb: RequestPb,
        to_state_machine_request: ToStateMachineRequest,
        to_response_pb: ToResponsePb,
        forward: Forward) -> Result<Response<ResponsePb>, Status>
    where
        RequestPb: Clone,
        ToStateMachineRequest: FnOnce(RequestPb, oneshot::Sender<StateMachineResponse>, Span) -> CharmStateMachineRequest,
        ToResponsePb: FnOnce(StateMachineResponse) -> ResponsePb,
        Forward: FnOnce(CharmClient<Channel>, RequestPb) -> F,
        F: Future<Output=Result<Response<ResponsePb>, Status>>,
    {
        let (tx, rx) = oneshot::channel();
        let proposal = to_state_machine_request(request_pb.clone(), tx, Span::current());
        let commit = self.raft_handle.propose(proposal).await.unwrap();
        match commit {
            Ok(()) => {
                // Great success!
                let state_machine_response = rx.await.unwrap();
                let response_pb = to_response_pb(state_machine_response);
                Ok(Response::new(response_pb))
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
            |request_pb, tx, span| CharmStateMachineRequest::Get { key: request_pb.key, response: Some(tx), span: Some(span) },
            |response| {
                let response_header = Some(self.get_response_header(&response.raft_info));
                GetResponse { response_header, value: response.value }
            },
            |mut client, req| async move { client.get(req).await }).await
    }

    #[tracing::instrument(skip_all)]
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let request_pb = request.into_inner();
        debug!("PUT: {:?}", request_pb);
        self.handle_request(
            request_pb,
            |request_pb, tx, span| CharmStateMachineRequest::Set { key: request_pb.key, value: request_pb.value, response: Some(tx), span: Some(span) },
            |response| {
                let response_header = Some(self.get_response_header(&response.raft_info));
                PutResponse { response_header }
            },
            |mut client, req| async move { client.put(req).await }).await
    }

    #[tracing::instrument(skip_all)]
    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let request = request.into_inner();
        debug!("DELETE: {:?}", request);
        self.handle_request(
            request,
            |request_pb, tx, span| CharmStateMachineRequest::Delete { key: request_pb.key, response: Some(tx), span: Some(span) },
            |response| {
                let response_header = Some(self.get_response_header(&response.raft_info));
                DeleteResponse { response_header }
            },
            |mut client, req| async move { client.delete(req).await }).await
    }

    #[tracing::instrument(skip_all)]
    async fn history(&self, request: Request<HistoryRequest>) -> Result<Response<HistoryResponse>, Status> {
        let request = request.into_inner();
        debug!("HISTORY: {:?}", request);
        self.handle_request(
            request,
            |request_pb, tx, span| CharmStateMachineRequest::History { key: request_pb.key, response: Some(tx), span: Some(span) },
            |response| {
                let response_header = Some(self.get_response_header(&response.raft_info));
                let events = self.history_to_pb(response.events);
                HistoryResponse { response_header, events }
            },
            |mut client, req| async move { client.history(req).await }).await
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