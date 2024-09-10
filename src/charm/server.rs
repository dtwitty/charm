use crate::charm::client::EasyCharmClient;
use crate::charm::config::CharmConfig;
use crate::charm::pb::charm_server::{Charm, CharmServer};
use crate::charm::pb::{DeleteRequest, DeleteResponse, GetRequest, GetResponse, PutRequest, PutResponse};
use crate::charm::retry::RetryStrategyBuilder;
use crate::charm::state_machine::CharmStateMachineRequest;
use crate::raft::core::error::RaftCoreError::NotLeader;
use crate::raft::types::NodeId;
use crate::raft::RaftHandle;
use crate::rng::CharmRng;
use crate::server::CharmPeer;
use dashmap::DashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;
use tokio::spawn;
use tokio::sync::oneshot;
use tonic::{async_trait, Request, Response, Status};
use tracing::{debug, Span};

struct CharmServerImpl {
    config: CharmConfig,
    raft_handle: RaftHandle<CharmStateMachineRequest>,
    clients: DashMap<NodeId, EasyCharmClient>,
    rng: CharmRng,
}

impl CharmServerImpl {
    fn get_client(&self, leader: NodeId) -> anyhow::Result<EasyCharmClient> {
        // Get or create a client for the leader.
        let entry = self.clients.entry(leader.clone());
        let client_ref_res = entry.or_try_insert_with(|| {
            // We use a tight retry strategy here because we are forwarding the request to the leader.
            // If the leader is unreachable, it probably won't be the leader for long.
            let retry_strategy = RetryStrategyBuilder::default()
                .rng(self.rng.clone())
                .total_retry_time(Duration::from_secs(1))
                .build().unwrap();
            let peer = self.get_leader_peer(&leader)?;
            let addr = peer.charm_addr();
            EasyCharmClient::new(addr, retry_strategy)
        });
        let client= client_ref_res.map_err(|e| Status::internal(e.to_string()))?;
        // Clone and drop the ref to avoid holding the lock.
        Ok(client.clone())
    }

    fn get_leader_peer(&self, leader: &NodeId) -> anyhow::Result<CharmPeer> {
        // Get the host off the leader node ID.
        // Look in the config for a peer with the same host.
        self.config.peers
            .iter()
            .find(|peer| peer.host == leader.host)
            .cloned()
            .ok_or(anyhow::anyhow!("no peer found for leader host `{}`", leader.host))
    }
}

#[async_trait]
impl Charm for CharmServerImpl {
    #[tracing::instrument(skip_all)]
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let request = request.into_inner();
        debug!("Get `{:?}`", request.key);
        let key = request.key;
        let (tx, rx) = oneshot::channel();
        let commit = self.raft_handle
            .propose(CharmStateMachineRequest::Get { key: key.clone(), response: Some(tx), span: Some(Span::current()) })
            .await.unwrap();

        match commit {
            Ok(()) => {
                // Great success!
                let value = rx.await.unwrap();
                Ok(Response::new(GetResponse { value }))
            }

            Err(NotLeader { leader_id: Some(leader) }) => {
                debug!("Not the leader, forwarding to `{:?}`", leader);
                let client = self.get_client(leader.clone()).map_err(|e| Status::internal(e.to_string()))?;
                // Forward the request to the leader.
                let response = client.get(key).await.map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(GetResponse { value: response }))
            }

            Err(NotLeader { leader_id: None }) => {
                debug!("Not the leader and don't know who is");
                Err(Status::unavailable("not the leader and don't know who is".to_string()))
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let request = request.into_inner();
        let key = request.key;
        let value = request.value;
        let (tx, rx) = oneshot::channel();
        let commit = self.raft_handle
            .propose(CharmStateMachineRequest::Set { key: key.clone(), value: value.clone(), response: Some(tx), span: Some(Span::current()) })
            .await.unwrap();

        match commit {
            Ok(()) => {
                // Great success!
                rx.await.unwrap();
                Ok(Response::new(PutResponse {}))
            }

            Err(NotLeader { leader_id: Some(leader) }) => {
                debug!("Not the leader, forwarding to `{:?}`", leader);
                let client = self.get_client(leader.clone()).map_err(|e| Status::internal(e.to_string()))?;
                // Forward the request to the leader.
                client.put(key, value).await.map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(PutResponse {}))
            }

            Err(NotLeader { leader_id: None }) => {
                debug!("Not the leader and don't know who is");
                Err(Status::unavailable("not the leader and don't know who is".to_string()))
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let request = request.into_inner();
        let key = request.key;
        let (tx, rx) = oneshot::channel();
        let commit = self.raft_handle
            .propose(CharmStateMachineRequest::Delete { key: key.clone(), response: Some(tx), span: Some(Span::current()) })
            .await.unwrap();

        match commit {
            Ok(()) => {
                // Great success!
                rx.await.unwrap();
                Ok(Response::new(DeleteResponse {}))
            }

            Err(NotLeader { leader_id: Some(leader) }) => {
                debug!("Not the leader, forwarding to `{:?}`", leader);
                let client = self.get_client(leader.clone()).map_err(|e| Status::internal(e.to_string()))?;
                // Forward the request to the leader.
                client.delete(key).await.map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(DeleteResponse {}))
            }

            Err(NotLeader { leader_id: None }) => {
                debug!("Not the leader and don't know who is");
                Err(Status::unavailable("not the leader and don't know who is".to_string()))
            }
        }
    }
}

pub fn run_server(config: CharmConfig, raft_handle: RaftHandle<CharmStateMachineRequest>, rng: CharmRng) {
    let port = config.listen.charm_port;
    let addr = (IpAddr::from(Ipv4Addr::UNSPECIFIED), port);
    let clients = DashMap::new();
    let charm_server = CharmServerImpl { config, raft_handle, clients, rng };

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