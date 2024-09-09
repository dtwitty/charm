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
use dashmap::DashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;
use tokio::spawn;
use tokio::sync::oneshot;
use tonic::{async_trait, Request, Response, Status};

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
            let addr = self.get_addr_for_leader(&leader)?;
            EasyCharmClient::new(addr, retry_strategy)
        });
        let client_or_status = client_ref_res.map_err(|_| Status::internal(format!("leader address {} is not valid", leader.0)));
        let client = client_or_status?;
        // Clone and drop the ref to avoid holding the lock.
        Ok(client.clone())
    }

    fn get_addr_for_leader(&self, leader: &NodeId) -> anyhow::Result<String> {
        // Get the host off the leader node ID.
        let leader_host = leader.0.split(':').next().unwrap();
        // Look in the config for a peer with the same host.
        self.config.peer_addrs
            .iter()
            .find(|peer| peer.starts_with(leader_host))
            .cloned()
            .ok_or(anyhow::anyhow!("no peer found for leader `{}`", leader.0))
    }
}

#[async_trait]
impl Charm for CharmServerImpl {
    #[tracing::instrument(level = "debug", skip(self))]
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let request = request.into_inner();
        let key = request.key;
        let (tx, rx) = oneshot::channel();
        let commit = self.raft_handle
            .propose(CharmStateMachineRequest::Get { key: key.clone(), response: Some(tx) })
            .await.unwrap();

        match commit {
            Ok(()) => {
                // Great success!
                let value = rx.await.unwrap();
                Ok(Response::new(GetResponse { value }))
            }

            Err(NotLeader { leader_id: Some(leader) }) => {
                // We are not the leader, but we know who is.
                let client = self.get_client(leader.clone()).map_err(|_| Status::internal(format!("failed to get client to forward request to leader `{}`", leader.0)))?;
                // Forward the request to the leader.
                let response = client.get(key).await.map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(GetResponse { value: response }))
            }

            Err(NotLeader { leader_id: None }) => {
                // We are not the leader, and we don't know who is.
                Err(Status::unavailable("not the leader and don't know who is".to_string()))
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let request = request.into_inner();
        let key = request.key;
        let value = request.value;
        let (tx, rx) = oneshot::channel();
        let commit = self.raft_handle
            .propose(CharmStateMachineRequest::Set { key: key.clone(), value: value.clone(), response: Some(tx) })
            .await.unwrap();

        match commit {
            Ok(()) => {
                // Great success!
                rx.await.unwrap();
                Ok(Response::new(PutResponse {}))
            }

            Err(NotLeader { leader_id: Some(leader) }) => {
                // We are not the leader, but we know who is.
                let client = self.get_client(leader.clone()).map_err(|_| Status::internal(format!("failed to get client to forward request to leader `{}`", leader.0)))?;
                // Forward the request to the leader.
                client.put(key, value).await.map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(PutResponse {}))
            }

            Err(NotLeader { leader_id: None }) => {
                // We are not the leader, and we don't know who is.
                Err(Status::unavailable("not the leader and don't know who is".to_string()))
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let request = request.into_inner();
        let key = request.key;
        let (tx, rx) = oneshot::channel();
        let commit = self.raft_handle
            .propose(CharmStateMachineRequest::Delete { key: key.clone(), response: Some(tx) })
            .await.unwrap();

        match commit {
            Ok(()) => {
                // Great success!
                rx.await.unwrap();
                Ok(Response::new(DeleteResponse {}))
            }

            Err(NotLeader { leader_id: Some(leader) }) => {
                // We are not the leader, but we know who is.
                let client = self.get_client(leader.clone()).map_err(|_| Status::internal(format!("failed to get client to forward request to leader `{}`", leader.0)))?;
                // Forward the request to the leader.
                client.delete(key).await.map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(DeleteResponse {}))
            }

            Err(NotLeader { leader_id: None }) => {
                // We are not the leader, and we don't know who is.
                Err(Status::unavailable("not the leader and don't know who is".to_string()))
            }
        }
    }
}

pub fn run_server(config: CharmConfig, raft_handle: RaftHandle<CharmStateMachineRequest>, rng: CharmRng) {
    let port = config.listen_addr.split(':').last().unwrap().parse().unwrap();
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