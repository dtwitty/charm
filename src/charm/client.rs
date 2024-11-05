use crate::charm::pb::charm_client::CharmClient;
use crate::charm::pb::{ClientId, DeleteRequest, DeleteResponse, GetRequest, GetResponse, PutRequest, PutResponse, RequestHeader};
use crate::charm::retry::RetryStrategy;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_retry::Retry;
use tonic::transport::Channel;
use tracing::{debug, instrument};
use uuid::Uuid;

#[derive(Clone)]
pub struct EasyCharmClient {
    client_id: Uuid,
    request_number: Arc<AtomicU64>,
    outstanding_requests: Arc<Mutex<BTreeSet<u64>>>,
    addr: String,
    client: CharmClient<Channel>,
    retry_strategy: RetryStrategy,
}

impl EasyCharmClient {
    pub fn new(addr: String, retry_strategy: RetryStrategy) -> anyhow::Result<Self> {
        let client = make_charm_client(addr.clone())?;
        let client_id = Uuid::new_v4();
        let request_number = Arc::new(AtomicU64::new(0));
        let outstanding_requests = Arc::new(Mutex::new(BTreeSet::new()));
        Ok(EasyCharmClient {
            client_id,
            request_number,
            outstanding_requests,
            addr,
            client,
            retry_strategy,
        })
    }

    #[instrument(fields(addr=self.addr.clone()), skip(self))]
    pub async fn get(&self, key: String) -> anyhow::Result<GetResponse> {
        let retry_strategy = self.retry_strategy.clone();
        let request_header = self.make_request_header();
        let request_pb = GetRequest { request_header, key: key.clone() };
        let attempt = AtomicU64::new(0);
        Retry::spawn(retry_strategy, || async {
            let attempt = attempt.fetch_add(1, Ordering::Relaxed);
            debug!("Attempt {} of request {}", attempt, request_header.unwrap().request_number);
            let mut request = tonic::Request::new(request_pb.clone());
            request.set_timeout(Duration::from_secs(1));
            let result = self.client.clone().get(request).await?;
            self.on_success(request_header.unwrap().request_number);
            Ok(result.into_inner())
        }).await
    }

    #[instrument(fields(addr=self.addr.clone()), skip(self))]
    pub async fn put(&self, key: String, value: String) -> anyhow::Result<PutResponse> {
        let retry_strategy = self.retry_strategy.clone();
        let request_header = self.make_request_header();
        let request_pb = PutRequest { request_header, key: key.clone(), value: value.clone() };
        let attempt = AtomicU64::new(0);
        Retry::spawn(retry_strategy, || async {
            let attempt = attempt.fetch_add(1, Ordering::Relaxed);
            debug!("Attempt {} of request {}", attempt, request_header.unwrap().request_number);
            let mut request = tonic::Request::new(request_pb.clone());
            request.set_timeout(Duration::from_secs(1));
            let result = self.client.clone().put(request).await?;
            self.on_success(request_header.unwrap().request_number);
            Ok(result.into_inner())
        }).await
    }

    #[instrument(fields(addr=self.addr.clone()), skip(self))]
    pub async fn delete(&self, key: String) -> anyhow::Result<DeleteResponse> {
        let retry_strategy = self.retry_strategy.clone();
        let request_header = self.make_request_header();
        let request_pb = DeleteRequest { request_header, key: key.clone() };
        let attempt = AtomicU64::new(0);
        Retry::spawn(retry_strategy, || async {
            let attempt = attempt.fetch_add(1, Ordering::Relaxed);
            debug!("Attempt {} of request {}", attempt, request_header.unwrap().request_number);
            let mut request = tonic::Request::new(request_pb.clone());
            request.set_timeout(Duration::from_secs(1));
            let result = self.client.clone().delete(request).await?;
            self.on_success(request_header.unwrap().request_number);
            Ok(result.into_inner())
        }).await
    }

    fn make_request_header(&self) -> Option<RequestHeader> {
        // Create the client ID.
        let (hi_bits, lo_bits) = self.client_id.as_u64_pair();
        let client_id = Some(ClientId {
            lo_bits,
            hi_bits,
        });

        // Give this request a unique number.
        let request_number = self.request_number.fetch_add(1, Ordering::Relaxed);

        // Add this request to the set of outstanding requests.
        let mut m = self.outstanding_requests.lock().unwrap();
        m.insert(request_number);

        // Find the first incomplete request number.
        let first_incomplete_request_number = *m.iter().next().expect("This should certainly exist");

        Some(RequestHeader {
            client_id,
            request_number,
            first_incomplete_request_number,
        })
    }

    fn on_success(&self, request_number: u64) {
        // Mark this request as complete.
        let mut m = self.outstanding_requests.lock().unwrap();
        m.remove(&request_number);
    }
}

pub fn make_charm_client(addr: String) -> anyhow::Result<CharmClient<Channel>> {
    let endpoint = Channel::from_shared(addr)?;
    #[cfg(not(feature = "turmoil"))]
    let channel = endpoint.connect_lazy();

    #[cfg(feature = "turmoil")]
    let channel = endpoint.connect_with_connector_lazy(crate::net::connector::connector());

    Ok(CharmClient::new(channel))
}
