use crate::charm::pb::charm_client::CharmClient;
use crate::charm::pb::{ClientId, DeleteRequest, DeleteResponse, GetRequest, GetResponse, PutRequest, PutResponse, RequestHeader};
use crate::charm::retry::{RetryStrategy, RetryStrategyBuilder, RetryStrategyIterator};
use crate::net::connector;
use failsafe::failure_policy::{success_rate_over_time_window, SuccessRateOverTimeWindow};
use failsafe::{Config, Instrument, StateMachine};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_retry::Retry;
use tonic::transport::Channel;
use tonic::Response;
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

#[derive(Clone)]
pub struct EasyCharmClient {
    client_id: Uuid,
    request_number: Arc<AtomicU64>,
    outstanding_requests: Arc<Mutex<BTreeSet<u64>>>,
    addr: String,
    client: CharmClient<Channel>,
    retry_strategy: RetryStrategy,
    circuit_breaker: StateMachine<SuccessRateOverTimeWindow<RetryStrategyIterator>, CircuitBreakerInstrument>,
}

struct CircuitBreakerInstrument {}

impl Instrument for CircuitBreakerInstrument {
    fn on_call_rejected(&self) {
        warn!("Circuit breaker rejected a call");
    }

    fn on_open(&self) {
        warn!("Circuit breaker is open");
    }

    fn on_half_open(&self) {
        warn!("Circuit breaker is half open");
    }

    fn on_closed(&self) {
        info!("Circuit breaker is closed");
    }
}

impl EasyCharmClient {
    pub fn new(addr: String, retry_strategy: RetryStrategy) -> anyhow::Result<Self> {
        let client = make_charm_client(addr.clone())?;
        let window = Duration::from_secs(20);

        let backoff = RetryStrategyBuilder::default()
            .rng(retry_strategy.clone_rng())
            .initial_delay(Duration::from_secs(1))
            .max_delay(Duration::from_secs(3))
            .build()?;
        let failure_policy = success_rate_over_time_window(0.1, 1000, window, backoff.into_iter());
        let circuit_breaker = Config::new()
            .failure_policy(failure_policy)
            .instrument(CircuitBreakerInstrument {})
            .build();
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
            circuit_breaker,
        })
    }

    #[instrument(fields(addr=self.addr.clone()), skip_all)]
    pub async fn get(&self, key: String) -> anyhow::Result<GetResponse> {
        debug!("Getting key: {}", key);
        let retry_strategy = self.retry_strategy.clone();
        let request_header = self.make_request_header();
        let request_pb = GetRequest { request_header, key: key.clone() };
        Retry::spawn(retry_strategy, || async {
            self.check_circuit_breaker()?;
            let mut request = tonic::Request::new(request_pb.clone());
            request.set_timeout(Duration::from_secs(1));
            let result = self.client.clone().get(request).await;
            let response = self.instrument_response(result)?;
            self.on_success(request_header.unwrap().request_number);
            Ok(response)
        }).await
    }

    #[instrument(fields(addr=self.addr.clone()), skip_all)]
    pub async fn put(&self, key: String, value: String) -> anyhow::Result<PutResponse> {
        debug!("Putting key: {} value: {}", key, value);
        let retry_strategy = self.retry_strategy.clone();
        let request_header = self.make_request_header();
        let request_pb = PutRequest { request_header, key: key.clone(), value: value.clone() };
        Retry::spawn(retry_strategy, || async {
            self.check_circuit_breaker()?;
            let mut request = tonic::Request::new(request_pb.clone());
            request.set_timeout(Duration::from_secs(1));
            let result = self.client.clone().put(request).await;
            let response = self.instrument_response(result)?;
            self.on_success(request_header.unwrap().request_number);
            Ok(response)
        }).await
    }

    #[instrument(fields(addr=self.addr.clone()), skip_all)]
    pub async fn delete(&self, key: String) -> anyhow::Result<DeleteResponse> {
        debug!("Deleting key: {}", key);
        let retry_strategy = self.retry_strategy.clone();
        let request_header = self.make_request_header();
        let request_pb = DeleteRequest { request_header, key: key.clone() };
        Retry::spawn(retry_strategy, || async {
            self.check_circuit_breaker()?;
            let mut request = tonic::Request::new(request_pb.clone());
            request.set_timeout(Duration::from_secs(1));
            let result = self.client.clone().delete(request).await;
            let response = self.instrument_response(result)?;
            self.on_success(request_header.unwrap().request_number);
            Ok(response)
        }).await
    }

    fn check_circuit_breaker(&self) -> anyhow::Result<()> {
        if !self.circuit_breaker.is_call_permitted() {
            return Err(anyhow::anyhow!("Circuit breaker is open"));
        }
        Ok(())
    }

    fn instrument_response<T, E>(&self, result: Result<Response<T>, E>) -> Result<T, E> {
        match result {
            Ok(v) => {
                self.circuit_breaker.on_success();
                Ok(v.into_inner())
            }
            Err(e) => {
                self.circuit_breaker.on_error();
                Err(e)
            }
        }
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
        let first_incomplete_request_number = m.iter().next().expect("This should certainly exist").clone();

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
    let channel = endpoint.connect_with_connector_lazy(connector::connector());

    Ok(CharmClient::new(channel))
}
