use crate::charm::pb::charm_client::CharmClient;
use crate::charm::pb::{ClientId, DeleteRequest, DeleteResponse, GetRequest, GetResponse, PutRequest, PutResponse, RequestHeader};
use crate::charm::retry::{RetryStrategy, RetryStrategyBuilder, RetryStrategyIterator};
use failsafe::failure_policy::{success_rate_over_time_window, SuccessRateOverTimeWindow};
use failsafe::{Config, Instrument, StateMachine};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
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
            .max_delay(Duration::from_secs(10))
            .build()?;
        let failure_policy = success_rate_over_time_window(0.9, 20, window, backoff.into_iter());
        let circuit_breaker = Config::new()
            .failure_policy(failure_policy)
            .instrument(CircuitBreakerInstrument {})
            .build();
        let client_id = Uuid::new_v4();
        let request_number = Arc::new(AtomicU64::new(0));
        Ok(EasyCharmClient {
            client_id,
            request_number,
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
        Retry::spawn(retry_strategy, || async {
            self.check_circuit_breaker()?;
            let request_header = self.make_request_header();
            let mut request = tonic::Request::new(GetRequest { request_header, key: key.clone() });
            request.set_timeout(Duration::from_secs(1));
            let result = self.client.clone().get(request).await;
            let response = self.instrument_response(result)?;
            Ok(response)
        }).await
    }

    #[instrument(fields(addr=self.addr.clone()), skip_all)]
    pub async fn put(&self, key: String, value: String) -> anyhow::Result<PutResponse> {
        debug!("Putting key: {} value: {}", key, value);
        let retry_strategy = self.retry_strategy.clone();
        Retry::spawn(retry_strategy, || async {
            self.check_circuit_breaker()?;
            let request_header = self.make_request_header();
            let mut request = tonic::Request::new(PutRequest { request_header, key: key.clone(), value: value.clone() });
            request.set_timeout(Duration::from_secs(1));
            let result = self.client.clone().put(request).await;
            let response = self.instrument_response(result)?;
            Ok(response)
        }).await
    }

    #[instrument(fields(addr=self.addr.clone()), skip_all)]
    pub async fn delete(&self, key: String) -> anyhow::Result<DeleteResponse> {
        debug!("Deleting key: {}", key);
        let retry_strategy = self.retry_strategy.clone();
        Retry::spawn(retry_strategy, || async {
            self.check_circuit_breaker()?;
            let request_header = self.make_request_header();
            let mut request = tonic::Request::new(DeleteRequest { request_header, key: key.clone() });
            request.set_timeout(Duration::from_secs(1));
            let result = self.client.clone().delete(request).await;
            let response = self.instrument_response(result)?;
            Ok(response)
        }).await
    }

    #[instrument(fields(addr=self.addr.clone()), skip_all)]
    pub async fn history(&self, key: String) -> anyhow::Result<crate::charm::pb::HistoryResponse> {
        debug!("Getting history for key: {}", key);
        let retry_strategy = self.retry_strategy.clone();
        Retry::spawn(retry_strategy, || async {
            self.check_circuit_breaker()?;
            let request_header = self.make_request_header();
            let mut request = tonic::Request::new(crate::charm::pb::HistoryRequest { request_header, key: key.clone() });
            request.set_timeout(Duration::from_secs(1));
            let result = self.client.clone().history(request).await;
            let response = self.instrument_response(result)?;
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
        let (hi_bits, lo_bits) = self.client_id.as_u64_pair();
        let client_id = Some(ClientId {
            lo_bits,
            hi_bits,
        });
        let request_number = self.request_number.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Some(RequestHeader {
            client_id,
            request_number,
        })
    }
}

pub fn make_charm_client(addr: String) -> anyhow::Result<CharmClient<Channel>> {
    let endpoint = Channel::from_shared(addr)?;
    #[cfg(not(feature = "turmoil"))]
    let channel = endpoint.connect_lazy();

    #[cfg(feature = "turmoil")]
    let channel = endpoint.connect_with_connector_lazy(crate::net::connector::TurmoilTcpConnector);

    Ok(CharmClient::new(channel))
}
