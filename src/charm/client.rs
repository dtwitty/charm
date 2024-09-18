use crate::charm::pb::charm_client::CharmClient;
use crate::charm::pb::{DeleteRequest, DeleteResponse, GetRequest, GetResponse, PutRequest, PutResponse};
use crate::charm::retry::{RetryStrategy, RetryStrategyBuilder, RetryStrategyIterator};
use failsafe::failure_policy::{success_rate_over_time_window, SuccessRateOverTimeWindow};
use failsafe::{Config, Instrument, StateMachine};
use std::time::Duration;
use tokio_retry::Retry;
use tonic::transport::Channel;
use tonic::Response;
use tracing::{debug, info, instrument, warn};

#[derive(Clone)]
pub struct EasyCharmClient {
    addr:String,
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
        Ok(EasyCharmClient {
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
            let mut request = tonic::Request::new(GetRequest { key: key.clone() });
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
            let mut request = tonic::Request::new(PutRequest { key: key.clone(), value: value.clone() });
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
            let mut request = tonic::Request::new(DeleteRequest { key: key.clone() });
            request.set_timeout(Duration::from_secs(1));
            let result = self.client.clone().delete(request).await;
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
}

fn make_charm_client(addr: String) -> anyhow::Result<CharmClient<Channel>> {
    let endpoint = Channel::from_shared(addr)?;
    #[cfg(not(feature = "turmoil"))]
    let channel = endpoint.connect_lazy();

    #[cfg(feature = "turmoil")]
    let channel = endpoint.connect_with_connector_lazy(crate::net::connector::TurmoilTcpConnector);

    Ok(CharmClient::new(channel))
}
