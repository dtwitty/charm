use crate::charm::pb::charm_client::CharmClient;
use crate::charm::pb::{DeleteRequest, GetRequest, PutRequest};
use crate::charm::retry::RetryStrategy;
use failsafe::backoff::{equal_jittered, EqualJittered};
use failsafe::failure_policy::{success_rate_over_time_window, SuccessRateOverTimeWindow};
use failsafe::{Config, StateMachine};
use std::time::Duration;
use tokio_retry::Retry;
use tonic::transport::Channel;
use tonic::Response;

#[derive(Clone)]
pub struct EasyCharmClient {
    client: CharmClient<Channel>,
    retry_strategy: RetryStrategy,
    circuit_breaker: StateMachine<SuccessRateOverTimeWindow<EqualJittered>, ()>,
}

impl EasyCharmClient {
    pub fn new(addr: String) -> anyhow::Result<Self> {
        let retry_strategy = RetryStrategy::new();
        Self::with_retry_strategy(addr, retry_strategy)
    }

    pub fn with_retry_strategy(addr: String, retry_strategy: RetryStrategy) -> anyhow::Result<Self> {
        let client = make_charm_client(addr)?;
        let window = Duration::from_secs(20);
        let backoff = equal_jittered(Duration::from_secs(1), Duration::from_secs(5));
        let failure_policy = success_rate_over_time_window(0.9, 20, window, backoff);
        let circuit_breaker = Config::new().failure_policy(failure_policy).build();
        Ok(EasyCharmClient {
            client,
            retry_strategy,
            circuit_breaker,
        })
    }


    pub async fn get(&self, key: String) -> anyhow::Result<Option<String>> {
        let retry_strategy = self.retry_strategy.clone();
        Retry::spawn(retry_strategy, || async {
            self.check_circuit_breaker()?;
            let request = tonic::Request::new(GetRequest { key: key.clone() });
            let result = self.client.clone().get(request).await;
            let response = self.instrument_response(result)?;
            Ok(response.value)
        }).await
    }

    pub async fn put(&self, key: String, value: String) -> anyhow::Result<()> {
        let retry_strategy = self.retry_strategy.clone();
        Retry::spawn(retry_strategy, || async {
            self.check_circuit_breaker()?;
            let request = tonic::Request::new(PutRequest { key: key.clone(), value: value.clone() });
            let result = self.client.clone().put(request).await;
            self.instrument_response(result)?;
            Ok(())
        }).await
    }

    pub async fn delete(&self, key: String) -> anyhow::Result<()> {
        let retry_strategy = self.retry_strategy.clone();
        Retry::spawn(retry_strategy, || async {
            self.check_circuit_breaker()?;
            let request = tonic::Request::new(DeleteRequest { key: key.clone() });
            let result = self.client.clone().delete(request).await;
            self.instrument_response(result)?;
            Ok(())
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
