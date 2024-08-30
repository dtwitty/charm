use tokio::spawn;
use tokio::sync::mpsc::UnboundedReceiver;
use tonic::async_trait;

#[async_trait]
pub trait StateMachine: Send + 'static {
    type Request: Send + 'static;
    async fn apply(&mut self, request: Self::Request);
}

pub fn run_state_machine_driver<S>(mut state_machine: S, mut rx: UnboundedReceiver<S::Request>)
where
    S: StateMachine,
{
    spawn(async move {
       while let Some(request) = rx.recv().await {
            state_machine.apply(request).await;
        }
    });
}

