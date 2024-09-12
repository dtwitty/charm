use tokio::spawn;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tonic::async_trait;

#[async_trait]
pub trait StateMachine: Send + 'static {
    type Request: Send + 'static;
    async fn apply(&mut self, request: Self::Request);
}

pub struct StateMachineHandle<R> {
    tx: UnboundedSender<R>
}

impl<R> StateMachineHandle<R> {
    #[must_use] pub fn new(tx: UnboundedSender<R>) -> Self {
        Self { tx }
    }

    pub fn apply(&self, request: R) {
        self.tx.send(request).unwrap();
    }
}

#[tracing::instrument(skip_all)]
async fn run<S>(mut state_machine: S, mut rx: UnboundedReceiver<S::Request>)
where
    S: StateMachine,
{
    while let Some(request) = rx.recv().await {
        state_machine.apply(request).await;
    }
}

pub fn run_state_machine_driver<S>(state_machine: S, rx: UnboundedReceiver<S::Request>)
where
    S: StateMachine,
{
    spawn(run(state_machine, rx));
}

