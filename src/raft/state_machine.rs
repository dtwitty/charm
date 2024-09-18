use crate::raft::types::RaftInfo;
use tokio::spawn;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tonic::async_trait;

#[async_trait]
pub trait StateMachine: Send + 'static {
    type Request: Send + 'static;
    async fn apply(&mut self, request: Self::Request, raft_info: RaftInfo);
}

pub struct StateMachineHandle<R> {
    tx: UnboundedSender<(R, RaftInfo)>
}

impl<R> StateMachineHandle<R> {
    #[must_use]
    pub fn new(tx: UnboundedSender<(R, RaftInfo)>) -> Self {
        Self { tx }
    }

    pub fn apply(&self, request: R, raft_info: RaftInfo) {
        self.tx.send((request, raft_info)).unwrap();
    }
}

#[tracing::instrument(skip_all)]
async fn run<S>(mut state_machine: S, mut rx: UnboundedReceiver<(S::Request, RaftInfo)>)
where
    S: StateMachine,
{
    while let Some((request, raft_info)) = rx.recv().await {
        state_machine.apply(request, raft_info).await;
    }
}

pub fn run_state_machine_driver<S>(state_machine: S, rx: UnboundedReceiver<(S::Request, RaftInfo)>)
where
    S: StateMachine,
{
    spawn(run(state_machine, rx));
}

