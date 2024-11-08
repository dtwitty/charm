use crate::raft::types::RaftInfo;
use tokio::spawn;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tonic::async_trait;
use tracing::debug;

#[async_trait]
pub trait StateMachine<I>: Send + 'static {
    type Request: Send + 'static;
    async fn apply(&mut self, request: Self::Request, raft_info: RaftInfo<I>);
}

pub struct StateMachineHandle<R, I> {
    tx: UnboundedSender<(R, RaftInfo<I>)>
}

impl<R, I> StateMachineHandle<R, I> {
    #[must_use]
    pub fn new(tx: UnboundedSender<(R, RaftInfo<I>)>) -> Self {
        Self { tx }
    }

    pub fn apply(&self, request: R, raft_info: RaftInfo<I>) {
        debug!("Applying index {} in term {} to the state machine", raft_info.index.0, raft_info.term.0);
        self.tx.send((request, raft_info)).unwrap();
    }
}

async fn run<S, I>(mut state_machine: S, mut rx: UnboundedReceiver<(S::Request, RaftInfo<I>)>)
where
    S: StateMachine<I>,
{
    while let Some((request, raft_info)) = rx.recv().await {
        state_machine.apply(request, raft_info).await;
    }
}

pub fn run_state_machine_driver<S, I>(state_machine: S, rx: UnboundedReceiver<(S::Request, RaftInfo<I>)>)
where
    S: StateMachine<I>,
    I: Send + 'static,
{
    spawn(run(state_machine, rx));
}

