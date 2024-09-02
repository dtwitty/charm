use crate::raft::core::config::RaftConfig;
use crate::raft::core::error::RaftCoreError;
use crate::raft::core::handle::RaftCoreHandle;
use crate::raft::core::run_core;
use crate::raft::network::inbound_network::run_inbound_network;
use crate::raft::network::outbound_network::{run_outbound_network, OutboundNetworkHandle};
use crate::raft::state_machine::{run_state_machine_driver, StateMachine, StateMachineHandle};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::oneshot;

pub mod messages;
pub mod types;
mod pb;
pub mod core;
pub mod network;
pub mod state_machine;

#[derive(Debug)]
pub struct RaftHandle<R: Send + 'static> {
    core_handle: RaftCoreHandle<R>,
}

impl<R: Send + 'static> Clone for RaftHandle<R> {
    fn clone(&self) -> Self {
        Self {
            core_handle: self.core_handle.clone(),
        }
    }
}

impl<R: Send + 'static> RaftHandle<R> {
    /// Propose a new entry to be replicated across the cluster. The returned
    /// `oneshot::Receiver` will be signalled when the proposal has been
    /// committed. Note that this does not mean the proposal has been applied
    /// to the state machine. It is up to the caller to pass their own method of handle
    /// the result of the proposal being applied to the state machine.
    pub fn propose(&self, proposal: R) -> oneshot::Receiver<Result<(), RaftCoreError>> {
        self.core_handle.propose(proposal)
    }
}

pub fn run_raft<S: StateMachine>(config: RaftConfig, state_machine: S) -> RaftHandle<S::Request>
where
    S::Request: Serialize + DeserializeOwned + Send + 'static,
{
    let (to_core_tx, to_core_rx) = unbounded_channel();
    let raft_handle = RaftHandle { core_handle: RaftCoreHandle::new(config.node_id.clone(), to_core_tx) };
    let (to_outbound_tx, to_outbound_rx) = unbounded_channel();
    let outbound_network_handle = OutboundNetworkHandle::new(to_outbound_tx);
    let (to_state_machine_tx, to_state_machine_rx) = unbounded_channel();
    let state_machine_handle = StateMachineHandle::new(to_state_machine_tx);
    run_state_machine_driver(state_machine, to_state_machine_rx);
    run_outbound_network(raft_handle.core_handle.clone(), to_outbound_rx);
    run_inbound_network(config.node_id.clone(), raft_handle.core_handle.clone());
    run_core(config, to_core_rx, outbound_network_handle, state_machine_handle);
    raft_handle
}