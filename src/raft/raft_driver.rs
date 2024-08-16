use std::time::Duration;

use crate::raft::events::Timer;
use crate::raft::messages::Message;
use crate::raft::types::NodeId;

pub trait RaftDriver {
    /// The nodes of this raft group.
    fn nodes() -> Vec<NodeId>;

    /// Send a message to the given node.
    fn send(&self, to: &NodeId, message: &Message);

    /// Emit a timer event after the given duration.
    fn set_timer(&self, timer: Timer, duration: Duration);

    /// Clear a timer event.
    fn clear_timer(&self, timer: Timer);
}
