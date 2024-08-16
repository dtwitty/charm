use crate::raft::messages::Message;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Timer {
    ElectionTimeout,
    HeartbeatTimeout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Message(Message),
    Timeout(Timer),
}