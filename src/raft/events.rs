use crate::raft::messages::Message;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Timer {
    Election,
    Heartbeat,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    MessageReceived(Message),
    TimerTimedOut(Timer),
}