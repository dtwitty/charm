use std::ops::Add;

/// A term is a monotonically increasing number representing the number of elections that have
/// occurred in the system. Used as a logical clock to order events in the system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash, Ord)]
pub struct Term(pub u64);

impl Term {
    pub fn next(self) -> Term {
        Term(self.0 + 1)
    }
}

impl Add for Term {
    type Output = Term;

    fn add(self, other: Term) -> Term {
        Term(self.0 + other.0)
    }
}

/// A position in the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash, Ord)]
pub struct Index(pub u64);

impl Index {
    pub fn next(self) -> Index {
        Index(self.0 + 1)
    }

    pub fn prev(self) -> Index {
        Index(self.0 - 1)
    }
}

impl Add for Index {
    type Output = Index;

    fn add(self, other: Index) -> Index {
        Index(self.0 + other.0)
    }
}

/// A unique identifier for a node in the cluster, typically an address like "host:port".
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash, Ord)]
pub struct NodeId(pub String);

/// A command to be executed by the state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command(pub Vec<u8>);

/// A log entry, containing a command to be executed and the term in which it was received.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub term: Term,
    pub command: Command,
}
