/// A term is a monotonically increasing number representing the number of elections that have
/// occurred in the system. Used as a logical clock to order events in the system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash, Ord)]
pub struct Term(pub u64);

impl Term {
    pub fn next(self) -> Term {
        Term(self.0 + 1)
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
        if self.0 == 0 {
            Index(0)
        } else {
            Index(self.0 - 1)
        }
    }
}

/// A unique identifier for a node in the cluster, typically an address like "host:port".
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash, Ord)]
pub struct NodeId {
    pub host: String,
    pub port: u16,
}

impl NodeId {
    pub fn to_string(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn to_addr(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }
}

/// A command to be executed by the state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Data(pub Vec<u8>);

/// A log entry, containing a command to be executed and the term in which it was received.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub term: Term,
    pub data: Data,
}
