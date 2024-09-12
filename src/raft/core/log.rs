use crate::raft::types::{Index, LogEntry, Term};

pub struct Log {
    entries: Vec<LogEntry>,
}

impl Log {
    pub fn new() -> Log {
        Log {
            entries: vec![],
        }
    }

    pub fn append(&mut self, entry: LogEntry) -> Index {
        self.entries.push(entry);
        Index(self.entries.len() as u64)
    }

    pub fn get(&self, index: Index) -> Option<&LogEntry> {
        if index.0 == 0 {
            return None;
        }

        let idx = index.0 as usize - 1;
        self.entries.get(idx)
    }

    pub fn last_index(&self) -> Index {
        Index(self.entries.len() as u64)
    }

    pub fn last_log_term(&self) -> Term {
        self.entries.last().map_or(Term(0), |entry| entry.term)
    }

    pub fn entries_from(&self, index: Index) -> Vec<LogEntry> {
        let idx = index.prev().0 as usize;
        self.entries[idx..].to_vec()
    }

    /// Truncate the log so that it no longer includes any entries with an index greater than or equal to `index`.
    pub fn truncate(&mut self, index: Index) {
        if index.0 == 0 {
            self.entries.clear();
            return;
        }

        let idx = index.0 as usize - 1;
        self.entries.truncate(idx);
    }
}
