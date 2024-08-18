use tracing::warn;

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

    pub fn append<I: IntoIterator<Item=LogEntry>>(&mut self, entries: I, mut index: Index) {
        for entry in entries {
            let existing_entry = self.get(index);
            match existing_entry {
                Some(existing_entry) => {
                    if existing_entry.term != entry.term {
                        // Remove all entries from here on.
                        warn!("Removing conflicting entries from index {:?}.", index);
                        self.entries.truncate(index.0 as usize - 1);

                        self.entries.push(entry);
                    }

                    // The entry already exists, so we don't need to do anything.
                }

                _ => {
                    // We are at the end of the log. Go ahead and append the entry.
                    self.entries.push(entry);
                }
            }
            index = index.next();
        }
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
        self.entries.last().map(|entry| entry.term).unwrap_or(Term(0))
    }

    pub fn entries_from(&self, index: Index) -> Vec<LogEntry> {
        let idx = index.0 as usize - 1;
        self.entries[idx..].to_vec()
    }
}
