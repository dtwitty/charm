use serde::{Deserialize, Serialize};

pub mod pb;
pub mod server;
pub mod client;
pub mod retry;
pub mod state_machine;
pub mod config;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CharmNodeInfo {
    pub host: String,
    pub port: u16,
}

impl CharmNodeInfo {
    pub fn to_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

