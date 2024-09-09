use derive_builder::Builder;

#[derive(Debug, Clone, Builder)]
pub struct CharmConfig {
    pub listen_addr: String,
    pub peer_addrs: Vec<String>,
}