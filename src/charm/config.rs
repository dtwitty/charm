use crate::server::CharmPeer;
use derive_builder::Builder;

#[derive(Debug, Clone, Builder)]
pub struct CharmConfig {
    pub listen: CharmPeer,
    pub peers: Vec<CharmPeer>,
}