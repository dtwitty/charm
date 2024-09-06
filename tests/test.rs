#![cfg(madsim)]

use charm::charm::client::EasyCharmClient;
use charm::charm::server::run_server;
use charm::charm::state_machine::CharmStateMachine;
use charm::raft;
use charm::raft::core::config::RaftConfig;
use charm::raft::run_raft;
use charm::raft::types::NodeId;
use madsim::runtime::Handle;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::pending;
use std::net::IpAddr::V4;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::net::unix::SocketAddr;
use tokio::sync::oneshot;
use tonic::async_trait;

#[madsim::test]
async fn test_charm() {
    let handle = Handle::current();

    let ips = vec![
        V4(Ipv4Addr::new(10, 0, 0, 1)),
        V4(Ipv4Addr::new(10, 0, 0, 2)),
        V4(Ipv4Addr::new(10, 0, 0, 3)),
    ];
    let nodes = ips.iter().map(|ip| {
        handle.create_node().name(format!("node-{}", ip)).ip(ip.clone()).build()
    }).collect::<Vec<_>>();

    let charm_addrs = ips.iter().map(|ip| SocketAddr::V4(SocketAddrV4::new(ip.clone(), 54321))).collect::<Vec<_>>();
    let raft_node_ids = nodes.iter().zip(ips.iter()).map(|(node, ip)| NodeId(format!("{}:54321", ip))).collect::<Vec<_>>();

    // Run the servers.
    for ((raft_node_id, socket_addr), node) in raft_node_ids.iter().zip(charm_addrs.iter()).zip(nodes.iter()) {
        let node_id_clones = raft_node_ids.clone();
        let node_id_clone = raft_node_id.clone();
        node.spawn(async move {
            let config = RaftConfig {
                node_id: node_id_clone,
                other_nodes: node_id_clones,
                election_timeout_min: Duration::from_millis(150),
                election_timeout_max: Duration::from_millis(300),
                heartbeat_interval: Duration::from_millis(50),
            };

            let sm = CharmStateMachine {
                map: HashMap::new(),
            };
            let raft_handle = run_raft(config, sm);
            let server = run_server(socket_addr.clone(), raft_handle);
            pending::<()>().await
        });
    }

    let client = handle.create_node().name("client").build();
    client.spawn(async move {
        sleep(Duration::from_secs(3)).await;
        let client = EasyCharmClient::new(raft_node_ids[0].clone()).await.unwrap();
        client.put("hello".to_string(), "world".to_string()).await.unwrap();
        let value = client.get("hello".to_string()).await.unwrap();
        assert_eq!(value, Some("world".to_string()));
        client.delete("hello".to_string()).await.unwrap();
        let value = client.get("hello".to_string()).await.unwrap();
        assert_eq!(value, None);
    });
}