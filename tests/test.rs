use charm::charm::client::EasyCharmClient;
use charm::charm::server::run_server;
use charm::charm::state_machine::CharmStateMachine;
use charm::raft::core::config::RaftConfigBuilder;
use charm::raft::run_raft;
use charm::raft::types::NodeId;
use std::future::pending;
use std::time::Duration;
use tokio::time::sleep;

#[test]
#[cfg(feature = "turmoil")]
fn test_charm() -> turmoil::Result {
    let mut sim = turmoil::Builder::new().build();

    let host_names = vec![
        "hostA",
        "hostB",
        "hostC",
    ];

    let other_nodes = host_names.clone().iter().map(|x| NodeId(format!("http://{}:54321", x))).collect::<Vec<_>>();
    for host_name in host_names.iter().cloned() {
        let config = RaftConfigBuilder::default()
            .node_id(NodeId(format!("http://{}:54321", host_name)))
            .other_nodes(other_nodes.clone())
            .build().unwrap();
        sim.host(host_name.to_string(), move ||
            {
                let config = config.clone();
                async move {
                    let sm = CharmStateMachine::new();
                    let raft_handle = run_raft(config, sm);
                    run_server(12345, raft_handle);
                    pending::<()>().await;
                    Ok(())
                }
        });
    }

    sim.client("client", async move {
        sleep(Duration::from_secs(3)).await;
        let client = EasyCharmClient::new("http://hostA:12345".to_string())?;
        client.put("hello".to_string(), "world".to_string()).await?;
        let value = client.get("hello".to_string()).await?;
        assert_eq!(value, Some("world".to_string()));
        client.delete("hello".to_string()).await?;
        let value = client.get("hello".to_string()).await?;
        assert_eq!(value, None);
        Ok(())
    });

    sim.run()
}