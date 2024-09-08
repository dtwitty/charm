use charm::charm::client::EasyCharmClient;
use charm::charm::server::run_server;
use charm::charm::state_machine::CharmStateMachine;
use charm::raft::core::config::RaftConfigBuilder;
use charm::raft::run_raft;
use charm::raft::types::NodeId;
use std::future::pending;
use std::time::Duration;

#[test]
#[cfg(feature = "turmoil")]
fn test_charm() -> turmoil::Result {
    configure_tracing();

    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(60))
        .build();

    let host_names = vec![
        "hostA",
        "hostB",
        "hostC",
    ];

    let other_nodes = host_names.clone().iter().map(|x| NodeId(format!("http://{}:54321", x))).collect::<Vec<_>>();
    for host_name in host_names.iter().cloned() {
        let node_id = NodeId(format!("http://{}:54321", host_name));
        let other_nodes = other_nodes.clone().into_iter().filter(|x| x != &node_id).collect::<Vec<_>>();
        let config = RaftConfigBuilder::default()
            .node_id(node_id)
            .other_nodes(other_nodes)
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
        let client = EasyCharmClient::new("http://hostA:12345".to_string())?;
        client.put("hello".to_string(), "world".to_string()).await?;
        let value = client.get("hello".to_string()).await?;
        assert_eq!(value, Some("world".to_string()));
        client.delete("hello".to_string()).await?;
        return Err("Not found".into());
        let value = client.get("hello".to_string()).await?;
        assert_eq!(value, None);
        Ok(())
    });

    sim.run()
}

#[derive(Clone)]
struct SimElapsedTime;
impl tracing_subscriber::fmt::time::FormatTime for SimElapsedTime {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        // Prints real time and sim elapsed time. Example: 2024-01-10T17:06:57.020452Z [76ms]
        tracing_subscriber::fmt::time()
            .format_time(w)
            .and_then(|()| write!(w, " [{:?}]", turmoil::sim_elapsed().unwrap_or_default()))
    }
}

fn configure_tracing() {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_timer(SimElapsedTime)
            .finish(),
    )
        .expect("Configure tracing");
}