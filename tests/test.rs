use charm::charm::client::EasyCharmClient;
use charm::charm::retry::RetryStrategyBuilder;
use charm::charm::server::run_server;
use charm::charm::state_machine::CharmStateMachine;
use charm::raft::core::config::RaftConfigBuilder;
use charm::raft::run_raft;
use charm::raft::types::NodeId;
use charm::rng::CharmRng;
use rand::RngCore;
use std::future::pending;
use std::time::Duration;
use tracing::error;
use wyrand::WyRand;

#[test]
#[cfg(feature = "turmoil")]
fn test_charm() -> turmoil::Result {
    // Run a bunch of tests with different seeds to try to find a seed that causes a failure.
    let mut bad_seed = None;
    for seed in 1..100 {
        let result = test_one(seed);
        if result.is_err() {
            bad_seed = Some(seed);
            break;
        }
    }

    if let Some(seed) = bad_seed {
        // Rerun the test with the bad seed to get a backtrace.
        // Add logging to the test to help debug the issue.
        configure_tracing();
        error!("Test failed with seed {}", seed);
        return test_one(seed);
    }

    Ok(())
}


fn test_one(seed: u64) -> turmoil::Result {
    // The simulation uses this seed.
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(60))
        .build_with_rng(Box::new(WyRand::new(seed)));

    // The rest are seeded deterministically but differently for each node and client.
    let mut seed_gen = WyRand::new(seed);

    let host_names = vec![
        "hostA",
        "hostB",
        "hostC",
    ];

    let other_nodes = host_names.clone().iter().map(|x| NodeId(format!("http://{}:54321", x))).collect::<Vec<_>>();
    for host_name in host_names.iter().cloned() {
        let seed = seed_gen.next_u64();
        let node_id = NodeId(format!("http://{}:54321", host_name));
        let other_nodes = other_nodes.clone().into_iter().filter(|x| x != &node_id).collect::<Vec<_>>();
        let config = RaftConfigBuilder::default()
            .node_id(node_id)
            .other_nodes(other_nodes)
            .build().unwrap();
        sim.host(host_name.to_string(), move ||
            {
                let config = config.clone();
                let rng = CharmRng::new(seed);
                async move {
                    let sm = CharmStateMachine::new();
                    let raft_handle = run_raft(config, sm, rng.clone());
                    run_server(12345, raft_handle, rng);
                    pending::<()>().await;
                    Ok(())
                }
        });
    }

    let client_seed = seed_gen.next_u64();
    sim.client("client", async move {
        let retry_strategy = RetryStrategyBuilder::default().rng(CharmRng::new(client_seed)).build().expect("valid");
        let client = EasyCharmClient::new("http://hostA:12345".to_string(), retry_strategy)?;
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