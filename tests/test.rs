use charm::charm::client::EasyCharmClient;
use charm::charm::config::CharmConfig;
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
use wyrand::WyRand;

#[test]
#[cfg(feature = "turmoil")]
fn test_charm() -> turmoil::Result {
    // Run a bunch of tests with different seeds to try to find a seed that causes a failure.
    for seed in 1..100 {
        let res = test_one(seed);
        if let Err(e) = res {
            eprintln!("seed {} failed: {:?}", seed, e);
            return Err(e);
        }
    }
    Ok(())
}

#[test]
#[cfg(feature = "turmoil")]
fn test_seed() -> turmoil::Result {
    let seed = 2;
    configure_tracing();
    test_one(seed)
}


#[tracing::instrument()]
fn test_one(seed: u64) -> turmoil::Result {
    // The simulation uses this seed.
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(3))
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
        let raft_config = RaftConfigBuilder::default()
            .node_id(node_id)
            .other_nodes(other_nodes)
            .build().unwrap();
        let charm_config = CharmConfig {
            listen_addr: format!("http://{}:12345", host_name),
            peer_addrs: host_names
                .clone()
                .into_iter()
                .filter(|h| h != &host_name)
                .map(|x| format!("http://{}:12345", x))
                .collect(),

        };
        sim.host(host_name.to_string(), move ||
            {
                let config = raft_config.clone();
                let rng = CharmRng::new(seed);
                let charm_config = charm_config.clone();
                async move {
                    let sm = CharmStateMachine::new();
                    let raft_handle = run_raft(config, sm, rng.clone());
                    run_server(charm_config, raft_handle, rng);
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
            .with_env_filter("info,charm::charm=debug")
            .with_timer(SimElapsedTime)
            .finish(),
    )
        .expect("Configure tracing");
}