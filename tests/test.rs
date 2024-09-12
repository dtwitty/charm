#[cfg(feature = "turmoil")]

use charm::charm::client::EasyCharmClient;
use charm::charm::retry::RetryStrategyBuilder;
use charm::rng::CharmRng;
use charm::server::{run_charm_server, CharmPeer, CharmServerConfigBuilder};
use rand::RngCore;
use std::time::Duration;
use turmoil::Sim;
use wyrand::WyRand;

#[test]
fn test_charm() -> turmoil::Result {
    // Run a bunch of tests with different seeds to try to find a seed that causes a failure.
    for seed in 1..1000 {
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
    let seed = 10;
    configure_tracing();
    test_one(seed)
}


#[tracing::instrument()]
fn test_one(seed: u64) -> turmoil::Result {
    // The simulation uses this seed.
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(5))
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(50))
        .enable_random_order()
        .fail_rate(0.01)
        .repair_rate(0.9)
        .build_with_rng(Box::new(WyRand::new(seed)));

    // The rest are seeded deterministically but differently for each node and client.
    let mut seed_gen = WyRand::new(seed);

    // Run a cluster of 3 nodes.
    run_cluster(&mut sim, &mut seed_gen, 3);


    let client_seed = seed_gen.next_u64();
    sim.client("client", async move {
        let retry_strategy = RetryStrategyBuilder::default().rng(CharmRng::new(client_seed)).build().expect("valid");
        let client = EasyCharmClient::new("http://host0:12345".to_string(), retry_strategy)?;
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

fn run_cluster(sim: &mut Sim, seed_gen: &mut impl RngCore, num_nodes: usize) {
    const RAFT_PORT: u16 = 54321;
    const CHARM_PORT: u16 = 12345;
    let host_names = (0..num_nodes).map(|i| format!("host{}", i)).collect::<Vec<_>>();

    for host_name in host_names.clone().iter().cloned() {
        let seed = seed_gen.next_u64();
        let host_names = host_names.clone();
        sim.host(host_name.to_string(), move ||
            {
                let host_names = host_names.clone();
                let charm_server_config = CharmServerConfigBuilder::default()
                    .rng_seed(seed)
                    .listen(
                        CharmPeer {
                            host: host_name.to_string(),
                            charm_port: CHARM_PORT,
                            raft_port: RAFT_PORT,
                        })
                    .peers(
                        host_names.iter().filter(|&h| h != &host_name)
                            .map(|h| CharmPeer {
                                host: h.to_string(),
                                charm_port: CHARM_PORT,
                                raft_port: RAFT_PORT,
                            })
                            .collect())
                    .build().unwrap();
                async move {
                    run_charm_server(charm_server_config).await;
                    Ok(())
                }
            });
    }
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
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_env_filter("info,charm::charm=debug")
            .with_timer(SimElapsedTime)
            .finish(),
    )
        .expect("Configure tracing");
}
