mod linearizability;
mod timing;

#[cfg(feature = "turmoil")]
pub mod tests {
    use crate::linearizability::{CharmHistory, CharmReq, CharmResp, ClientHistory};
    use crate::timing::{ClusterCrashSchedule, CrashEvent, RandomDuration};
    use charm::charm::client::EasyCharmClient;
    use charm::charm::retry::RetryStrategyBuilder;
    use charm::rng::CharmRng;
    use charm::server::{run_charm_server, CharmPeer, CharmServerConfigBuilder};
    use rand::RngCore;
    use rayon::prelude::*;
    use std::fs::{create_dir_all, remove_dir_all};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tracing::{error, info, warn};
    use turmoil::Sim;
    use wyrand::WyRand;

    #[test]
    fn test_charm() {
        let bad_seed = (0..10000)
            .into_par_iter()
            .find_any(|seed| {
                matches!(test_one(*seed), Err(_))
            });
        if let Some(b) = bad_seed {
            let msg = format!("seed {b} failed");
            panic!("{msg}");
        }
    }

    #[test]
    #[cfg(feature = "turmoil")]
    fn test_seed() -> turmoil::Result {
        let seed = 3694;
        configure_tracing();
        test_one(seed)
    }

    #[tracing::instrument()]
    fn test_one(seed: u64) -> turmoil::Result {
        // The simulation uses this seed.
        let mut sim = turmoil::Builder::new()
            .simulation_duration(Duration::from_secs(5 * 60))
            .min_message_latency(Duration::from_millis(1))
            .max_message_latency(Duration::from_millis(25))
            .enable_random_order()
            .tcp_capacity(1024)
            .build_with_rng(Box::new(WyRand::new(seed)));

        // The rest are seeded deterministically but differently for each node and client.
        let mut seed_gen = WyRand::new(seed);

        // Run a cluster of 3 nodes.
        let num_nodes = 3;
        run_cluster(seed, &mut sim, &mut seed_gen, num_nodes);

        let history = CharmHistory::new();

        // Run 3 clients...
        let num_clients = 3;
        for c in 0..num_clients {
            let client_name = format!("client{c}");
            let client_history = history.for_client(c);
            sim.client(client_name, run_client(seed_gen.next_u64(), client_history));
        }

        let nodes = (0..num_nodes).map(|i| format!("host{i}")).collect::<Vec<_>>();
        let crash_rng = WyRand::new(seed_gen.next_u64());
        let mut cluster_crash_schedule = ClusterCrashSchedule::new(
            crash_rng,
            nodes,
            // How often to crash
            Duration::from_secs(30),
            // How long to recover
            Duration::from_secs(5),
            // Recovery time stddev
            Duration::from_millis(250),
        );

        while !sim.step()? {
            let elapsed = sim.elapsed();
            let events = cluster_crash_schedule.advance(elapsed);
            for (host, event) in events {
                match event {
                    CrashEvent::Crash => {
                        warn!("crash {host}");
                        sim.crash(host);
                    }

                    CrashEvent::Recover => {
                        warn!("recover {host}");
                        sim.bounce(host);
                    }
                }
            }
        }

        // Check that the history is linearizable.
        history.linearize().ok_or_else(|| {
            error!("history is not linearizable! Printing approximate history.");
            history.history_by_raft_time().iter().for_each(|(c, s, req, resp)| {
                error!("{c:?}({s:?}): {req:?} -> {resp:?}");
            });
            "history is not linearizable".into()
        }).map(|_| ())
    }

    async fn run_client(client_seed: u64, history: Arc<Mutex<ClientHistory>>) -> turmoil::Result {
        let mut history = history.lock().unwrap();
        let mut client_rng = WyRand::new(client_seed);
        let retry_strategy = RetryStrategyBuilder::default()
            .rng(CharmRng::new(client_seed))
            .build().expect("valid");

        // Client connects to a random host.
        let host = format!("host{}", client_rng.next_u64() % 3);
        let client = EasyCharmClient::new(format!("http://{host}:12345"), retry_strategy)?;
        let mut sleep_dist = RandomDuration::new(client_rng.clone(), Duration::from_millis(250), Duration::from_millis(10));

        for k in 0..4 {
            let i = client_rng.next_u64() % 3;
            let key = format!("key{}", client_rng.next_u64() % 1);
            match i {
                0 => {
                    history.on_invoke(CharmReq::Get(key.clone()));
                    info!("Starting request {k}: get {key}");
                    let resp = client.get(key.clone()).await?;
                    info!("Finished request {k}: get {key} -> {resp:?}");
                    history.on_return(CharmResp::Get(resp));
                }

                1 => {
                    let value = format!("value{}", client_rng.next_u64() % 3);
                    history.on_invoke(CharmReq::Put(key.clone(), value.clone()));
                    info!("Starting request {k}: put {key} -> {value}");
                    let resp = client.put(key.clone(), value.clone()).await?;
                    info!("Finished request {k}: put {key} -> {value} -> {resp:?}");
                    history.on_return(CharmResp::Put(resp));
                }

                2 => {
                    history.on_invoke(CharmReq::Delete(key.clone()));
                    info!("Starting request {k}: delete {key}");
                    let resp = client.delete(key.clone()).await?;
                    info!("Finished request {k}: delete {key} -> {resp:?}");
                    history.on_return(CharmResp::Delete(resp));
                }

                _ => panic!("unexpected value"),
            };

            let wait = sleep_dist.sample();
            tokio::time::sleep(wait).await;
        }

        Ok(())
    }

    fn run_cluster(test_seed: u64, sim: &mut Sim, seed_gen: &mut impl RngCore, num_nodes: usize) {
        // Set up the environment for the test.
        remove_dir_all(format!("test_data/{}", test_seed)).ok();
        create_dir_all(format!("test_data/{}", test_seed)).unwrap();

        const RAFT_PORT: u16 = 54321;
        const CHARM_PORT: u16 = 12345;
        let host_names = (0..num_nodes).map(|i| format!("host{i}")).collect::<Vec<_>>();

        for host_name in host_names.clone().iter().cloned() {
            let seed = seed_gen.next_u64();
            let host_names = host_names.clone();
            let raft_storage_filename = format!("test_data/{}/raft_storage_{}.sqlite", test_seed, host_name);
            let raft_log_storage_filename = format!("test_data/{}/raft_log_storage_{}.sqlite", test_seed, host_name);
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
                        .raft_log_storage_filename(raft_log_storage_filename.clone())
                        .raft_storage_filename(raft_storage_filename.clone())
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
                .with_env_filter("info,charm::charm=debug,charm::raft::core::node=debug,charm::raft::state_machine=debug")
                .with_timer(SimElapsedTime)
                .finish(),
        )
            .expect("Configure tracing");
    }

}