#[cfg(feature = "turmoil")]
pub mod tests {
    use charm::charm::client::EasyCharmClient;
    use charm::charm::pb::{DeleteRequest, DeleteResponse, GetRequest, GetResponse, PutRequest, PutResponse};
    use charm::charm::retry::RetryStrategyBuilder;
    use charm::rng::CharmRng;
    use charm::server::{run_charm_server, CharmPeer, CharmServerConfigBuilder};
    use rand::RngCore;
    use std::collections::{HashMap, HashSet};
    use std::fs::{create_dir_all, remove_dir_all};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use turmoil::{elapsed, Sim};
    use wyrand::WyRand;

    #[test]
    fn test_charm() -> turmoil::Result {
        // Run a bunch of tests with different seeds to try to find a seed that causes a failure.
        for seed in 1..1000 {
            let res = test_one(seed);
            if let Err(e) = res {
                eprintln!("seed {seed} failed: {e:?}");
                return Err(e);
            }
        }
        Ok(())
    }

    #[test]
    #[cfg(feature = "turmoil")]
    fn test_seed() -> turmoil::Result {
        let seed = 1;
        configure_tracing();
        test_one(seed)
    }

    enum RequestResponse {
        Get(GetRequest, anyhow::Result<GetResponse>),
        Put(PutRequest, anyhow::Result<PutResponse>),
        Delete(DeleteRequest, anyhow::Result<DeleteResponse>),
    }

    struct Event {
        request_response: RequestResponse,
        host: String,
        start: Duration,
        end: Duration,
    }


    #[tracing::instrument()]
    fn test_one(seed: u64) -> turmoil::Result {
        // The simulation uses this seed.
        let mut sim = turmoil::Builder::new()
            .simulation_duration(Duration::from_secs(10))
            .min_message_latency(Duration::from_millis(1))
            .max_message_latency(Duration::from_millis(50))
            .enable_random_order()
            .build_with_rng(Box::new(WyRand::new(seed)));

        // The rest are seeded deterministically but differently for each node and client.
        let mut seed_gen = WyRand::new(seed);

        // Run a cluster of 3 nodes.
        run_cluster(seed, &mut sim, &mut seed_gen, 3);

        let history = Arc::new(Mutex::new(Vec::new()));
        // Run 3 clients...
        for c in 0..3 {
            sim.client(format!("client{c}"), run_client(seed_gen.next_u64(), history.clone()));
        }

        let r = sim.run();
        let h = history.lock().unwrap();
        assert!(h.len() > 0);
        r
    }

    async fn run_client(client_seed: u64, history: Arc<Mutex<Vec<Event>>>) -> turmoil::Result {
        let mut client_rng = WyRand::new(client_seed);
        let retry_strategy = RetryStrategyBuilder::default()
            .total_retry_time(Duration::from_secs(5 * 60))
            .rng(CharmRng::new(client_seed))
            .build().expect("valid");

        // Client connects to a random host.
        let host = format!("host{}", client_rng.next_u64() % 3);
        let client = EasyCharmClient::new(format!("http://{host}:12345"), retry_strategy)?;

        for _ in 0..10 {
            let start = elapsed();

            let i = client_rng.next_u64() % 3;
            let event = match i {
                _ => todo!(),
            };

            history.lock().unwrap().push(event);
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

    // Checks the following:
    // - The leader is the same in each term.
    // - Index is monotonic with respect to term.
    // - Indexes are unique among PUT and DELETE requests.
    // - GET that share an index all return the same value.
    // - The linearization formed by the index is consistent with timing of requests.
    // - The linearization formed by the index is consistent with a hashmap.

    fn check_history(history: &Vec<Event>) {
        check_log_constraints(history);
        check_index_uniqueness(history);
    }

    // Checks the following:
    // - The leader is the same in each term.
    // - Index is monotonic with respect to term.
    fn check_log_constraints(history: &Vec<Event>) {
        let mut response_headers = history.iter().filter_map(|event| {
            match &event.request_response {
                RequestResponse::Get(_, Ok(resp)) => Some(resp.response_header.clone().unwrap()),
                RequestResponse::Put(_, Ok(resp)) => Some(resp.response_header.clone().unwrap()),
                RequestResponse::Delete(_, Ok(resp)) => Some(resp.response_header.clone().unwrap()),
                _ => None,
            }
        }).collect::<Vec<_>>();

        // Assert that index and term are monotonic with each other.
        // Also assert that the leader is the same in each term.
        let mut leader_for_term = HashMap::new();
        response_headers.sort_by_key(|header| header.raft_index);
        let mut prev_index = 0;
        let mut prev_term = 0;
        for header in &response_headers {
            assert!(header.raft_index > prev_index);
            assert!(header.raft_term >= prev_term);
            assert_eq!(leader_for_term.entry(header.raft_term).or_insert(header.leader_id.clone()), &header.leader_id);
            prev_index = header.raft_index;
            prev_term = header.raft_term;
        }
    }

    // Checks the following:
    // - Indexes are unique among PUT and DELETE requests.
    // - GET that share an index all return the same value.
    fn check_index_uniqueness(history: &Vec<Event>) {
        let mut reads = HashMap::new();
        let mut writes = HashSet::new();

        for event in history {
            match &event.request_response {
                RequestResponse::Get(_, Ok(resp)) => {
                    let index = resp.response_header.clone().unwrap().raft_index;
                    let term = resp.response_header.clone().unwrap().raft_term;
                    let value = resp.value.clone();
                    let entry = reads.entry(index).or_insert_with(|| (term, value.clone()));
                    assert_eq!(*entry, (term, value));
                }

                RequestResponse::Put(req, Ok(resp)) => {
                    let index = resp.response_header.clone().unwrap().raft_index;
                    let term = resp.response_header.clone().unwrap().raft_term;
                    let entry = writes.insert(index);
                    assert!(entry);
                }

                RequestResponse::Delete(req, Ok(resp)) => {
                    let index = resp.response_header.clone().unwrap().raft_index;
                    let term = resp.response_header.clone().unwrap().raft_term;
                    let entry = writes.insert(index);
                    assert!(entry);
                }

                _ => {}
            }
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
}