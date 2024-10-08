#[cfg(feature = "turmoil")]
pub mod tests {
    use charm::charm::client::EasyCharmClient;
    use charm::charm::pb::{DeleteResponse, GetResponse, PutResponse, ResponseHeader};
    use charm::charm::retry::RetryStrategyBuilder;
    use charm::rng::CharmRng;
    use charm::server::{run_charm_server, CharmPeer, CharmServerConfigBuilder};
    use rand::{Rng, RngCore};
    use stateright::semantics::{ConsistencyTester, LinearizabilityTester, SequentialSpec};
    use std::collections::HashMap;
    use std::fs::{create_dir_all, remove_dir_all};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tracing::{info, warn};
    use turmoil::Sim;
    use wyrand::WyRand;

    #[test]
    fn test_charm() -> turmoil::Result {
        // Run a bunch of tests with different seeds to try to find a seed that causes a failure.
        for seed in 0..1000 {
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
        let seed = 53;
        configure_tracing();
        test_one(seed)
    }

    #[tracing::instrument()]
    fn test_one(seed: u64) -> turmoil::Result {
        // The simulation uses this seed.
        let mut sim = turmoil::Builder::new()
            .simulation_duration(Duration::from_secs(120))
            .min_message_latency(Duration::from_millis(1))
            .max_message_latency(Duration::from_millis(50))
            .enable_random_order()
            .tcp_capacity(512)
            .build_with_rng(Box::new(WyRand::new(seed)));

        // The rest are seeded deterministically but differently for each node and client.
        let mut seed_gen = WyRand::new(seed);

        // Run a cluster of 3 nodes.
        run_cluster(seed, &mut sim, &mut seed_gen, 3);

        let history = Arc::new(Mutex::new(LinearizabilityTester::new(CharmSpec::new())));

        // Run 3 clients...
        for c in 0..3 {
            let client_name = format!("client{c}");
            sim.client(client_name.clone(), run_client(c, seed_gen.next_u64(), history.clone()));
        }

        let mut crash_rng = WyRand::new(seed_gen.next_u64());
        let mut next_crash = random_duration(&mut crash_rng, Duration::from_secs(20), Duration::from_secs(1));
        let mut restarts = HashMap::new();
        while !sim.step()? {
            let elapsed = sim.elapsed();
            if elapsed >= next_crash {
                next_crash += random_duration(&mut crash_rng, Duration::from_secs(20), Duration::from_secs(1));
                let host = format!("host{}", crash_rng.next_u64() % 3);
                warn!("crashing {host}");
                sim.crash(host.clone());
                let restart_in = random_duration(&mut crash_rng, Duration::from_secs(3), Duration::from_secs(1));
                let restart_time = elapsed + restart_in;
                restarts.insert(host, restart_time);
            }
            for (host, restart_time) in restarts.clone() {
                if elapsed >= restart_time {
                    warn!("restarting {host}");
                    sim.bounce(host.clone());
                    restarts.remove(&host);
                }
            }
        }

        // Check that the history is linearizable.
        let history = history.lock().unwrap();
        let serialized = history.serialized_history().ok_or(
            anyhow::Error::msg("history is not linearizable".to_string())
        )?;

        // Print the history:
        serialized.iter().for_each(|(req, resp)| {
            info!("{req:?} -> {resp:?}");
        });


        Ok(())
    }

    // Returns a normally-distributed random duration.
    fn random_duration<R: RngCore>(rng: &mut R, mean: Duration, std_dev: Duration) -> Duration {
        let mean_millis = mean.as_millis() as f64;
        let std_dev_millis = std_dev.as_millis() as f64;
        let normal = rand_distr::Normal::new(mean_millis, std_dev_millis).unwrap();
        let millis = rng.sample(normal);
        Duration::from_millis(millis as u64).max(Duration::ZERO)
    }

    async fn run_client(client_num: u64, client_seed: u64, history: Arc<Mutex<LinearizabilityTester<u64, CharmSpec>>>) -> turmoil::Result {
        let mut client_rng = WyRand::new(client_seed);
        let retry_strategy = RetryStrategyBuilder::default()
            .rng(CharmRng::new(client_seed))
            .build().expect("valid");

        // Client connects to a random host.
        let host = format!("host{}", client_rng.next_u64() % 3);
        let client = EasyCharmClient::new(format!("http://{host}:12345"), retry_strategy)?;

        for _ in 0..10 {
            let i = client_rng.next_u64() % 3;
            let key = format!("key{}", client_rng.next_u64() % 1);
            match i {
                0 => {
                    {
                        history.lock().unwrap().on_invoke(client_num, CharmReq::Get(key.clone())).expect("valid");
                    }
                    info!("get {key}");
                    let resp = client.get(key.clone()).await?;
                    info!("get {key} -> {resp:?}");
                    {
                        history.lock().unwrap().on_return(client_num, CharmResp::Get(resp)).expect("valid");
                    }
                }

                1 => {
                    let value = format!("value{}", client_rng.next_u64() % 3);
                    {
                        history.lock().unwrap().on_invoke(client_num, CharmReq::Put(key.clone(), value.clone())).expect("valid");
                    }
                    info!("put {key} -> {value}");
                    let resp = client.put(key.clone(), value.clone()).await?;
                    info!("put {key} -> {value} -> {resp:?}");
                    {
                        history.lock().unwrap().on_return(client_num, CharmResp::Put(resp)).expect("valid");
                    }
                }

                2 => {
                    {
                        history.lock().unwrap().on_invoke(client_num, CharmReq::Delete(key.clone())).expect("valid");
                    }
                    info!("delete {key}");
                    let resp = client.delete(key.clone()).await?;
                    info!("delete {key} -> {resp:?}");
                    {
                        history.lock().unwrap().on_return(client_num, CharmResp::Delete(resp)).expect("valid");
                    }
                }

                _ => panic!("unexpected value"),
            };
            
            let wait = random_duration(&mut client_rng, Duration::from_secs(1), Duration::from_millis(100));
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
                .with_env_filter("info,charm::client=debug")
                .with_timer(SimElapsedTime)
                .finish(),
        )
            .expect("Configure tracing");
    }

    #[derive(Debug, Clone, PartialEq)]
    enum CharmReq {
        Get(String),
        Put(String, String),
        Delete(String),
    }

    #[derive(Debug, Clone, PartialEq)]
    enum CharmResp {
        Get(GetResponse),
        Put(PutResponse),
        Delete(DeleteResponse),
    }

    #[derive(Debug, Clone)]
    struct CharmSpec {
        data: HashMap<String, String>,
        leader_addr: String,
        term: u64,
        index: u64,
    }

    impl CharmSpec {
        fn new() -> Self {
            Self {
                data: HashMap::new(),
                leader_addr: "".to_string(),
                term: 0,
                index: 0,
            }
        }

        fn update_term_and_index(&mut self, response_header: &Option<ResponseHeader>) -> bool {
            if let Some(response_header) = response_header {
                if self.term > response_header.raft_term {
                    return false;
                }
                if self.term == response_header.raft_term && self.leader_addr != response_header.leader_addr {
                    return false;
                }
                self.term = response_header.raft_term;
                self.leader_addr = response_header.leader_addr.clone();

                if self.index >= response_header.raft_index {
                    return false;
                }
                self.index = response_header.raft_index;

                true
            } else {
                false
            }
        }
    }

    impl SequentialSpec for CharmSpec {
        type Op = CharmReq;
        type Ret = CharmResp;

        fn invoke(&mut self, op: &Self::Op) -> Self::Ret {
            match op {
                CharmReq::Get(key) => {
                    let value = self.data.get(key).cloned();
                    let response_header = Some(ResponseHeader {
                        leader_addr: "".to_string(),
                        raft_term: self.term,
                        raft_index: self.index,
                    });
                    let response = GetResponse { value, response_header };
                    CharmResp::Get(response)
                }
                CharmReq::Put(key, value) => {
                    self.index += 1;
                    self.data.insert(key.clone(), value.clone());
                    let response_header = Some(ResponseHeader {
                        leader_addr: "".to_string(),
                        raft_term: self.term,
                        raft_index: self.index,
                    });
                    let response = PutResponse { response_header };
                    CharmResp::Put(response)
                }
                CharmReq::Delete(key) => {
                    self.index += 1;
                    self.data.remove(key);
                    let response_header = Some(ResponseHeader {
                        leader_addr: "".to_string(),
                        raft_term: self.term,
                        raft_index: self.index,
                    });
                    let response = DeleteResponse { response_header };
                    CharmResp::Delete(response)
                }
            }
        }

        fn is_valid_step(&mut self, op: &Self::Op, ret: &Self::Ret) -> bool {
            match (op, ret) {
                (CharmReq::Get(key), CharmResp::Get(resp)) => {
                    if !self.update_term_and_index(&resp.response_header) {
                        return false;
                    }

                    let value = self.data.get(key).cloned();
                    value == resp.value
                }
                (CharmReq::Put(key, value), CharmResp::Put(resp)) => {
                    if !self.update_term_and_index(&resp.response_header) {
                        return false;
                    }

                    self.data.insert(key.clone(), value.clone());
                    true
                }
                (CharmReq::Delete(key), CharmResp::Delete(resp)) => {
                    if !self.update_term_and_index(&resp.response_header) {
                        return false;
                    }

                    self.data.remove(key);
                    true
                }
                _ => false,
            }
        }
    }
}