use std::path::PathBuf;

use charm::server::{run_charm_server, CharmPeer, CharmServerConfigBuilder};
use clap::Parser;
use rand::RngCore;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The address to bind to, of the form "host:port".
    #[arg(long, default_value = "localhost")]
    bind_host: String,

    /// The port to bind to for the charm service.
    #[arg(long)]
    charm_port: u16,

    /// The port to bind to for the raft service.
    #[arg(long)]
    raft_port: u16,

    /// The addresses of the other nodes in the cluster (can include this one for convenience).
    /// Of the form: "host,charm_port,raft_port)".
    #[arg(long)]
    peer_addresses: Vec<String>,

    /// The path to the data directory where raft logs and state machine data will be stored.
    /// If the directory does not exist, it will be created.
    #[arg(long)]
    data_dir: PathBuf,
}

#[tokio::main]
async fn main() {
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .finish(),
    )
        .expect("Configure tracing");

    let args = Args::parse();

    // Create the data directory if it does not exist.
    if !args.data_dir.exists() {
        std::fs::create_dir_all(&args.data_dir).expect("failed to create data directory");
    }

    let rng_seed = rand::thread_rng().next_u64();

    let raft_log_storage_filename = args.data_dir.join("raft_log.db").to_str().unwrap().to_string();
    let raft_storage_filename = args.data_dir.join("raft.db").to_str().unwrap().to_string();


    let listen_peer = CharmPeer {
        host: args.bind_host.clone(),
        charm_port: args.charm_port,
        raft_port: args.raft_port,
    };

    let peers = args.peer_addresses.iter().map(|addr| {
        let parts: Vec<&str> = addr.split(',').collect();
        let host = parts[0];
        let charm_port = parts[1].parse().expect("failed to parse charm port");
        let raft_port = parts[2].parse().expect("failed to parse raft port");
        CharmPeer {
            host: host.to_string(),
            charm_port,
            raft_port,
        }
    }).filter(|p| p != &listen_peer).collect();

    let charm_server_config = CharmServerConfigBuilder::default()
        .rng_seed(rng_seed)
        .listen(listen_peer)
        .peers(peers)
        .raft_log_storage_filename(raft_log_storage_filename)
        .raft_storage_filename(raft_storage_filename)
        .build().unwrap();

    run_charm_server(charm_server_config).await;
}

