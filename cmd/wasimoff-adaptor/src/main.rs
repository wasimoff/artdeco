use std::{collections::HashMap, fs, path::Path, pin::pin};

use artdeco::{
    daemon::wasimoff::wasimoff_broker,
    scheduler::{fixed::Fixed, roundrobin::RoundRobin},
    task::TaskExecutable,
};
use bytes::Bytes;
use clap::{Parser, ValueEnum, arg, command};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_websockets::{Message, ServerBuilder};
use tracing::{debug, error, info};
use tracing_subscriber::EnvFilter;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Socket listen address
    #[arg(long)]
    socket: String,

    /// Nats broker URL
    #[arg(long)]
    broker_url: String,

    /// Location(s) of WASM binaries
    #[arg(long)]
    binaries: Vec<String>,

    /// Scheduler
    #[arg(value_enum, long, default_value_t = Scheduler::RoundRobin)]
    scheduler: Scheduler,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum Scheduler {
    /// Fixed Scheduler
    Fixed,
    /// RoundRobin Scheduler
    RoundRobin,
}

#[tokio::main]
async fn main() {
    let filter = EnvFilter::new("error").add_directive("artdeco=debug".parse().unwrap());
    let subscriber = tracing_subscriber::fmt().with_env_filter(filter).finish();
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|_err| eprintln!("Unable to set global default subscriber"))
        .unwrap();

    let args = Args::parse();

    let listener = TcpListener::bind(&args.socket).await.unwrap();
    debug!("Initialized websocket at {}", args.socket);
    let (stream, _addr) = listener.accept().await.unwrap();
    let (_request, ws_stream) = ServerBuilder::new().accept(stream).await.unwrap();

    let mapped_stream =
        ws_stream.filter_map(async |msg| msg.ok().map(|msg| msg.into_payload().into()));
    let mapped_sink = pin![
        mapped_stream
            .with::<_, _, _, anyhow::Error>(async |bytes: Bytes| Ok(Message::binary(bytes)))
    ];

    info!("Accepted websocket connection from");

    let binaries = parse_binaries(args.binaries);
    match args.scheduler {
        Scheduler::Fixed => wasimoff_broker(mapped_sink, Fixed::new(), args.broker_url, &binaries)
            .await
            .unwrap(),
        Scheduler::RoundRobin => {
            wasimoff_broker(mapped_sink, RoundRobin::new(), args.broker_url, &binaries)
                .await
                .unwrap()
        }
    }
}

fn parse_binaries(binary_locations: Vec<String>) -> HashMap<String, TaskExecutable> {
    let mut executables = HashMap::new();

    for location in binary_locations {
        let path = Path::new(&location);

        // Verify the file exists
        if !path.exists() {
            error!("Warning: Binary file does not exist: {}", location);
            continue;
        }

        // Verify it's a file (not a directory)
        if !path.is_file() {
            error!("Warning: Path is not a file: {}", location);
            continue;
        }

        // Verify it ends in .wasm
        if !location.ends_with(".wasm") {
            error!("Warning: Binary file does not end in .wasm: {}", location);
            continue;
        }

        // Read the binary content
        match fs::read(path) {
            Ok(content) => {
                let executable = TaskExecutable::new(content);
                let hash_ref = executable.hash_ref().clone();
                executables.insert(hash_ref, executable);
                debug!("Loaded binary: {} -> {}", location, executables.len());
            }
            Err(e) => {
                error!("Error reading binary file {}: {}", location, e);
            }
        }
    }

    executables
}
