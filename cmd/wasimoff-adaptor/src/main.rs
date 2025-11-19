use std::{collections::HashMap, fs, path::Path};

use anyhow::Result;
use artdeco::{
    daemon::wasimoff::wasimoff_broker,
    scheduler::{drift::Drift, fixed::Fixed, roundrobin::RoundRobin},
    task::TaskExecutable,
};
use bytes::Bytes;
use clap::{Parser, ValueEnum, arg, command};
use futures::{SinkExt, StreamExt, channel::mpsc};
use tokio::net::TcpListener;
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        Message,
        handshake::server::{Request, Response},
    },
};
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

    /// Drift increase threshold
    #[arg(long, default_value_t = 5)]
    drift_increase_threshold: usize,

    /// Drift decrease threshold
    #[arg(long, default_value_t = 1)]
    drift_decrease_threshold: usize,

    /// Max history size for Drift/Uniform/Greedy
    #[arg(long, default_value_t = 100)]
    max_history: usize,

    /// Connection pool size for Drift/Uniform/Greedy
    #[arg(long, default_value_t = 0)]
    connection_pool: usize,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum Scheduler {
    /// Fixed Scheduler
    Fixed,
    /// RoundRobin Scheduler
    RoundRobin,
    /// Uniform Scheduler
    Uniform,
    /// Drift Scheduler
    Drift,
    /// Greedy Scheduler
    Greedy,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::new("error")
        .add_directive("artdeco=debug".parse().unwrap())
        .add_directive("wasimoff_adaptor=debug".parse().unwrap())
        .add_directive("artdeco::daemon::wasimoff=trace".parse().unwrap());
    let subscriber = tracing_subscriber::fmt().with_env_filter(filter).finish();
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|_err| eprintln!("Unable to set global default subscriber"))
        .unwrap();

    let args = Args::parse();
    let binaries = parse_binaries(args.binaries);

    let (task_sender, task_receiver) = mpsc::channel::<Bytes>(100);
    let (result_sender, result_receiver) = mpsc::channel::<Bytes>(100);

    let broker_daemon = tokio::spawn(async move {
        match args.scheduler {
            Scheduler::Fixed => wasimoff_broker(
                task_receiver,
                result_sender,
                Fixed::new(),
                args.broker_url,
                &binaries,
            )
            .await
            .unwrap(),
            Scheduler::RoundRobin => wasimoff_broker(
                task_receiver,
                result_sender,
                RoundRobin::new(),
                args.broker_url,
                &binaries,
            )
            .await
            .unwrap(),
            Scheduler::Uniform => wasimoff_broker(
                task_receiver,
                result_sender,
                Drift::new_uniform(args.max_history, args.connection_pool),
                args.broker_url,
                &binaries,
            )
            .await
            .unwrap(),
            Scheduler::Drift => wasimoff_broker(
                task_receiver,
                result_sender,
                Drift::new_drift(
                    args.drift_increase_threshold,
                    args.drift_decrease_threshold,
                    args.max_history,
                    args.connection_pool,
                ),
                args.broker_url,
                &binaries,
            )
            .await
            .unwrap(),
            Scheduler::Greedy => wasimoff_broker(
                task_receiver,
                result_sender,
                Drift::new_greedy(args.max_history, args.connection_pool),
                args.broker_url,
                &binaries,
            )
            .await
            .unwrap(),
        }
    });

    let listener = TcpListener::bind(&args.socket).await.unwrap();
    debug!("Initialized websocket at {}", args.socket);
    let (stream, _addr) = listener.accept().await.unwrap();
    let ws_stream = accept_hdr_async(stream, |_request: &Request, mut response: Response| {
        response.headers_mut().append(
            "Sec-WebSocket-Protocol",
            "wasimoff_provider_v1_protobuf".parse().unwrap(),
        );
        Ok(response)
    })
    .await
    .unwrap();

    let (ws_write, ws_read) = ws_stream.split();

    let stream_forward = ws_read
        .filter_map(|msg| async move {
            msg.ok().and_then(|msg| match msg {
                Message::Binary(bytes) => Some(Ok::<_, anyhow::Error>(bytes)),
                _ => None,
            })
        })
        .forward(task_sender.sink_err_into());

    let result_forward = result_receiver
        .map(|item| Ok::<_, anyhow::Error>(Message::binary(item)))
        .forward(ws_write.sink_err_into());

    tokio::try_join!(stream_forward, result_forward)?;

    broker_daemon.await.unwrap();
    info!("Accepted websocket connection");
    Ok(())
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
