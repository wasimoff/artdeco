use std::marker::PhantomData;

use artdeco::{
    daemon::nats::daemon_nats,
    scheduler::fixed::Fixed,
    task::{Status, TaskExecutable, Workload},
};
use futures::{StreamExt, channel::mpsc};

use tracing::info;
use tracing_subscriber::EnvFilter;

const FIBBONACCI_WASM: &[u8; 59778] = include_bytes!("../fibbonacci.wasm");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::new("info")
        .add_directive("artdeco=debug".parse()?)
        .add_directive("stun_types=error".parse()?);
    let subscriber = tracing_subscriber::fmt().with_env_filter(filter).finish();
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|_err| eprintln!("Unable to set global default subscriber"))
        .unwrap();

    // Create a task queue
    let (mut sender, receiver) = mpsc::channel(2);
    // Create response queue
    let (response_sender, mut response_receiver) = mpsc::channel(2);

    // Create a workload
    let exec = TaskExecutable::new(FIBBONACCI_WASM as &[u8]);
    let workload = Workload {
        executable: exec,
        args: vec!["fibonacci.wasm".to_owned(), "10".to_owned()],
        deadline: None,
        response_channel: response_sender,
        custom_data: (),
        metrics_type: PhantomData {},
    };

    // Send the workload to the queue
    sender.start_send(workload.clone())?;
    sender.start_send(workload)?;
    drop(sender); // drop sender so the daemon stops after offloading all tasks

    // Create a scheduler
    let scheduler = Fixed::new();
    //let receiver_stream = ReceiverStream::new(receiver);
    let args: Vec<String> = std::env::args().collect();
    let binding = "nats".to_string();
    let nats_url = args.get(1).unwrap_or(&binding);
    daemon_nats(receiver, scheduler, nats_url).await?;
    let result = response_receiver.next().await.unwrap();
    let result2 = response_receiver.next().await.unwrap();
    info!("received response in main, {:?}", result);
    assert_eq!(
        result.status,
        Status::QoSError("qos: no idle workers for immediate mode".to_string())
    );
    info!("received response in main, {:?}", result2);
    Ok(())
}
