use artdeco::{
    nats::daemon_nats,
    scheduler::fixed::Fixed,
    task::{TaskExecutable, Workload},
};
use futures::sink::drain;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::Level;

const FIBBONACCI_WASM: &[u8; 59778] = include_bytes!("../fibbonacci.wasm");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|_err| eprintln!("Unable to set global default subscriber"))
        .unwrap();

    // Create a task queue
    let (sender, receiver) = mpsc::channel(1);

    // Create a workload
    let exec = TaskExecutable::new(FIBBONACCI_WASM as &[u8]);
    let workload = Workload {
        executable: exec,
        args: vec!["fibonacci.wasm".to_owned(), "10".to_owned()],
        response_channel: drain(),
    };

    // Send the workload to the queue
    sender.send(workload).await?;
    drop(sender); // drop sender so the daemon stops after offloading all tasks

    // Create a scheduler
    let scheduler = Fixed::new();
    let receiver_stream = ReceiverStream::new(receiver);
    daemon_nats(receiver_stream, scheduler, "nats").await
}
