use artdeco::{
    nats::daemon_nats,
    scheduler::fixed::Fixed,
    task::{TaskExecutable, Workload},
};
use futures::{StreamExt, channel::mpsc};

use tracing::{Level, info};

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
    let (mut sender, receiver) = mpsc::channel(1);
    // Create response queue
    let (response_sender, mut response_receiver) = mpsc::channel(1);

    // Create a workload
    let exec = TaskExecutable::new(FIBBONACCI_WASM as &[u8]);
    let workload = Workload {
        executable: exec,
        args: vec!["fibonacci.wasm".to_owned(), "10".to_owned()],
        response_channel: response_sender,
    };

    // Send the workload to the queue
    sender.start_send(workload)?;
    drop(sender); // drop sender so the daemon stops after offloading all tasks

    // Create a scheduler
    let scheduler = Fixed::new();
    //let receiver_stream = ReceiverStream::new(receiver);
    daemon_nats(receiver, scheduler, "nats").await?;
    let result = response_receiver.next().await.unwrap();
    info!("received response in main, {:?}", result);
    Ok(())
}
