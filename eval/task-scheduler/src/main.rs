use artdeco::{
    nats::daemon_nats,
    scheduler::roundrobin::SchedulerRoundRobin,
    task::{Task, TaskExecutable},
};
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create a task queue with some example tasks
    let (sender, receiver) = mpsc::channel(100);

    // Example: Load a WASM binary and create tasks
    if let Ok(wasm_content) = std::fs::read("../../wasi-apps/fibonacci/exe.wasm") {
        let task = Task {
            executable: TaskExecutable {
                file_ref: "fibonacci.wasm".to_owned(),
                content: wasm_content,
            },
            args: vec!["fibonacci.wasm".to_owned(), "10".to_owned()],
            submit_instant: Instant::now(),
        };
        sender.send(task).await?;
    }

    let receiver_stream = ReceiverStream::new(receiver);

    // Use RoundRobin scheduler instead of Fixed
    let scheduler = SchedulerRoundRobin::new();
    // send to different thread
    daemon_nats(receiver_stream, scheduler).await
    // loop through task result stream until its closed
}
