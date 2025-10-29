use std::{
    fmt::Display,
    time::{Duration, Instant},
};

use artdeco::{
    daemon::nats::daemon_nats,
    protocol::wasimoff::task::trace_event::EventType,
    scheduler::fixed::Fixed,
    task::{TaskExecutable, Workload, WorkloadResult},
};
use futures::{StreamExt, channel::mpsc};

use tokio::time::sleep;
use tracing::info;
use tracing_subscriber::EnvFilter;

const FIBBONACCI_WASM: &[u8; 59778] = include_bytes!("../../integration-test/fibbonacci.wasm");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::new("info")
        .add_directive("stun_types=error".parse()?)
        .add_directive("artdeco=info".parse()?);
    let subscriber = tracing_subscriber::fmt().with_env_filter(filter).finish();
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|_err| eprintln!("Unable to set global default subscriber"))
        .unwrap();

    // Create a task queue
    let (mut sender, receiver) = mpsc::channel(2);
    // Create response queue
    let (response_sender, mut response_receiver) = mpsc::channel(2);

    tokio::spawn(async move {
        // Create a scheduler
        let scheduler = Fixed::new();
        let args: Vec<String> = std::env::args().collect();
        let binding = "artdeco.devpi.de".to_string();
        let nats_url = args.get(1).unwrap_or(&binding);
        daemon_nats(receiver, scheduler, nats_url).await.unwrap();
    });

    // Create a workload
    let exec = TaskExecutable::new(FIBBONACCI_WASM as &[u8]);
    let workload = Workload {
        executable: exec,
        args: vec!["fibonacci.wasm".to_owned(), "30".to_owned()],
        deadline: None,
        response_channel: response_sender,
        custom_data: (),
    };

    // wait 8 seconds for provider announcements
    sleep(Duration::from_secs(12)).await;

    info!("Sending Tasks");
    // Send the workload to the queue
    sender.start_send(workload.clone())?;
    sender.start_send(workload)?;
    drop(sender); // drop sender so the daemon stops after offloading all tasks    

    let first_result = response_receiver.next().await.unwrap();
    let second_result = response_receiver.next().await.unwrap();

    let first_time = calc_task_time(&first_result);
    let second_time = calc_task_time(&second_result);

    info!("First Result {}", first_time);
    info!("Second Result {}", second_time);
    Ok(())
}

#[derive(Debug)]
struct TaskTime {
    w_connection: Duration,
    wo_connection: Duration,
}

impl Display for TaskTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TaskTime {{ including connection setup: {:?}, without connection setup: {:?} }}",
            self.w_connection, self.wo_connection
        )
    }
}

fn calc_task_time<A>(workload: &WorkloadResult<A>) -> TaskTime {
    // Calculate duration from SchedulerEnter to SchedulerLeave
    let mut scheduled_time = None;
    let mut scheduler_enter_time = None;
    let mut scheduler_leave_time = None;

    for event in &workload.metrics.wasimoff_trace {
        let unixnano = event.unixnano();
        let instant = Instant::now()
            - Duration::from_nanos(
                (std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as i64
                    - unixnano) as u64,
            );
        match event.event() {
            EventType::ArtDecoSchedulerScheduled => scheduled_time = Some(instant),
            EventType::ArtDecoSchedulerLeave => scheduler_leave_time = Some(instant),
            EventType::ArtDecoSchedulerEnter => scheduler_enter_time = Some(instant),
            _ => {}
        }
    }

    if let (Some(enter), Some(leave), Some(scheduled)) =
        (scheduler_enter_time, scheduler_leave_time, scheduled_time)
    {
        TaskTime {
            w_connection: leave.duration_since(enter),
            wo_connection: leave.duration_since(scheduled),
        }
    } else {
        unreachable!()
    }
}
