use bytes::Bytes;
use futures::Sink;
use sha2::{Digest, Sha256};
use slab::Slab;

#[derive(Debug, Clone)]
pub struct TaskExecutable {
    pub(crate) shasum: String,
    pub(crate) content: Bytes,
}

impl TaskExecutable {
    pub fn new(content: impl Into<Bytes>) -> Self {
        let content_bytes: Bytes = content.into();
        let hash = Sha256::digest(&content_bytes);
        let hex = "sha256:".to_owned()
            + &hash
                .iter()
                .map(|d| format!("{:02x}", d))
                .collect::<String>();
        Self {
            shasum: hex,
            content: content_bytes,
        }
    }

    pub fn hash_ref(&self) -> &String {
        &self.shasum
    }

    pub fn content(&self) -> &[u8] {
        &self.content
    }
}

#[derive(Debug, Clone)]
pub enum TaskId {
    Consumer(usize),
    Scheduler(usize),
}

#[derive(Debug, Clone)]
pub struct Task {
    pub executable: TaskExecutable,
    pub args: Vec<String>,
    pub id: TaskId,
    pub metrics: TaskMetrics,
}

#[derive(Debug, Clone)]
pub struct TaskMetrics {
    /*pub(crate) creation_timestamp: Instant,
    pub(crate) scheduling_timestamp: Vec<Instant>,
    pub(crate) sending_timestamp: Instant,
    pub(crate) receiving_timestamp: Instant,
    // retry counter
    // local queue instants
    pub(crate) result_timestamp: Instant,*/
}

pub struct Workload<S> {
    pub executable: TaskExecutable,
    pub args: Vec<String>,
    pub response_channel: S,
}

impl<S: Sink<WorkloadResult>> Workload<S> {
    pub(crate) fn to_task(self, slab: &mut Slab<S>) -> Task {
        let Self {
            executable,
            args,
            response_channel,
        } = self;
        let id = slab.insert(response_channel);
        Task {
            executable,
            args,
            id: TaskId::Consumer(id),
            metrics: TaskMetrics {},
        }
    }
}

#[derive(Debug, Clone)]
pub struct TaskResult {
    pub id: TaskId,
    pub status: Status,
    pub metrics: TaskMetrics,
}

#[derive(Debug, Clone)]
pub enum Status {
    Error(String),
    QoSError(String),
    Finished {
        exit_code: i32,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        output_file: Option<Vec<u8>>,
    },
}

impl TaskResult {
    pub(crate) fn to_workload_result<S: Sink<WorkloadResult>>(
        self,
        slab: &mut Slab<S>,
    ) -> (S, WorkloadResult) {
        let Self {
            id,
            status,
            metrics,
        } = self;
        if let TaskId::Consumer(key) = id {
            let sink = slab.remove(key);
            let workload_result = WorkloadResult { status, metrics };
            return (sink, workload_result);
        } else {
            panic!("should not be called with scheduler tasks");
        }
    }
}

#[derive(Debug)]
pub struct WorkloadResult {
    // timestamps, error/success code, std...
    pub status: Status,
    pub metrics: TaskMetrics,
}
