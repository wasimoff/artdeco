use std::time::SystemTime;

use bytes::Bytes;
use futures::Sink;
use sha2::{Digest, Sha256};
use slab::Slab;

use crate::protocol::wasimoff;

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
    pub deadline: Option<SystemTime>,
    pub metrics: TaskMetrics,
}

#[derive(Debug, Clone, Default)]
pub struct TaskMetrics {
    pub executor_id: Option<String>,
    pub wasimoff_trace: Vec<wasimoff::task::TraceEvent>,
}

impl TaskMetrics {
    pub fn push_trace_event_now(
        &mut self,
        event: wasimoff::task::trace_event::EventType,
        msg: Option<String>,
    ) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;

        let trace_event = wasimoff::task::TraceEvent {
            unixnano: Some(now),
            event: Some(::protobuf::EnumOrUnknown::new(event)),
            details: msg,
            special_fields: ::protobuf::SpecialFields::new(),
        };

        self.wasimoff_trace.push(trace_event);
    }
}

#[derive(Clone)]
pub struct Workload<S, D> {
    pub executable: TaskExecutable,
    pub args: Vec<String>,
    pub deadline: Option<SystemTime>,
    pub response_channel: S,
    pub custom_data: D,
}

pub(crate) struct AssociatedData<S, D> {
    pub response_channel: S,
    pub custom_data: D,
}

impl<D, S: Sink<WorkloadResult<D>>> Workload<S, D> {
    pub(crate) fn into_task(self, slab: &mut Slab<AssociatedData<S, D>>) -> Task {
        let Self {
            executable,
            args,
            response_channel,
            deadline,
            custom_data,
        } = self;
        let id = slab.insert(AssociatedData {
            response_channel,
            custom_data,
        });
        Task {
            executable,
            args,
            id: TaskId::Consumer(id),
            metrics: TaskMetrics::default(),
            deadline,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TaskResult {
    pub id: TaskId,
    pub status: Status,
    pub metrics: TaskMetrics,
    pub(crate) executable: TaskExecutable,
    pub(crate) args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub(crate) fn into_workload_result<D, S: Sink<WorkloadResult<D>>>(
        self,
        slab: &mut Slab<AssociatedData<S, D>>,
    ) -> (S, WorkloadResult<D>) {
        let Self {
            id,
            status,
            metrics,
            ..
        } = self;
        if let TaskId::Consumer(key) = id {
            let AssociatedData {
                response_channel,
                custom_data,
            } = slab.remove(key);
            let workload_result = WorkloadResult {
                status,
                metrics,
                custom_data,
            };
            (response_channel, workload_result)
        } else {
            panic!("should not be called with scheduler tasks");
        }
    }

    pub(crate) fn into_task(self) -> Task {
        let Self {
            id,
            status: _,
            metrics,
            executable,
            args,
        } = self;
        Task {
            executable,
            args,
            id,
            deadline: None,
            metrics,
        }
    }
}

#[derive(Debug)]
pub struct WorkloadResult<D> {
    // timestamps, error/success code, std...
    pub status: Status,
    pub metrics: TaskMetrics,
    pub custom_data: D,
}
