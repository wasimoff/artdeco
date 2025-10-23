use std::{
    marker::PhantomData,
    time::{Instant, SystemTime},
};

use bytes::Bytes;
use futures::Sink;
use nid::Nanoid;
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
pub struct Task<M> {
    pub executable: TaskExecutable,
    pub args: Vec<String>,
    pub id: TaskId,
    pub deadline: Option<SystemTime>,
    pub metrics: TaskMetrics<M>,
}

#[derive(Debug, Clone, Default)]
pub struct TaskMetrics<M> {
    pub executor_id: Option<Nanoid>,
    pub trace: Vec<TraceEvent<M>>,
}

#[derive(Debug, Clone)]
pub enum TraceEvent<M> {
    External(Vec<u8>),
    Scheduler(M),
    SchedulerEnter(Instant),
    SchedulerScheduled(Instant),
    WasimoffSerialized(Instant),
    //ConnectionSend(Instant),
    //ConnectionReceive(Instant),
    WasimoffDeserialized(Instant),
    SchedulerResultEnter(Instant),
    SchedulerLeave(Instant),
    // other events
}

pub trait WasimoffTraceEvent {
    fn to_wasimoff(&self) -> Option<wasimoff::task::TraceEvent>;
}

impl WasimoffTraceEvent for () {
    fn to_wasimoff(&self) -> Option<wasimoff::task::TraceEvent> {
        None
    }
}

impl<M> WasimoffTraceEvent for TraceEvent<M>
where
    M: WasimoffTraceEvent,
{
    fn to_wasimoff(&self) -> Option<wasimoff::task::TraceEvent> {
        match self {
            TraceEvent::External(_items) => None,
            TraceEvent::Scheduler(scheduler_event) => scheduler_event.to_wasimoff(),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct Workload<S, D, M> {
    pub executable: TaskExecutable,
    pub args: Vec<String>,
    pub deadline: Option<SystemTime>,
    pub response_channel: S,
    pub custom_data: D,
    pub metrics_type: PhantomData<M>,
}

pub(crate) struct AssociatedData<S, D> {
    pub response_channel: S,
    pub custom_data: D,
}

impl<M: Default, D, S: Sink<WorkloadResult<D, M>>> Workload<S, D, M> {
    pub(crate) fn into_task(self, slab: &mut Slab<AssociatedData<S, D>>) -> Task<M> {
        let Self {
            executable,
            args,
            response_channel,
            deadline,
            custom_data,
            metrics_type: _,
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
pub struct TaskResult<M> {
    pub id: TaskId,
    pub status: Status,
    pub metrics: TaskMetrics<M>,
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

impl<M> TaskResult<M> {
    pub(crate) fn into_workload_result<D, S: Sink<WorkloadResult<D, M>>>(
        self,
        slab: &mut Slab<AssociatedData<S, D>>,
    ) -> (S, WorkloadResult<D, M>) {
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

    pub(crate) fn into_task(self) -> Task<M> {
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
pub struct WorkloadResult<D, M> {
    // timestamps, error/success code, std...
    pub status: Status,
    pub metrics: TaskMetrics<M>,
    pub custom_data: D,
}
