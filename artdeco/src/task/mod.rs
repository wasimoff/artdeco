use std::{marker::PhantomData, time::SystemTime};

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
pub struct Task<M> {
    pub executable: TaskExecutable,
    pub args: Vec<String>,
    pub id: TaskId,
    pub deadline: Option<SystemTime>,
    pub metrics: TaskMetrics<M>,
}

#[derive(Debug, Clone)]
pub struct TaskMetrics<D> {
    phantom_data: PhantomData<D>, /*pub(crate) creation_timestamp: Instant,
                                  pub(crate) scheduling_timestamp: Vec<Instant>,
                                  pub(crate) sending_timestamp: Instant,
                                  pub(crate) receiving_timestamp: Instant,
                                  // retry counter
                                  // local queue instants
                                  pub(crate) result_timestamp: Instant,*/
}

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

impl<M, D, S: Sink<WorkloadResult<D, M>>> Workload<S, D, M> {
    pub(crate) fn to_task(self, slab: &mut Slab<AssociatedData<S, D>>) -> Task<M> {
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
            metrics: TaskMetrics {
                phantom_data: PhantomData,
            },
            deadline,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TaskResult<M> {
    pub id: TaskId,
    pub status: Status,
    pub metrics: TaskMetrics<M>,
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

impl<M> TaskResult<M> {
    pub(crate) fn to_workload_result<D, S: Sink<WorkloadResult<D, M>>>(
        self,
        slab: &mut Slab<AssociatedData<S, D>>,
    ) -> (S, WorkloadResult<D, M>) {
        let Self {
            id,
            status,
            metrics,
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
            return (response_channel, workload_result);
        } else {
            panic!("should not be called with scheduler tasks");
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
