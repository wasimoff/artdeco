use std::{
    collections::{HashMap, VecDeque},
    time::Instant,
};

use nid::Nanoid;

use crate::{
    provider::wasimoff::{WasimoffConfig, WasimoffProvider},
    task::Task,
};

pub mod wasimoff;

#[derive(PartialEq, Eq, Hash, Clone, Copy)]
pub struct TaskHandle {
    pub(crate) id: u64,
}

#[derive(Default, Clone, Copy)]
pub struct TaskMetrics {
    transmitTime: u64,
    startTime: u64,
    endTime: u64,
    receivedTime: u64,
}

#[derive(Clone)]
pub enum TaskStatus {
    Running,
    Finished {
        metrics: TaskMetrics,
        exit_code: i32,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        artifacts: Vec<u8>,
    },
    ExecutionError(String),
    QoSError(String),
}

pub enum Input {
    Timeout(Instant),
    ProviderReceive(Nanoid, Vec<u8>),
}

pub enum Output {
    TaskStatusUpdate(TaskStatus),
    ProviderTransmit(Nanoid, Vec<u8>),
    None,
}

pub struct ProviderManager {
    wasimoff_providers: HashMap<Nanoid, WasimoffProvider>,
    output_buffer: VecDeque<Output>,
}

impl ProviderManager {
    pub fn new() -> Self {
        Self {
            wasimoff_providers: HashMap::new(),
            output_buffer: VecDeque::new(),
        }
    }

    pub fn handle_input(&mut self, input: Input) {
        match input {
            Input::Timeout(_instant) => {
                // Ignore Timeout
            }
            Input::ProviderReceive(nanoid, items) => {
                if let Some(provider) = self.wasimoff_providers.get_mut(&nanoid) {
                    provider.handle_input(&items);
                }
            }
        }
    }

    pub fn offload(&mut self, task: Task, destination: Nanoid) {
        if let Some(provider) = self.wasimoff_providers.get_mut(&destination) {
            provider.offload(task);
        }
    }

    pub fn create(&mut self, id: Nanoid) {
        if self.wasimoff_providers.get(&id).is_none() {
            let config = WasimoffConfig {
                client_identifier: id,
            };
            self.wasimoff_providers
                .insert(id, WasimoffProvider::new(config));
        }
    }

    pub fn poll_output(&mut self) -> Output {
        for (id, provider) in &mut self.wasimoff_providers {
            if let Some(output) = provider.poll_output() {
                self.output_buffer
                    .push_back(Output::ProviderTransmit(*id, output.0));
            }
        }
        self.output_buffer.pop_front().unwrap_or(Output::None)
    }
}
