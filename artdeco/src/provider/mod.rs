use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

use nid::Nanoid;

use crate::{
    provider::wasimoff::{WasimoffConfig, WasimoffProvider},
    task::{Task, TaskMetrics, TaskResult},
};

pub mod wasimoff;

#[derive(PartialEq, Eq, Hash, Clone, Copy)]
pub struct TaskHandle {
    pub(crate) id: u64,
}

pub enum Input {
    Timeout(Instant),
    ProviderReceive(Instant, Nanoid, Vec<u8>),
}

pub enum Output<M> {
    TaskResult(Nanoid, TaskResult<M>),
    ProviderTransmit(Nanoid, Vec<u8>),
    Timeout(Instant),
}

pub struct ProviderManager<M> {
    wasimoff_providers: HashMap<Nanoid, WasimoffProvider<M>>,
    output_buffer: VecDeque<Output<M>>,
    last_instant: Instant,
}

impl<M> ProviderManager<M> {
    pub fn new() -> Self {
        Self {
            wasimoff_providers: HashMap::new(),
            output_buffer: VecDeque::new(),
            last_instant: Instant::now(),
        }
    }

    pub fn handle_input(&mut self, input: Input) {
        match input {
            Input::Timeout(instant) => {
                self.update_instant(instant);
            }
            Input::ProviderReceive(instant, nanoid, items) => {
                self.update_instant(instant);
                if let Some(provider) = self.wasimoff_providers.get_mut(&nanoid) {
                    provider.handle_input(&items, instant);
                }
            }
        }
    }

    fn update_instant(&mut self, instant: Instant) {
        self.last_instant = instant;
        for (_, provider) in &mut self.wasimoff_providers {
            provider.handle_timeout(instant);
        }
    }

    pub fn offload(&mut self, task: Task<M>, destination: Nanoid) {
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

    pub fn poll_output(&mut self) -> Output<M> {
        let mut smallest_timeout = self.last_instant + Duration::from_secs(10);

        for (id, provider) in &mut self.wasimoff_providers {
            match provider.poll_output() {
                wasimoff::Output::Transmit(items) => self
                    .output_buffer
                    .push_back(Output::ProviderTransmit(*id, items)),
                wasimoff::Output::TaskResult(task_result) => self
                    .output_buffer
                    .push_back(Output::TaskResult(*id, task_result)),
                wasimoff::Output::Timeout(instant) => {
                    smallest_timeout = smallest_timeout.min(instant)
                }
            }
        }
        self.output_buffer
            .pop_front()
            .unwrap_or(Output::Timeout(smallest_timeout))
    }
}
