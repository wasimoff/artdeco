use crate::{
    offloader::ProviderAnnounce,
    scheduler::{Output, ProviderState, Scheduler},
    task::{Task, TaskResult},
};
use nid::Nanoid;
use std::time::Instant;

pub struct SchedulerRoundRobin {
    last_id: Option<String>,
}

impl SchedulerRoundRobin {
    pub fn new() -> Self {
        SchedulerRoundRobin { last_id: None }
    }
}

pub struct RoundRobinMetrics {}

impl Scheduler<RoundRobinMetrics> for SchedulerRoundRobin {
    fn handle_timeout(&mut self, _instant: Instant) {
        // TODO: Implement timeout handling for round robin scheduler
    }

    fn handle_announce(&mut self, _announce: ProviderAnnounce) {
        // TODO: Implement provider announce handling for round robin scheduler
    }

    fn handle_provider_state(&mut self, _uuid: Nanoid, _provider_state: ProviderState) {
        // TODO: Implement provider state handling for round robin scheduler
    }

    fn handle_taskresult(
        &mut self,
        _uuid: Nanoid,
        _task_result: TaskResult<RoundRobinMetrics>,
    ) -> Option<TaskResult<RoundRobinMetrics>> {
        // TODO: Implement task result handling for round robin scheduler
        None
    }

    fn poll_output(&mut self) -> Output<RoundRobinMetrics> {
        // TODO: Implement proper round robin scheduling logic
        // For now, return a timeout as a placeholder
        Output::Timeout(Instant::now() + std::time::Duration::from_secs(1))
    }

    fn schedule(&mut self, _task: Task<RoundRobinMetrics>) {
        // TODO: Implement task scheduling for round robin scheduler
    }
}
