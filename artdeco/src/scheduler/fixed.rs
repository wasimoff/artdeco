use std::time::{Duration, Instant};

use nid::Nanoid;

use crate::{
    scheduler::{Output, Scheduler},
    task::{Task, TaskResult},
};

pub struct Fixed {
    fixed_uuid: Option<Nanoid>,
    connected: bool,
    task_list: Vec<Task<()>>,
    last_instant: Instant,
}

impl Fixed {
    pub fn new() -> Self {
        Self {
            fixed_uuid: None,
            connected: false,
            task_list: Vec::new(),
            last_instant: Instant::now(),
        }
    }
}

impl Scheduler<()> for Fixed {
    fn handle_timeout(&mut self, instant: std::time::Instant) {
        self.last_instant = instant;
    }

    fn handle_announce(&mut self, announce: crate::offloader::ProviderAnnounce) {
        if self.fixed_uuid.is_none() {
            self.fixed_uuid = Some(announce.announce.id);
        }
    }

    fn handle_provider_state(
        &mut self,
        uuid: Nanoid,
        provider_state: super::ProviderState,
        instant: std::time::Instant,
    ) {
        if self.fixed_uuid.is_some_and(|fixed_id| fixed_id == uuid)
            && matches!(provider_state, super::ProviderState::Connected)
        {
            self.connected = true
        }
    }

    fn poll_output(&mut self) -> super::Output<()> {
        if let Some(dest) = self.fixed_uuid {
            if !self.connected {
                return Output::Connect(dest);
            } else if let Some(task) = self.task_list.pop() {
                return Output::Offload(dest, task);
            }
        }
        return Output::Timeout(self.last_instant + Duration::from_secs(10));
    }

    fn schedule(&mut self, task: Task<()>) {
        self.task_list.push(task);
    }

    fn handle_taskresult(
        &mut self,
        uuid: Nanoid,
        task_result: TaskResult<()>,
    ) -> Option<TaskResult<()>> {
        Some(task_result)
    }
}
