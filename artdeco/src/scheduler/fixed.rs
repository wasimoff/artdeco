use std::time::Instant;

use nid::Nanoid;
use tracing::debug;

use crate::{
    consumer::TIMEOUT,
    scheduler::{Output, Scheduler},
    task::{Task, TaskResult},
};

pub struct Fixed {
    fixed_uuid: Option<Nanoid>,
    connected: ConnectionStatus,
    task_list: Vec<Task<()>>,
    last_instant: Instant,
}

impl Default for Fixed {
    fn default() -> Self {
        Self::new()
    }
}

enum ConnectionStatus {
    Disconnected,
    WaitingForConnection,
    Connected,
}

impl Fixed {
    pub fn new() -> Self {
        Self {
            fixed_uuid: None,
            connected: ConnectionStatus::Disconnected,
            task_list: Vec::new(),
            last_instant: Instant::now(),
        }
    }
}

impl Scheduler<()> for Fixed {
    fn handle_timeout(&mut self, instant: std::time::Instant) {
        self.last_instant = instant;
    }

    fn handle_announce(&mut self, announce: crate::consumer::ProviderAnnounce) {
        if self.fixed_uuid.is_none() {
            self.fixed_uuid = Some(announce.announce.id);
        }
    }

    fn handle_provider_state(
        &mut self,
        uuid: Nanoid,
        provider_state: super::ProviderState,
        _instant: std::time::Instant,
    ) {
        if self.fixed_uuid.is_some_and(|fixed_id| fixed_id == uuid)
            && matches!(provider_state, super::ProviderState::Connected)
        {
            self.connected = ConnectionStatus::Connected;
        }
    }

    fn poll_output(&mut self) -> super::Output<()> {
        if let Some(dest) = self.fixed_uuid
            && !self.task_list.is_empty()
        {
            match self.connected {
                ConnectionStatus::Disconnected => {
                    self.connected = ConnectionStatus::WaitingForConnection;
                    return Output::Connect(dest);
                }
                ConnectionStatus::Connected => {
                    if let Some(task) = self.task_list.pop() {
                        return Output::Offload(dest, task);
                    }
                }
                ConnectionStatus::WaitingForConnection => {}
            }
        }
        Output::Timeout(self.last_instant + TIMEOUT)
    }

    fn schedule(&mut self, task: Task<()>) {
        self.task_list.push(task);
    }

    fn handle_taskresult(
        &mut self,
        _uuid: Nanoid,
        task_result: TaskResult<()>,
    ) -> Option<TaskResult<()>> {
        debug!("Scheduler task result {:?}", task_result);
        Some(task_result)
    }
}
