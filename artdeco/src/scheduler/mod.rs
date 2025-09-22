use std::time::Instant;

use nid::Nanoid;

use crate::{
    offloader::ProviderAnnounce,
    task::{Task, TaskResult},
};

pub mod bandit;
pub mod fixed;
pub mod roundrobin;

pub enum ProviderState {
    Connected,
    Disconnected,
    Failure,
}

pub enum Output<M> {
    /// Scheduler timeout, indicates when `poll_output` should be called next
    Timeout(Instant),
    /// Establish a new provider connection
    Connect(Nanoid),
    /// Send task to provider
    Offload(Nanoid, Task<M>),
}

pub trait Scheduler<M> {
    /// Scheduler timeout
    ///
    /// Advances time internally. Afterwards new Events via `poll_output` may be available.
    fn handle_timeout(&mut self, instant: Instant);

    /// Inform scheduler about new providers
    fn handle_announce(&mut self, announce: ProviderAnnounce);

    /// Provider state update.
    ///
    /// Connected/Disconnected/Failure events
    fn handle_provider_state(&mut self, uuid: Nanoid, provider_state: ProviderState);

    fn handle_taskresult(
        &mut self,
        uuid: Nanoid,
        task_result: TaskResult<M>,
    ) -> Option<TaskResult<M>>;

    /// Poll scheduler for new events
    fn poll_output(&mut self) -> Output<M>;

    /// Schedule a task
    fn schedule(&mut self, task: Task<M>);
}
