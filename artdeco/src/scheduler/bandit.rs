use std::{cmp::Ordering, time::Instant};

use nid::Nanoid;

use crate::{
    consumer::ProviderAnnounce,
    scheduler::Scheduler,
    task::{Task, TaskResult},
};

use super::{Output, ProviderState};

pub struct Bandit {
    buckets: Vec<Bucket>,
}

#[derive(Eq, Ord, Default, Clone)]
struct Bucket {
    performance_value: i32,
    providers: Vec<Nanoid>,
}

impl PartialEq for Bucket {
    fn eq(&self, other: &Self) -> bool {
        self.performance_value == other.performance_value
    }
}

impl PartialOrd for Bucket {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.performance_value.partial_cmp(&other.performance_value)
    }
}

impl Bandit {
    pub fn new(number_of_buckets: usize) -> Self {
        let mut vec = Vec::new();
        vec.resize(
            number_of_buckets,
            Bucket {
                performance_value: 0,
                providers: Vec::new(),
            },
        );
        Self { buckets: vec }
    }
}

#[derive(Default, Debug)]
pub struct BanditMetrics {}

impl Scheduler<BanditMetrics> for Bandit {
    fn handle_timeout(&mut self, instant: Instant) {
        todo!()
    }

    fn handle_announce(&mut self, announce: ProviderAnnounce) {
        todo!()
    }

    fn handle_provider_state(
        &mut self,
        uuid: nid::Nanoid,
        provider_state: ProviderState,
        instant: Instant,
    ) {
        todo!()
    }

    fn handle_taskresult(
        &mut self,
        uuid: nid::Nanoid,
        task_result: TaskResult<BanditMetrics>,
    ) -> Option<TaskResult<BanditMetrics>> {
        todo!()
    }

    fn poll_output(&mut self) -> Output<BanditMetrics> {
        todo!()
    }

    fn schedule(&mut self, task: Task<BanditMetrics>) {
        todo!()
    }
}
