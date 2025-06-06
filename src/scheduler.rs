use log::info;

use crate::{ProviderCacheEntry, Task};

trait Scheduler {
    async fn schedule(&mut self, task: Task);
    fn update_provider(&mut self, provider: ProviderCacheEntry);
    fn remove_provider(&mut self, provider: ProviderCacheEntry);
}

struct SchedulerRoundRobin {
    provider_list: Vec<ProviderCacheEntry>,
    next_index: usize,
}

impl Scheduler for SchedulerRoundRobin {
    async fn schedule(&mut self, task: Task) {
        info!("Scheduling task: {:?}", task);
    }

    fn update_provider(&mut self, provider: ProviderCacheEntry) {
        todo!()
    }

    fn remove_provider(&mut self, provider: ProviderCacheEntry) {
        // Find the index of the provider in the provider_list
        if let Some(index) = self.provider_list.iter().position(|p| p == &provider) {
            // Remove the provider from the list
            self.provider_list.remove(index);

            // If the found index is <= last_index, decrement last_index by one
            if index <= self.next_index {
                self.next_index = self.next_index.saturating_sub(1);
            }
        }
    }
}
