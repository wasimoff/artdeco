use crate::{
    offloader::ProviderAnnounce,
    scheduler::{Output, ProviderState, Scheduler},
    task::{Task, TaskResult},
};
use nid::Nanoid;
use std::collections::{HashMap, VecDeque};
use std::time::Instant;

#[derive(Clone)]
struct ProviderInfo {
    announce: ProviderAnnounce,
    state: ProviderState,
}

pub struct SchedulerRoundRobin {
    last_id: Option<Nanoid>,
    providers: HashMap<Nanoid, ProviderInfo>,
    pending_tasks: VecDeque<Task<RoundRobinMetrics>>,
    event_buffer: VecDeque<Output<RoundRobinMetrics>>,
}

impl SchedulerRoundRobin {
    pub fn new() -> Self {
        SchedulerRoundRobin { 
            last_id: None,
            providers: HashMap::new(),
            pending_tasks: VecDeque::new(),
            event_buffer: VecDeque::new(),
        }
    }

    /// Get the next provider ID in round-robin order
    fn get_next_provider(&mut self) -> Option<Nanoid> {
        if self.providers.is_empty() {
            return None;
        }

        // Get all provider IDs and sort them for consistent ordering
        let mut provider_ids: Vec<Nanoid> = self.providers.keys().cloned().collect();
        provider_ids.sort_by_key(|id| id.to_string());

        match &self.last_id {
            None => {
                // First time, start with the first provider
                let next_id = provider_ids.first().cloned()?;
                self.last_id = Some(next_id);
                Some(next_id)
            }
            Some(last) => {
                // Find the next provider in round-robin order
                if let Some(current_index) = provider_ids.iter().position(|id| id == last) {
                    let next_index = (current_index + 1) % provider_ids.len();
                    let next_id = provider_ids[next_index];
                    self.last_id = Some(next_id);
                    Some(next_id)
                } else {
                    // Last provider no longer exists, start from the beginning
                    let next_id = provider_ids.first().cloned()?;
                    self.last_id = Some(next_id);
                    Some(next_id)
                }
            }
        }
    }

    /// Check if a provider is connected
    fn is_provider_connected(&self, provider_id: &Nanoid) -> bool {
        self.providers
            .get(provider_id)
            .map(|info| matches!(info.state, ProviderState::Connected))
            .unwrap_or(false)
    }

    /// Process pending tasks and generate events
    fn process_pending_tasks(&mut self) {
        while let Some(task) = self.pending_tasks.pop_front() {
            if let Some(provider_id) = self.get_next_provider() {
                if self.is_provider_connected(&provider_id) {
                    // Provider is connected, schedule the task directly
                    self.event_buffer.push_back(Output::Offload(provider_id, task));
                } else {
                    // Provider is not connected, connect first then schedule
                    self.event_buffer.push_back(Output::Connect(provider_id));
                    self.event_buffer.push_back(Output::Offload(provider_id, task));
                }
            } else {
                // No providers available, put the task back and break
                self.pending_tasks.push_front(task);
                break;
            }
        }
    }
}

pub struct RoundRobinMetrics {}

impl Scheduler<RoundRobinMetrics> for SchedulerRoundRobin {
    fn handle_timeout(&mut self, _instant: Instant) {
        // Process any pending tasks when we get a timeout
        self.process_pending_tasks();
    }

    fn handle_announce(&mut self, announce: ProviderAnnounce) {
        // Update the internal list of available providers
        let provider_id = announce.announce.id;
        let provider_info = ProviderInfo {
            announce,
            state: ProviderState::Disconnected, // Default state for new announcements
        };
        self.providers.insert(provider_id, provider_info);
        
        // Process any pending tasks since we have a new provider
        self.process_pending_tasks();
    }

    fn handle_provider_state(
        &mut self,
        uuid: Nanoid,
        provider_state: ProviderState,
        _instant: Instant,
    ) {
        // Update the provider state if the provider exists
        if let Some(provider_info) = self.providers.get_mut(&uuid) {
            provider_info.state = provider_state;
            
            // If a provider just connected, try to process pending tasks
            if matches!(provider_info.state, ProviderState::Connected) {
                self.process_pending_tasks();
            }
        }
        // Note: If provider doesn't exist, we could log a warning but for now we ignore it
        // as the provider might announce itself later
    }

    fn handle_taskresult(
        &mut self,
        _uuid: Nanoid,
        task_result: TaskResult<RoundRobinMetrics>,
    ) -> Option<TaskResult<RoundRobinMetrics>> {
        // For round-robin scheduler, we just pass through task results
        Some(task_result)
    }

    fn poll_output(&mut self) -> Output<RoundRobinMetrics> {
        // Return buffered events first
        if let Some(event) = self.event_buffer.pop_front() {
            return event;
        }

        // If no events in buffer, return a timeout
        Output::Timeout(Instant::now() + std::time::Duration::from_secs(1))
    }

    fn schedule(&mut self, task: Task<RoundRobinMetrics>) {
        // Add the task to our pending queue
        self.pending_tasks.push_back(task);
        
        // Try to process it immediately
        self.process_pending_tasks();
    }
}
