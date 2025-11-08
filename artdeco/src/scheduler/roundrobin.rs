use crate::{
    consumer::{ProviderAnnounce, TIMEOUT},
    protocol::wasimoff::task::trace_event::EventType,
    scheduler::{Output, ProviderState, Scheduler},
    task::{Task, TaskResult},
};
use indexmap::IndexMap;
use nid::Nanoid;
use std::time::Instant;
use std::{collections::VecDeque, time::Duration};

// provider is either
// disconnected
// waitingfor connection
// idle
// running

// task is either
// unscheduled
// scheduled to provider x

// task is scheduled to next provider
// waits for connection, if not connected
// then scheduled

// next provider = next not busy provider (idle/disconnected)
// provider automatically disconnected after timeout without announcement
// provider automatically blacklisted after connection timeout
// provider automatically disconnected after timeout idle

#[derive(Clone)]
struct ProviderInfo {
    state: ProviderStatus,
    last_announce: Instant,
    // TODO multiple workers
}

#[derive(Clone)]
enum ProviderStatus {
    Disconnected,
    WaitingForConnection(Task),
    Idle,
    Busy,
}

pub struct RoundRobin {
    next_provider_index: usize,
    last_instant: Instant,
    providers: IndexMap<Nanoid, ProviderInfo>,
    pending_tasks: VecDeque<Task>,
    event_buffer: VecDeque<Output>,
    provider_timeout: Duration,
}

impl RoundRobin {
    pub fn new() -> Self {
        RoundRobin {
            next_provider_index: 0,
            last_instant: Instant::now(),
            providers: IndexMap::new(),
            pending_tasks: VecDeque::new(),
            event_buffer: VecDeque::new(),
            provider_timeout: Duration::from_secs(6000),
        }
    }

    /// Get the next provider ID in round-robin order
    fn next_provider(&mut self) -> Option<Nanoid> {
        if self.providers.is_empty() {
            return None;
        }

        let provider_count = self.providers.len();

        // Search for next available provider starting from next_provider_index
        for _ in 0..provider_count {
            let current_index = self.next_provider_index % provider_count;

            if let Some((provider_id, provider_info)) = self.providers.get_index(current_index) {
                let provider_id = *provider_id;

                if matches!(
                    provider_info.state,
                    ProviderStatus::Idle | ProviderStatus::Disconnected
                ) {
                    // Move to next provider for the next call
                    self.next_provider_index = (current_index + 1) % provider_count;
                    return Some(provider_id);
                }
            }

            // Try next provider
            self.next_provider_index = (current_index + 1) % provider_count;
        }

        // No available providers found
        None
    }

    /// Sort providers alphabetically by their Nanoid
    fn sort_providers(&mut self) {
        self.providers.sort_by_key(|id, _| *id);

        // Reset the next_provider_index since the order has changed
        self.next_provider_index = 0;
    }

    /// Process pending tasks and generate events
    fn process_pending_tasks(&mut self) {
        let Some(mut next_task) = self.pending_tasks.pop_front() else {
            return;
        };
        let Some(next_provider) = self.next_provider() else {
            self.pending_tasks.push_front(next_task);
            return;
        };

        let provider = self
            .providers
            .get_mut(&next_provider)
            .expect("provider existed just in the previous call!?");

        match provider.state {
            ProviderStatus::Disconnected => {
                next_task
                    .metrics
                    .push_trace_event_now(EventType::ArtDecoSchedulerProviderConnect, None);
                provider.state = ProviderStatus::WaitingForConnection(next_task);
                let connect_event = Output::Connect(next_provider);
                self.event_buffer.push_back(connect_event);
            }
            ProviderStatus::Idle => {
                next_task
                    .metrics
                    .push_trace_event_now(EventType::ArtDecoSchedulerProviderOffload, None);
                provider.state = ProviderStatus::Busy;
                let schedule_event = Output::Offload(next_provider, next_task);
                self.event_buffer.push_back(schedule_event);
            }
            _ => unreachable!("scheduler does not select busy or waitingforconnection providers"),
        }
    }
}

impl Default for RoundRobin {
    fn default() -> Self {
        Self::new()
    }
}

impl Scheduler for RoundRobin {
    fn handle_timeout(&mut self, instant: Instant) {
        self.last_instant = instant;
        let initial_count = self.providers.len();

        self.providers.retain(|&_provider_id, provider_info| {
            instant.duration_since(provider_info.last_announce) <= self.provider_timeout
        });

        // If providers were removed, reset the next_provider_index to avoid index out of bounds
        if self.providers.len() != initial_count {
            self.next_provider_index = 0;
        }
    }

    fn handle_announce(&mut self, announce: ProviderAnnounce) {
        let provider_id = announce.announce.id;
        let was_new_provider = !self.providers.contains_key(&provider_id);

        let provider_info = self
            .providers
            .entry(provider_id)
            .or_insert_with(|| ProviderInfo {
                state: ProviderStatus::Disconnected,
                last_announce: announce.last,
            });
        provider_info.last_announce = announce.last;

        // If this was a new provider, sort the providers alphabetically
        if was_new_provider {
            self.sort_providers();
        }

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
        let Some(provider_info) = self.providers.get_mut(&uuid) else {
            return;
        };
        let current_state =
            std::mem::replace(&mut provider_info.state, ProviderStatus::Disconnected);
        match (provider_state, current_state) {
            (ProviderState::Connected, ProviderStatus::Idle | ProviderStatus::Busy) => {
                unreachable!("provider is already idle but now somehow freshly connected")
            }
            (ProviderState::Disconnected, ProviderStatus::Disconnected) => {
                unreachable!("provider is already disconnected")
            }
            (ProviderState::Connected, ProviderStatus::Disconnected) => {
                provider_info.state = ProviderStatus::Idle;
            }
            (ProviderState::Connected, ProviderStatus::WaitingForConnection(mut task)) => {
                task.metrics
                    .push_trace_event_now(EventType::ArtDecoSchedulerProviderOffload, None);
                let schedule_event = Output::Offload(uuid, task);
                self.event_buffer.push_back(schedule_event);
                provider_info.state = ProviderStatus::Busy;
            }
            (ProviderState::Disconnected, ProviderStatus::WaitingForConnection(mut task)) => {
                task.metrics
                    .push_trace_event_now(EventType::ArtDecoSchedulerRequeue, None);
                self.pending_tasks.push_back(task);
                provider_info.state = ProviderStatus::Disconnected;
            }
            (ProviderState::Disconnected, _) => {
                provider_info.state = ProviderStatus::Disconnected;
            }
        }
    }

    fn handle_taskresult(&mut self, uuid: Nanoid, task_result: TaskResult) -> Option<TaskResult> {
        let mut result = None;

        // Look up the provider and update its state to idle if it was busy
        if let Some(provider_info) = self.providers.get_mut(&uuid)
            && matches!(provider_info.state, ProviderStatus::Busy)
        {
            provider_info.state = ProviderStatus::Idle;
        }

        // Check if the task result indicates a QoS error and reschedule if needed
        if matches!(task_result.status, crate::task::Status::QoSError(_)) {
            let mut task = task_result.into_task();
            task.metrics
                .push_trace_event_now(EventType::ArtDecoSchedulerRequeue, None);
            self.pending_tasks.push_back(task);
        } else {
            result = Some(task_result);
        }

        // Try to process any pending tasks since this provider is now available
        self.process_pending_tasks();

        result
    }

    fn poll_output(&mut self) -> Output {
        // Return buffered events first
        if let Some(event) = self.event_buffer.pop_front() {
            return event;
        }

        // If no events in buffer, return a timeout
        Output::Timeout(self.last_instant + TIMEOUT)
    }

    fn schedule(&mut self, task: Task) {
        // Add the task to our pending queue
        self.pending_tasks.push_back(task);

        // Try to process it immediately
        self.process_pending_tasks();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        consumer::ProviderAnnounceMsg,
        scheduler::{Output, ProviderState, Scheduler},
        task::{Task, TaskExecutable, TaskId, TaskMetrics},
    };
    use nid::Nanoid;
    use std::time::SystemTime;

    #[test]
    fn test_roundrobin() {
        let mut scheduler = RoundRobin::new();

        // Create 3 test providers
        let providers = [Nanoid::new(), Nanoid::new(), Nanoid::new()];
        println!("Created providers: {:?}", providers);

        // Announce and connect all providers
        for &provider_id in &providers {
            let announce = ProviderAnnounce {
                announce: ProviderAnnounceMsg {
                    id: provider_id,
                    concurrency: 1,
                    tasks: 0,
                    timestamp: 0,
                    active_connections: 0,
                    max_connections: 0,
                },
                last: Instant::now(),
            };
            scheduler.handle_announce(announce);
            scheduler.handle_provider_state(provider_id, ProviderState::Connected, Instant::now());
            println!("Provider {} announced and connected", provider_id);
        }

        let executable = TaskExecutable::new("test5".as_bytes());

        // Create and schedule 4 tasks
        for i in 1..=4 {
            let task = Task {
                id: TaskId::Consumer(i),
                executable: executable.clone(),
                args: vec![format!("arg{}", i)],
                deadline: Some(SystemTime::now() + std::time::Duration::from_secs(60)),
                metrics: TaskMetrics::default(),
            };
            println!("Scheduling task {}", i);
            scheduler.schedule(task);
            println!(
                "Buffer size after scheduling task {}: {}",
                i,
                scheduler.event_buffer.len()
            );
        }

        // Collect offload events
        let mut offload_events = Vec::new();
        let mut all_events = Vec::new();
        for iteration in 0..10 {
            // Limit iterations to prevent infinite loop
            let output = scheduler.poll_output();
            println!(
                "Iteration {}: {:?}",
                iteration,
                match &output {
                    Output::Offload(id, _) => format!("Offload({})", id),
                    Output::Connect(id) => format!("Connect({})", id),
                    Output::Timeout(_) => "Timeout".to_string(),
                }
            );

            match output {
                Output::Offload(provider_id, _task) => {
                    offload_events.push(provider_id);
                    all_events.push(format!("Offload({})", provider_id));
                    // Only expect 3 offloads initially (one per provider)
                    if offload_events.len() == 3 {
                        break;
                    }
                }
                Output::Timeout(_) => {
                    all_events.push("Timeout".to_string());
                    // No more events available
                    break;
                }
                Output::Connect(id) => {
                    all_events.push(format!("Connect({})", id));
                    // Other event types, continue polling
                    continue;
                }
            }
        }

        println!("All events: {:?}", all_events);
        println!("all offload events collected: {}", offload_events.len());

        // Verify round-robin distribution - should only have 3 offloads (all providers busy)
        // Providers are now sorted alphabetically, not in announcement order
        let mut sorted_providers = providers.clone();
        sorted_providers.sort();
        println!("Providers in announcement order: {:?}", providers);
        println!("Providers in sorted order: {:?}", sorted_providers);
        println!("Offload events: {:?}", offload_events);

        assert_eq!(offload_events.len(), 3);
        assert_eq!(offload_events[0], sorted_providers[0]); // Task 1 -> First provider alphabetically
        assert_eq!(offload_events[1], sorted_providers[1]); // Task 2 -> Second provider alphabetically  
        assert_eq!(offload_events[2], sorted_providers[2]); // Task 3 -> Third provider alphabetically

        // Now all providers are busy, next poll should return timeout
        assert!(matches!(scheduler.poll_output(), Output::Timeout(_)));

        // Complete task on the first provider alphabetically to free it up
        use crate::task::{Status, TaskResult};
        let completed_task_result = TaskResult {
            id: TaskId::Consumer(1),
            executable: executable.clone(),
            args: vec!["arg1".to_string()],
            status: Status::Finished {
                exit_code: 0,
                stdout: vec![],
                stderr: vec![],
                output_file: None,
            },
            metrics: TaskMetrics::default(),
        };
        let returned_result =
            scheduler.handle_taskresult(sorted_providers[0], completed_task_result);
        assert!(returned_result.is_some());

        // Now we should get the 4th offload event for the pending task
        match scheduler.poll_output() {
            Output::Offload(provider_id, _) => {
                assert_eq!(provider_id, sorted_providers[0]); // Task 4 -> First provider alphabetically (now available)
            }
            other => panic!("Expected Offload event, got {:?}", other),
        }

        // Test disconnected provider behavior
        scheduler.handle_provider_state(providers[1], ProviderState::Disconnected, Instant::now());

        let task5 = Task {
            id: TaskId::Consumer(5),
            executable: executable.clone(),
            args: vec!["arg5".to_string()],
            deadline: Some(SystemTime::now() + std::time::Duration::from_secs(60)),
            metrics: TaskMetrics::default(),
        };
        scheduler.schedule(task5);
    }
}
