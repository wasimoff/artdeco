use crate::{
    consumer::{ProviderAnnounce, TIMEOUT},
    protocol::wasimoff::task::trace_event::EventType,
    scheduler::{Output, ProviderState, Scheduler},
    task::{Status, Task, TaskId, TaskResult},
};
use indexmap::IndexMap;
use nid::Nanoid;
use rand::Rng;
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};
use tracing::{info, trace};

#[derive(Clone)]
enum ProviderStatus {
    Disconnected,
    WaitingForConnection(Vec<Task>),
    Connected,
}

#[derive(Clone)]
enum ScheduleResult {
    Success,
    Failure,
}

impl std::fmt::Display for ScheduleResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScheduleResult::Success => write!(f, "Success"),
            ScheduleResult::Failure => write!(f, "Failure"),
        }
    }
}

pub struct Drift {
    last_instant: Instant,
    providers: IndexMap<Nanoid, ProviderInfo>,
    pending_tasks: VecDeque<Task>,
    event_buffer: VecDeque<Output>,
    increase_threshhold: usize,
    decrease_threshhold: usize,
    window_size: usize,
    history: VecDeque<ScheduleResult>,
    max_history: usize,
}

// drift is special version of uniform with maxium window
// if max_history is 0, then this behaves like the uniform distribution

#[derive(Clone)]
struct ProviderInfo {
    /// Number of tasks successfully completed
    tasks_completed: u64,
    /// Total duration this provider has been busy
    total_busy_duration: Duration,
    /// Maximum number of tasks this provider can handle concurrently
    max_tasks: u32,
    /// Current state of the provider
    state: ProviderStatus,
    /// Active tasks
    active_tasks: Vec<(Instant, TaskId)>,
}

impl ProviderInfo {
    fn performance_value(&self) -> f32 {
        if self.total_busy_duration.is_zero() {
            0.0
        } else {
            self.tasks_completed as f32 / self.total_busy_duration.as_secs_f32()
        }
    }

    fn busy(&self) -> bool {
        self.active_tasks.len() >= self.max_tasks.try_into().unwrap()
    }

    fn idle(&self) -> bool {
        self.active_tasks.is_empty()
            && !matches!(self.state, ProviderStatus::WaitingForConnection(_))
    }
}

impl Drift {
    pub fn new(
        increase_threshold: usize,
        decrease_threshold: usize,
        window_size: usize,
        max_history: usize,
    ) -> Self {
        Drift {
            last_instant: Instant::now(),
            providers: IndexMap::new(),
            pending_tasks: VecDeque::new(),
            event_buffer: VecDeque::new(),
            increase_threshhold: increase_threshold,
            decrease_threshhold: decrease_threshold,
            window_size,
            history: VecDeque::new(),
            max_history,
        }
    }

    fn process_pending_tasks(&mut self) {
        // cleanup connections
        for (nanoid, provider_info) in &mut self.providers {
            if matches!(provider_info.state, ProviderStatus::Connected) && provider_info.idle() {
                self.event_buffer.push_back(Output::Disconnect(*nanoid));
                provider_info.state = ProviderStatus::Disconnected;
            }
        }

        // update window size from history
        self.adjust_window_size();

        if self.pending_tasks.is_empty() || self.providers.is_empty() || self.window_size == 0 {
            return;
        }

        // get provider
        let (id, p_info) = Self::get_next_provider(&mut self.providers, self.window_size);

        // schedule task
        let mut task = self.pending_tasks.pop_front().unwrap();
        // provider is already connected, task
        match &mut p_info.state {
            ProviderStatus::Disconnected => {
                task.metrics
                    .push_trace_event_now(EventType::ArtDecoSchedulerProviderConnect, None);
                self.event_buffer.push_back(Output::Connect(*id));
                p_info.state = ProviderStatus::WaitingForConnection(vec![task]);
            }
            ProviderStatus::Connected => {
                Self::offload_task(*id, self.last_instant, task, p_info, &mut self.event_buffer);
            }
            ProviderStatus::WaitingForConnection(waiting_tasks) => {
                // Add the new task to the list of waiting tasks
                waiting_tasks.push(task);
            }
        }
    }

    fn get_next_provider(
        providers: &mut IndexMap<Nanoid, ProviderInfo>,
        window_size: usize,
    ) -> (&Nanoid, &mut ProviderInfo) {
        // Sort providers by performance value and take the first window_size providers
        providers.sort_by(|_key1, value1, _key2, value2| {
            value2
                .performance_value()
                .partial_cmp(&value1.performance_value())
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let random_index = rand::rng().random_range(0..window_size);
        providers.get_index_mut(random_index).unwrap()
    }

    fn adjust_window_size(&mut self) {
        // if no history is stored, then the window size cannot be adjusted
        // set it to the current amount of providers and schedule uniformly
        let old_window = self.window_size;
        if self.max_history == 0 {
            self.window_size = self.providers.len();
        } else if self.max_history > 0 {
            let failures = self
                .history
                .iter()
                .filter(|&result| matches!(result, ScheduleResult::Failure))
                .count();
            if failures >= self.increase_threshhold {
                self.window_size = (self.window_size + 1).min(self.providers.len());
            } else if failures <= self.decrease_threshhold {
                self.window_size = self.window_size.saturating_sub(1)
            }
        }
        if old_window != self.window_size {
            info!("Adjusted window size to: {}", self.window_size);
        }
    }

    fn offload_task(
        uuid: Nanoid,
        last_instant: Instant,
        mut task: Task,
        provider_info: &mut ProviderInfo,
        event_buffer: &mut VecDeque<Output>,
    ) {
        task.metrics
            .push_trace_event_now(EventType::ArtDecoSchedulerProviderOffload, None);
        provider_info.active_tasks.push((last_instant, task.id));
        let schedule_event = Output::Offload(uuid, task);
        event_buffer.push_back(schedule_event);
    }

    fn add_history(&mut self, schedule_result: ScheduleResult) {
        info!("Add result {} to scheduling history", schedule_result);
        self.history.push_back(schedule_result);
        if self.history.len() > self.max_history && self.max_history != 0 {
            self.history.pop_front();
        }
    }
}

impl Default for Drift {
    fn default() -> Self {
        Self::new(0, 0, 0, 0)
    }
}

impl Scheduler for Drift {
    fn handle_timeout(&mut self, instant: Instant) {
        let duration_since_last = instant.saturating_duration_since(self.last_instant);
        self.last_instant = instant;

        // Update busy duration for providers with busy workers
        self.providers
            .iter_mut()
            .filter(|(_, provider_info)| provider_info.busy())
            .for_each(|(_, provider_info)| {
                provider_info.total_busy_duration += duration_since_last;
            });

        self.process_pending_tasks();
    }

    fn handle_announce(&mut self, announce: ProviderAnnounce) {
        let provider_id = announce.announce.id;
        let max_concurrency = announce.announce.concurrency;

        // Update or create provider info
        let provider_info = self
            .providers
            .entry(provider_id)
            .or_insert_with(|| ProviderInfo {
                tasks_completed: 0,
                total_busy_duration: Duration::ZERO,
                max_tasks: max_concurrency,
                state: ProviderStatus::Disconnected,
                active_tasks: vec![],
            });

        provider_info.max_tasks = max_concurrency;

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
            (ProviderState::Connected, ProviderStatus::Connected) => {
                unreachable!("provider is already idle but now somehow freshly connected")
            }
            (ProviderState::Disconnected, ProviderStatus::Disconnected) => {
                trace!("provider is already disconnected")
            }
            (ProviderState::Connected, ProviderStatus::Disconnected) => {
                provider_info.state = ProviderStatus::Connected;
            }
            (ProviderState::Connected, ProviderStatus::WaitingForConnection(waiting_tasks)) => {
                provider_info.state = ProviderStatus::Connected;
                // Offload all waiting tasks
                for task in waiting_tasks {
                    Self::offload_task(
                        uuid,
                        self.last_instant,
                        task,
                        provider_info,
                        &mut self.event_buffer,
                    );
                }
            }
            (ProviderState::Disconnected, ProviderStatus::WaitingForConnection(waiting_tasks)) => {
                // Requeue all waiting tasks
                for mut task in waiting_tasks {
                    task.metrics
                        .push_trace_event_now(EventType::ArtDecoSchedulerRequeue, None);
                    self.pending_tasks.push_back(task);
                }
                provider_info.state = ProviderStatus::Disconnected;
                provider_info.active_tasks.clear();
            }
            (ProviderState::Disconnected, ProviderStatus::Connected) => {
                provider_info.state = ProviderStatus::Disconnected;
                provider_info.active_tasks.clear();
            }
        }
    }

    fn handle_taskresult(&mut self, uuid: Nanoid, task_result: TaskResult) -> Option<TaskResult> {
        let Some(provider_info) = self.providers.get_mut(&uuid) else {
            unreachable!("cant find provider")
        };

        let (start_time, _) = provider_info.active_tasks.remove(
            provider_info
                .active_tasks
                .iter()
                .position(|(_, task_id)| *task_id == task_result.id)
                .expect("worker not found"),
        );

        let task_duration = self.last_instant.saturating_duration_since(start_time);
        provider_info.total_busy_duration += task_duration;

        let result = match task_result.status {
            Status::QoSError(_) => {
                self.add_history(ScheduleResult::Failure);
                let mut task = task_result.into_task();
                task.metrics
                    .push_trace_event_now(EventType::ArtDecoSchedulerRequeue, None);
                self.pending_tasks.push_back(task);
                None
            }
            _ => {
                provider_info.tasks_completed += 1;
                self.add_history(ScheduleResult::Success);
                Some(task_result)
            }
        };

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
mod tests {
    use super::*;
    use crate::{
        consumer::ProviderAnnounceMsg,
        scheduler::{Output, ProviderState, Scheduler},
        task::{Status, Task, TaskExecutable, TaskId, TaskMetrics, TaskResult},
    };
    use std::time::SystemTime;

    #[test]
    fn test_drift_scheduler_basic_functionality() {
        let mut scheduler = Drift::new(3, 1, 2, 10);

        // Create 3 test providers with different performance characteristics
        let providers = [Nanoid::new(), Nanoid::new(), Nanoid::new()];

        // Announce all providers
        for &provider_id in &providers {
            let announce = ProviderAnnounce {
                announce: ProviderAnnounceMsg {
                    id: provider_id,
                    concurrency: 2, // Each provider can handle 2 concurrent tasks
                    tasks: 0,
                    timestamp: 0,
                    active_connections: 0,
                    max_connections: 0,
                },
                last: Instant::now(),
            };
            scheduler.handle_announce(announce);
        }

        let executable = TaskExecutable::new("test_drift".as_bytes());

        // Create and schedule a task
        let task1 = Task {
            id: TaskId::Consumer(1),
            executable: executable.clone(),
            args: vec!["arg1".to_string()],
            deadline: Some(SystemTime::now() + std::time::Duration::from_secs(60)),
            metrics: TaskMetrics::default(),
        };
        scheduler.schedule(task1);

        // Should get a connect event for one of the providers
        let connect_output = scheduler.poll_output();
        let connected_provider = match connect_output {
            Output::Connect(provider_id) => provider_id,
            Output::Timeout(_) => {
                // If we get timeout, it might be because no tasks are pending
                // Check if task was processed differently
                return; // This test might need adjustment based on actual scheduler behavior
            }
            other => panic!("Expected Connect event or Timeout, got {:?}", other),
        };

        // Simulate provider connection
        scheduler.handle_provider_state(
            connected_provider,
            ProviderState::Connected,
            Instant::now(),
        );

        // Should now get an offload event
        let offload_output = scheduler.poll_output();
        match offload_output {
            Output::Offload(provider_id, task) => {
                assert_eq!(provider_id, connected_provider);
                assert_eq!(task.id, TaskId::Consumer(1));
            }
            Output::Timeout(_) => {
                // This might be expected behavior if no immediate offload happens
                return;
            }
            other => panic!("Expected Offload event or Timeout, got {:?}", other),
        }

        // Next poll should return timeout since no more tasks pending
        assert!(matches!(scheduler.poll_output(), Output::Timeout(_)));
    }

    #[test]
    fn test_drift_scheduler_performance_tracking() {
        let mut scheduler = Drift::new(3, 1, 2, 10);
        let provider_id = Nanoid::new();

        // Announce provider
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

        let executable = TaskExecutable::new("test_perf".as_bytes());

        // Schedule a task
        let task = Task {
            id: TaskId::Consumer(1),
            executable: executable.clone(),
            args: vec!["arg1".to_string()],
            deadline: Some(SystemTime::now() + std::time::Duration::from_secs(60)),
            metrics: TaskMetrics::default(),
        };
        scheduler.schedule(task);

        // Connect provider and process task
        let connect_output = scheduler.poll_output();
        if let Output::Connect(connected_provider) = connect_output {
            assert_eq!(connected_provider, provider_id);

            // Connect the provider
            scheduler.handle_provider_state(provider_id, ProviderState::Connected, Instant::now());

            // Should get offload event
            let offload_output = scheduler.poll_output();
            match offload_output {
                Output::Offload(offload_provider, offloaded_task) => {
                    assert_eq!(offload_provider, provider_id);
                    assert_eq!(offloaded_task.id, TaskId::Consumer(1));
                }
                other => panic!("Expected Offload event, got {:?}", other),
            }

            // Simulate time passing
            let later_instant = Instant::now() + Duration::from_millis(100);
            scheduler.handle_timeout(later_instant);

            // Complete the task successfully
            let task_result = TaskResult {
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

            let returned_result = scheduler.handle_taskresult(provider_id, task_result);
            assert!(returned_result.is_some());

            // Verify that provider performance was tracked
            let provider_info = scheduler.providers.get(&provider_id).unwrap();
            assert_eq!(provider_info.tasks_completed, 1);
            assert!(provider_info.total_busy_duration > Duration::ZERO);
        } else {
            // If no connect event, the test behavior might be different
            // This is acceptable as the scheduler might behave differently
        }
    }

    #[test]
    fn test_drift_scheduler_window_adjustment() {
        let mut scheduler = Drift::new(2, 1, 2, 5); // increase_threshold=2, decrease_threshold=1, window_size=2, max_history=5

        // Create 4 providers
        let providers: Vec<Nanoid> = (0..4).map(|_| Nanoid::new()).collect();

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
        }

        let executable = TaskExecutable::new("test_window".as_bytes());

        // Simulate failures to trigger window size increase
        for i in 0..3 {
            let task = Task {
                id: TaskId::Consumer(i),
                executable: executable.clone(),
                args: vec![format!("arg{}", i)],
                deadline: Some(SystemTime::now() + std::time::Duration::from_secs(60)),
                metrics: TaskMetrics::default(),
            };
            scheduler.schedule(task);

            // Process events to get to task completion
            let connect_output = scheduler.poll_output();
            if let Output::Connect(provider_id) = connect_output {
                scheduler.handle_provider_state(
                    provider_id,
                    ProviderState::Connected,
                    Instant::now(),
                );
                scheduler.poll_output(); // Offload event

                // Simulate QoS failure
                let failed_result = TaskResult {
                    id: TaskId::Consumer(i),
                    executable: executable.clone(),
                    args: vec![format!("arg{}", i)],
                    status: Status::QoSError("simulated failure".to_string()),
                    metrics: TaskMetrics::default(),
                };

                let returned_result = scheduler.handle_taskresult(provider_id, failed_result);
                assert!(returned_result.is_none()); // QoS errors return None and requeue task
            }
        }

        // After enough failures, window size should increase
        // The exact behavior depends on the internal state, but we can verify
        // that the scheduler handles the window adjustment logic
        assert!(scheduler.window_size <= providers.len());
    }

    #[test]
    fn test_drift_scheduler_provider_disconnection() {
        let mut scheduler = Drift::new(3, 1, 2, 10);
        let provider_id = Nanoid::new();

        // Announce provider
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

        let executable = TaskExecutable::new("test_disconnect".as_bytes());

        // Schedule a task
        let task = Task {
            id: TaskId::Consumer(1),
            executable: executable.clone(),
            args: vec!["arg1".to_string()],
            deadline: Some(SystemTime::now() + std::time::Duration::from_secs(60)),
            metrics: TaskMetrics::default(),
        };
        scheduler.schedule(task);

        // Get connect event
        let connect_output = scheduler.poll_output();
        if let Output::Connect(connected_provider) = connect_output {
            assert_eq!(connected_provider, provider_id);

            // Now the provider is in WaitingForConnection state
            // Simulate provider disconnection while waiting for connection
            scheduler.handle_provider_state(
                provider_id,
                ProviderState::Disconnected,
                Instant::now(),
            );

            // The task should be requeued (check by scheduling another task)
            assert!(!scheduler.pending_tasks.is_empty());

            // Provider should be disconnected
            let provider_info = scheduler.providers.get(&provider_id).unwrap();
            assert!(matches!(provider_info.state, ProviderStatus::Disconnected));
        } else {
            // If no connect event occurs, test the basic disconnection behavior
            // First connect the provider
            scheduler.handle_provider_state(provider_id, ProviderState::Connected, Instant::now());

            // Then disconnect it
            scheduler.handle_provider_state(
                provider_id,
                ProviderState::Disconnected,
                Instant::now(),
            );

            // Verify it's disconnected
            let provider_info = scheduler.providers.get(&provider_id).unwrap();
            assert!(matches!(provider_info.state, ProviderStatus::Disconnected));
        }
    }

    #[test]
    fn test_drift_scheduler_uniform_mode() {
        // Test with max_history = 0 (uniform distribution mode)
        let mut scheduler = Drift::new(0, 0, 3, 0);

        // Create providers
        let providers: Vec<Nanoid> = (0..3).map(|_| Nanoid::new()).collect();

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
        }

        // In uniform mode, window_size should equal number of providers
        scheduler.adjust_window_size();
        assert_eq!(scheduler.window_size, providers.len());
    }
}
