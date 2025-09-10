use std::time::Instant;

#[derive(Debug, Clone)]
pub struct TaskExecutable {
    pub file_ref: String,
    pub content: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Task {
    pub executable: TaskExecutable,
    pub args: Vec<String>,
    pub submit_instant: Instant,
}
