use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::{Duration, Instant},
};

use nid::Nanoid;
use protobuf::{Message, MessageField, MessageFull, well_known_types::any::Any};
use tracing::{debug, error};

use crate::{protobuf_gen::wasimoff, task::Task};
use crate::{protobuf_gen::wasimoff::Envelope, provider::TaskMetrics};
use crate::{protobuf_gen::wasimoff::envelope::MessageType, task::TaskResult};
use crate::{
    protobuf_gen::wasimoff::task::wasip1::{self},
    task::Status,
};
use crate::{
    protobuf_gen::wasimoff::task::{self, QoS},
    task::TaskId,
};

#[derive(PartialEq, Eq, Hash)]
pub struct WasimoffConfig {
    pub client_identifier: Nanoid,
}

pub struct WasimoffProvider<M> {
    config: WasimoffConfig,
    pending_outputs: VecDeque<Output<M>>,
    next_sequence: u64,
    active_offloads: HashMap<u64, ActiveTask<M>>,
    remote_files: HashSet<String>,
    last_instant: Instant,
}

pub enum Output<M> {
    Transmit(Vec<u8>),
    TaskResult(TaskResult<M>),
    Timeout(Instant),
}

struct ActiveTask<M> {
    metrics: TaskMetrics<M>,
    id: TaskId,
}

impl<M> WasimoffProvider<M> {
    pub fn new(config: WasimoffConfig) -> Self {
        Self {
            config,
            pending_outputs: VecDeque::new(),
            next_sequence: 1,
            active_offloads: HashMap::new(),
            remote_files: HashSet::new(),
            last_instant: Instant::now(),
        }
    }

    pub fn handle_timeout(&mut self, timeout: Instant) {
        self.last_instant = timeout;
    }

    pub fn handle_input(&mut self, buffer: &[u8], instant: Instant) {
        self.handle_timeout(instant);
        let envelope = Envelope::parse_from_bytes(buffer).unwrap();
        let sequence_number = envelope.sequence();

        let pending_task = self.active_offloads.remove(&sequence_number);
        if pending_task.is_none() {
            error!(
                "expected matching pending task for sequence number {}",
                sequence_number
            );
            return;
        }
        let pending_task = pending_task.unwrap();

        // TODO set metrics

        debug!("Received envelope payload {:?}", envelope.payload);
        let task_response: wasip1::Response = envelope.payload.unpack().unwrap().unwrap();
        let result = task_response.result.unwrap();

        match result {
            wasip1::response::Result::Error(message) => {
                let status = if message.starts_with("qos") {
                    Status::QoSError(message)
                } else {
                    Status::Error(message)
                };
                let error_output = Output::TaskResult(TaskResult {
                    id: pending_task.id,
                    status,
                    metrics: pending_task.metrics,
                });
                self.pending_outputs.push_back(error_output);
            }
            wasip1::response::Result::Ok(output) => {
                debug!("Received ok response {:?}", output);
                let wasip1::Output {
                    status,
                    stdout,
                    stderr,
                    mut artifacts,
                    special_fields: _,
                } = output;
                let ok_output = Output::TaskResult(TaskResult {
                    id: pending_task.id,
                    status: Status::Finished {
                        exit_code: status.unwrap(),
                        stdout: stdout.unwrap(),
                        stderr: stderr.unwrap(),
                        output_file: artifacts.take().map(|mut file| file.take_blob()),
                    },
                    metrics: pending_task.metrics,
                });
                self.pending_outputs.push_back(ok_output);
            }
        }
    }

    pub fn poll_output(&mut self) -> Output<M> {
        if !self.pending_outputs.is_empty() {
            self.pending_outputs.pop_front().unwrap()
        } else {
            Output::Timeout(self.last_instant + Duration::from_secs(10))
        }
    }

    fn pack(&mut self, message: &impl MessageFull) -> Envelope {
        let mut envelope = Envelope::new();
        envelope.set_type(MessageType::Request);
        envelope.payload = MessageField::some(Any::pack(message).unwrap());
        envelope.sequence = Some(self.next_sequence);
        self.next_sequence += 1;
        envelope
    }

    pub fn offload(&mut self, task: Task<M>) {
        let Task {
            executable,
            args,
            id,
            metrics,
            deadline,
        } = task;
        let mut offload_message = wasip1::Request::new();

        let mut metadata = task::Metadata::new();
        // task id
        let task_id = match id {
            crate::task::TaskId::Consumer(id_usize) => {
                format!("{}:c:{}", self.config.client_identifier, id_usize)
            }
            crate::task::TaskId::Scheduler(id_usize) => {
                format!("{}:s:{}", self.config.client_identifier, id_usize)
            }
        };
        metadata.set_id(task_id);
        offload_message.info = MessageField::some(metadata);

        // qos
        let mut qos = QoS::new();
        qos.set_immediate(true);
        offload_message.qos = MessageField::some(qos);

        let mut file = wasimoff::File::new();
        let file_ref = executable.hash_ref().clone();
        // check if task file has been uploaded before
        if !self.remote_files.contains(&file_ref) {
            // attach file as blob
            file.set_blob(executable.content().to_vec());
            // mark file as cached
            self.remote_files.insert(file_ref.clone());
        }
        file.set_ref(file_ref);

        let mut params = task::wasip1::Params::new();
        params.binary = MessageField::some(file);
        params.args = args;
        offload_message.params = MessageField::some(params);

        let envelope = self.pack(&offload_message);
        self.active_offloads
            .insert(envelope.sequence(), ActiveTask { metrics, id });
        let bytes = envelope.write_to_bytes().unwrap();
        self.pending_outputs.push_back(Output::Transmit(bytes));
    }
}

impl<D> PartialEq for WasimoffProvider<D> {
    fn eq(&self, other: &Self) -> bool {
        self.config == other.config
    }
}

impl<D> Eq for WasimoffProvider<D> {}

impl<D> std::hash::Hash for WasimoffProvider<D> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.config.hash(state);
    }
}

#[cfg(test)]
mod test {
    use protobuf::Message;

    use crate::protobuf_gen::wasimoff::Envelope;

    #[test]
    fn test_upload_msg() {
        let serialized_upload: [u8; 80] = [
            8, 1, 34, 76, 10, 57, 116, 121, 112, 101, 46, 103, 111, 111, 103, 108, 101, 97, 112,
            105, 115, 46, 99, 111, 109, 47, 119, 97, 115, 105, 109, 111, 102, 102, 46, 118, 49, 46,
            70, 105, 108, 101, 115, 121, 115, 116, 101, 109, 46, 85, 112, 108, 111, 97, 100, 46,
            82, 101, 113, 117, 101, 115, 116, 18, 15, 10, 13, 10, 9, 116, 101, 115, 116, 46, 119,
            97, 115, 109, 26, 0,
        ];
        let envelope = Envelope::parse_from_bytes(&serialized_upload).unwrap();
        assert_eq!(envelope.sequence(), 1);
    }

    #[test]
    fn to_utf8() {
        let stderr: Vec<u8> = vec![
            102, 105, 98, 111, 110, 97, 99, 99, 105, 32, 114, 97, 110, 107, 32, 117, 51, 50, 32,
            114, 101, 113, 117, 105, 114, 101, 100, 32, 105, 110, 32, 102, 105, 114, 115, 116, 32,
            97, 114, 103, 117, 109, 101, 110, 116, 33, 10,
        ];
        let stderr_string = String::from_utf8(stderr).unwrap();
        println!("{}", stderr_string);
        let stdout: Vec<u8> = vec![50, 49, 55, 56, 51, 48, 57, 10];
        let stdout_string = String::from_utf8(stdout).unwrap();
        println!("{}", stdout_string);
    }
}
