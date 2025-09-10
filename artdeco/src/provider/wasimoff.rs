use std::{
    collections::{HashMap, HashSet, VecDeque},
    mem,
};

use nid::Nanoid;
use protobuf::{Message, MessageField, MessageFull, well_known_types::any::Any};
use tracing::debug;

use crate::protobuf_gen::wasimoff::envelope::MessageType;
use crate::protobuf_gen::wasimoff::task::wasip1::{self};
use crate::protobuf_gen::wasimoff::task::{self};
use crate::{protobuf_gen::wasimoff, task::Task};
use crate::{
    protobuf_gen::wasimoff::Envelope,
    provider::{TaskMetrics, TaskStatus},
};

#[derive(PartialEq, Eq, Hash)]
pub struct WasimoffConfig {
    pub client_identifier: Nanoid,
}

pub struct WasimoffProvider {
    config: WasimoffConfig,
    state: State,
    pending_transmits: VecDeque<Envelope>,
    next_sequence: u64,
    next_task_id: u64,
    task_status: HashMap<u64, TaskStatus>,
    sequence_task_map: HashMap<u64, u64>,
    remote_files: HashSet<String>,
}

pub enum State {
    Invalid,
    Standby,
}

impl Default for State {
    fn default() -> Self {
        State::Invalid
    }
}

pub struct Transmit(pub Vec<u8>);

impl WasimoffProvider {
    pub fn new(config: WasimoffConfig) -> Self {
        Self {
            config,
            state: State::Standby,
            pending_transmits: VecDeque::new(),
            next_sequence: 1,
            next_task_id: 1,
            task_status: HashMap::new(),
            sequence_task_map: HashMap::new(),
            remote_files: HashSet::new(),
        }
    }

    pub fn handle_input(&mut self, buffer: &[u8]) {
        let state = mem::take(&mut self.state);
        let envelope = Envelope::parse_from_bytes(buffer).unwrap();
        let sequence_number = envelope.sequence();
        self.state = match state {
            State::Invalid => panic!("invalid wasimoff state"),
            State::Standby => {
                let pending_task = *self
                    .sequence_task_map
                    .get(&sequence_number)
                    .expect("expected matching pending task for sequence number");
                debug!("Received envelope payload {:?}", envelope.payload);
                let task_response: wasip1::Response = envelope.payload.unpack().unwrap().unwrap();
                let result = task_response.result.unwrap();
                match result {
                    wasip1::response::Result::Error(message) => {
                        self.task_status
                            .insert(pending_task, TaskStatus::ExecutionError(message));
                    }
                    wasip1::response::Result::Ok(output) => {
                        debug!("Received ok response {:?}", output);
                        let wasip1::Output {
                            status,
                            stdout,
                            stderr,
                            artifacts,
                            special_fields: _,
                        } = output;
                        self.task_status.insert(
                            pending_task,
                            TaskStatus::Finished {
                                metrics: TaskMetrics::default(),
                                exit_code: status.unwrap(),
                                stdout: stdout.unwrap(),
                                stderr: stderr.unwrap(),
                                artifacts: artifacts.blob().to_vec(),
                            },
                        );
                    }
                    wasip1::response::Result::Qoserror(message) => {
                        self.task_status
                            .insert(pending_task, TaskStatus::QoSError(message));
                    }
                }
                State::Standby
            }
        }
    }

    pub fn poll_output(&mut self) -> Option<Transmit> {
        match &mut self.state {
            State::Invalid => panic!("invalid wasimoff state"),
            State::Standby => {
                if !self.pending_transmits.is_empty() {
                    return Some(Transmit(
                        self.pending_transmits
                            .pop_front()
                            .unwrap()
                            .write_to_bytes()
                            .unwrap(),
                    ));
                } else {
                    return None;
                }
            }
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

    pub fn offload(&mut self, task: Task) -> u64 {
        let State::Standby = self.state else {
            panic!("invalid state");
        };
        debug!("offloading in provider");
        let Task {
            executable,
            args,
            submit_instant: instant,
        } = task;
        let mut offload_message = wasip1::Request::new();

        let mut metadata = task::Metadata::new();
        let task_id = self.next_task_id;
        self.sequence_task_map.insert(self.next_sequence, task_id);
        metadata.set_id(task_id.to_string());
        self.next_task_id += 1;
        offload_message.info = MessageField::some(metadata);

        let mut file = wasimoff::File::new();
        let file_ref = executable.file_ref.clone();
        // check if task file exists on remote
        if !self.remote_files.contains(&file_ref) {
            // attach file as blob
            file.set_blob(executable.content);
            // mark file as cached
            self.remote_files.insert(file_ref.clone());
        }
        file.set_ref(file_ref);

        let mut params = task::wasip1::Params::new();
        params.binary = MessageField::some(file);
        params.args = args;
        offload_message.params = MessageField::some(params);

        let envelope = self.pack(&offload_message);
        self.pending_transmits.push_back(envelope);

        self.task_status.insert(task_id, TaskStatus::Running);

        task_id
    }
}

impl PartialEq for WasimoffProvider {
    fn eq(&self, other: &Self) -> bool {
        self.config == other.config
    }
}

impl Eq for WasimoffProvider {}

impl std::hash::Hash for WasimoffProvider {
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
