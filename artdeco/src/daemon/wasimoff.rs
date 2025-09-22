use std::{
    collections::HashMap,
    marker::PhantomData,
    time::{Duration, SystemTime},
};

use anyhow::Result;
use async_nats::ToServerAddrs;
use futures::{
    SinkExt, StreamExt,
    channel::mpsc::{self, Sender},
};
use protobuf::{Message, MessageField, well_known_types::any::Any};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{error, info, warn};

use crate::{
    daemon::nats::daemon_nats,
    protocol::wasimoff::{
        Envelope,
        envelope::MessageType,
        task::{
            self, Metadata, Trace,
            wasip1::{self, Response as Wasip1Response, response},
        },
    },
    scheduler::Scheduler,
    task::{Status, TaskExecutable, WasimoffTraceEvent, Workload, WorkloadResult},
};

use std::fmt::Debug;

struct CustomData {
    task_id: Option<String>,
    reference: Option<String>,
    trace: Option<Trace>,
    sequence_number: u64,
}

pub async fn wasimoff_broker<M: WasimoffTraceEvent + Debug + Send + Default + 'static>(
    mut socket: impl AsyncRead + AsyncWrite + Unpin,
    scheduler: impl Scheduler<M> + Send + 'static,
    nats_url: impl ToServerAddrs + Send + 'static,
    executables: &HashMap<&str, TaskExecutable>,
) -> Result<()> {
    info!("Starting wasimoff broker");

    // Create channels for task queue and responses
    let (mut task_sender, task_receiver) =
        mpsc::channel::<Workload<Sender<WorkloadResult<CustomData, M>>, CustomData, M>>(100);
    let (response_sender, response_receiver) = mpsc::channel::<WorkloadResult<CustomData, M>>(100);

    tokio::spawn(daemon_nats(task_receiver, scheduler, nats_url));
    let mut fused_task_responses = response_receiver.fuse();

    // Buffer for reading from socket
    let mut recv_buffer = vec![0u8; 4096];

    tokio::select! {
        read_result = socket.read(&mut recv_buffer) => {
            match read_result {
                Ok(num_bytes) => {
                    if num_bytes > 0 {
                        let (subslice, _rest) = recv_buffer.as_slice().split_at(num_bytes);
                        read_from_socket(subslice, &mut task_sender, response_sender.clone(), executables).await;
                    }
                },
                Err(_) => todo!(),
            }
        },
        task_response = fused_task_responses.next() => {
            if let Some(next_response) = task_response {
                write_to_socket(&mut socket, next_response).await;
            }
        }
    }

    info!("Wasimoff broker shutting down");
    Ok(())
}

async fn read_from_socket<M: WasimoffTraceEvent>(
    buffer: &[u8],
    task_queue: &mut Sender<Workload<Sender<WorkloadResult<CustomData, M>>, CustomData, M>>,
    back_channel: Sender<WorkloadResult<CustomData, M>>,
    task_executables: &HashMap<&str, TaskExecutable>,
) {
    match Envelope::parse_from_bytes(buffer) {
        Ok(envelope) => match envelope.payload.unpack::<wasip1::Request>() {
            Ok(request_opt) => {
                let request = request_opt.unwrap();
                let metadata = request.info;
                let params = request.params;
                let qos = request.qos;
                let file_ref = params.binary.ref_();
                let args = params.args.clone();

                let deadline = qos.deadline.as_ref().cloned().map(|timestamp| {
                    SystemTime::UNIX_EPOCH + Duration::from_nanos(timestamp.nanos as u64)
                });
                let Metadata {
                    id: task_id,
                    requester: _,
                    provider: _,
                    reference,
                    trace,
                    special_fields: _,
                } = metadata.unwrap();
                let trace = trace.into_option();

                if let Some(executable) = task_executables.get(file_ref) {
                    let workload = Workload {
                        executable: executable.clone(),
                        response_channel: back_channel.clone(),
                        args,
                        deadline,
                        custom_data: CustomData {
                            task_id,
                            reference,
                            trace,
                            sequence_number: envelope.sequence(),
                        },
                        metrics_type: PhantomData {},
                    };

                    if let Err(e) = task_queue.send(workload).await {
                        error!("Failed to send workload to task queue: {}", e);
                    }
                } else {
                    warn!("ignoring task with unknown binary");
                }
            }
            Err(err) => error!(
                "cannot parse envelope content as request, ignoring, {}",
                err
            ),
        },
        Err(e) => {
            error!("Failed to parse envelope: {}", e);
        }
    }
}

async fn write_to_socket<M: WasimoffTraceEvent>(
    mut socket: impl AsyncWrite + Unpin,
    task_result: WorkloadResult<CustomData, M>,
) {
    let WorkloadResult {
        status,
        metrics,
        mut custom_data,
    } = task_result;
    let mut response = Wasip1Response::new();

    // set metadata
    let mut info = Metadata::new();
    info.id = custom_data.task_id;
    info.reference = custom_data.reference;
    if let Some(executor_id) = metrics.executor_id {
        info.set_provider(executor_id.to_string());
    }

    let mut trace = custom_data.trace.take();
    if let Some(trace) = &mut trace {
        metrics
            .trace
            .iter()
            .filter_map(WasimoffTraceEvent::to_wasimoff)
            .for_each(|event: task::TraceEvent| {
                trace.events.push(event);
            });
    }
    info.trace = trace.into();

    response.info = Some(info).into();

    // set result
    let result = match status {
        Status::Error(msg) | Status::QoSError(msg) => response::Result::Error(msg),
        Status::Finished {
            exit_code,
            stdout,
            stderr,
            output_file,
        } => {
            let mut output = task::wasip1::Output::new();
            output.set_status(exit_code);
            output.set_stdout(stdout);
            output.set_stderr(stderr);
            if output_file.is_some() {
                error!("file artifacts not implemented yet");
            }
            response::Result::Ok(output)
        }
    };
    response.result = Some(result);

    let mut envelope = Envelope::new();
    envelope.set_type(MessageType::Response);
    envelope.payload = MessageField::some(Any::pack(&response).unwrap());
    envelope.sequence = Some(custom_data.sequence_number);

    match envelope.write_to_bytes() {
        Ok(serialized) => {
            if let Err(e) = socket.write_all(&serialized).await {
                error!("Failed to write response to socket: {}", e);
            }
        }
        Err(e) => {
            error!("Failed to serialize envelope: {}", e);
        }
    }
}
