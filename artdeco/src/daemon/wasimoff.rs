use std::{
    collections::HashMap,
    error::Error,
    fmt::Display,
    pin::pin,
    time::{Duration, SystemTime},
};

use anyhow::Result;
use async_nats::ToServerAddrs;
use bytes::Bytes;
use futures::{Sink, Stream, StreamExt};
use protobuf::{Message, MessageField, well_known_types::any::Any};
use tracing::{error, info, warn};

use crate::util::CustomSinkExt;
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
    task::{Status, TaskExecutable, WorkloadResult},
};

struct CustomData {
    task_id: Option<String>,
    reference: Option<String>,
    trace: Option<Trace>,
    sequence_number: u64,
}

pub async fn wasimoff_broker(
    datagram_stream: impl Stream<Item = Bytes> + Unpin,
    datagram_sink: impl Sink<Bytes, Error = impl Send + Sync + Error> + Unpin + Clone + 'static,
    scheduler: impl Scheduler + Send + 'static,
    nats_url: impl ToServerAddrs + Send + 'static,
    executables: &HashMap<String, TaskExecutable>,
) -> Result<()> {
    info!("Starting wasimoff broker");

    let response_sender =
        datagram_sink.map(|task_result: WorkloadResult<CustomData>| write_to_socket(task_result));
    let task_stream = pin![datagram_stream.filter_map(async |bytes| {
        read_from_socket(bytes, response_sender.clone(), executables)
    })];

    daemon_nats(task_stream, scheduler, nats_url).await?;

    info!("Wasimoff broker shutting down");
    Ok(())
}

fn read_from_socket(
    buffer: Bytes,
    back_channel: impl Sink<WorkloadResult<CustomData>, Error = impl Display> + 'static + Clone + Unpin,
    task_executables: &HashMap<String, TaskExecutable>,
) -> Option<
    crate::task::Workload<
        impl Sink<WorkloadResult<CustomData>, Error = impl Display> + 'static + Unpin,
        CustomData,
    >,
> {
    match Envelope::parse_from_bytes(&buffer) {
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
                    let workload = crate::task::Workload {
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
                    };

                    return Some(workload);
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
    None
}

fn write_to_socket(task_result: WorkloadResult<CustomData>) -> Bytes {
    let WorkloadResult {
        status,
        metrics,
        mut custom_data,
    } = task_result;
    let mut response = Wasip1Response::new();

    // set metadata
    let mut info: Metadata = Metadata::new();
    info.id = custom_data.task_id.or(Some("abrakadabra".into()));
    info.reference = custom_data.reference;
    info.provider = metrics
        .executor_id
        .or(Some("artdeco-wasimoff-adaptor".into()));
    assert!(info.id.is_some());
    assert!(info.provider.is_some());

    let mut trace = custom_data.trace.take();
    if let Some(trace) = &mut trace {
        metrics
            .wasimoff_trace
            .into_iter()
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

    envelope.write_to_bytes().unwrap().into()
}
