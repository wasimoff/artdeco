use std::{fmt::Debug, fmt::Display, pin::pin};

use async_nats::{PublishMessage, Subject, ToServerAddrs};
use futures::{Sink, SinkExt, Stream, StreamExt};
use tracing::debug;

use crate::{daemon::generic::daemon, scheduler::Scheduler, task::Workload, task::WorkloadResult};

pub async fn daemon_nats<
    D,
    M: Debug + Default,
    S: Sink<WorkloadResult<D, M>, Error = impl Display> + Unpin,
>(
    task_queue: impl Stream<Item = Workload<S, D, M>> + Unpin,
    scheduler: impl Scheduler<M>,
    nats_url: impl ToServerAddrs,
) -> anyhow::Result<()> {
    let client = async_nats::connect(nats_url).await?;
    debug!("Connected to NATS server");

    let sdp_sub = client.subscribe("sdp").await?;
    let provider_sub = client.subscribe("providers").await?;
    let sdp_stream = sdp_sub.map(|message| String::from_utf8(message.payload.to_vec()).unwrap());
    let provider_stream =
        provider_sub.map(|message| String::from_utf8(message.payload.to_vec()).unwrap());
    let sdp_sink = pin!(
        client.with::<_, _, _, anyhow::Error>(|payload: String| async {
            Ok(PublishMessage {
                subject: Subject::from("sdp"),
                payload: payload.into(),
                reply: None,
                headers: None,
            })
        })
    );

    daemon(task_queue, provider_stream, sdp_stream, sdp_sink, scheduler).await
}
