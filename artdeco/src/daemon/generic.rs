use std::{
    fmt::{Debug, Display},
    net::{IpAddr, Ipv6Addr, SocketAddr, ToSocketAddrs},
    time::Instant,
};

use futures::{Sink, SinkExt, Stream, StreamExt};
use nid::Nanoid;
use slab::Slab;
use systemstat::{Platform, System};
use tokio::{net::UdpSocket, time::sleep_until};
use tracing::{error, info, trace};

use crate::{
    connection::rtc_connection::RTCConnectionConfig,
    consumer::{self, Consumer, Input, ProviderAnnounce, UdpReceive},
    scheduler::Scheduler,
    task::{AssociatedData, Workload, WorkloadResult},
};

// copied from str0m chat.rs
pub fn local_candidates(port: u16) -> Vec<SocketAddr> {
    let system = System::new();
    let networks = system.networks().unwrap();
    let mut addr = Vec::new();

    for net in networks.values() {
        for n in &net.addrs {
            if let systemstat::IpAddr::V4(v) = n.addr
                && !v.is_loopback()
                && !v.is_link_local()
                && !v.is_broadcast()
            {
                addr.push((IpAddr::V4(v), port).into());
            }

            if let systemstat::IpAddr::V6(v) = n.addr
                && !v.is_loopback()
            {
                addr.push((IpAddr::V6(v), port).into());
            }
        }
    }

    if addr.is_empty() {
        panic!("Found no usable network interface");
    }
    addr
}

pub async fn daemon<
    D,
    M: Debug + Default,
    S: Sink<WorkloadResult<D, M>, Error = impl Display> + Unpin,
>(
    task_queue: impl Stream<Item = Workload<S, D, M>> + Unpin,
    provider_stream: impl Stream<Item = String> + Unpin,
    sdp_stream: impl Stream<Item = String> + Unpin,
    mut sdp_sink: impl Sink<String, Error = impl Display> + Unpin,
    scheduler: impl Scheduler<M>,
) -> anyhow::Result<()> {
    let bind_addr = Ipv6Addr::UNSPECIFIED;
    let udp_socket = UdpSocket::bind(format!("{bind_addr}:0")).await?;

    let local_port = udp_socket.local_addr().unwrap().port();
    let host_addr = local_candidates(local_port);
    let local_addr = *host_addr.first().unwrap();
    info!("local address is {}", local_addr);

    let rtc_config = RTCConnectionConfig {
        local_host_addr: host_addr,
        data_channel_name: "wasimoff".to_owned(),
        local_uuid: Nanoid::new(),
        start: Instant::now(),
    };
    let google_stun_addr = "74.125.250.129:19302"
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();
    let mut offloader = Consumer::new(rtc_config, scheduler, google_stun_addr);
    let mut udp_buffer = vec![0; 2000];
    let mut active_tasks: Slab<AssociatedData<S, D>> = Slab::new();

    let mut fused_task_queue = task_queue.fuse();
    let mut fused_sdp_stream = sdp_stream.fuse();
    let mut fused_provider_stream = provider_stream.fuse();
    let sleep = sleep_until(Instant::now().into());
    tokio::pin!(sleep);

    loop {
        let next_timeout = match offloader.poll_output() {
            consumer::Output::Timeout(instant) => {
                trace!("offloader timeout {:?}", instant);
                instant
            }
            consumer::Output::SocketTransmit(transmit) => {
                trace!("offloader socket transmit {:?}", transmit);
                if let Err(error) = udp_socket
                    .send_to(&transmit.contents, transmit.destination)
                    .await
                {
                    error!(
                        "error during UDP socket send, {}, packet destination: {}",
                        error, transmit.destination
                    );
                }
                continue;
            }
            consumer::Output::SdpTransmit(sdp_transmit) => {
                trace!("offloader sdp transmit {:?}", sdp_transmit);
                if let Err(error) = sdp_sink.send(sdp_transmit).await {
                    error!("error during send of sdp transmit, {}", error);
                }
                continue;
            }
            consumer::Output::TaskResult(result) => {
                trace!("received result: {:?}", result);
                let (mut channel, workload_result) = result.into_workload_result(&mut active_tasks);
                if let Err(err) = channel.send(workload_result).await {
                    error!("could not respond with workload result, {}", err);
                }
                continue;
            }
        };

        sleep.as_mut().reset(next_timeout.into());

        tokio::select! {
            // poll task queue
            Some(next_task) = fused_task_queue.next() => {
                trace!("task queue new task");
                // offload task using offloader
                // deconstruct Workload, insert channel into slab, create new task
                let task = next_task.into_task(&mut active_tasks);
                offloader.handle_task(task);
                trace!("task future completes");
            }
            // poll announce stream for new providers
            Some(announce_str) = fused_provider_stream.next() => {
                let announce_str: String = announce_str;
                trace!("announce stream event {:?}", announce_str);
                match serde_json::from_str(&announce_str) {
                    Ok(announce_msg_parsed) => {
                        let pa = ProviderAnnounce {
                            announce: announce_msg_parsed,
                            last: Instant::now(),
                        };
                        offloader.handle_announce(pa);
                    },
                    Err(error) => {
                        error!("error during announce message parsing, {}", error);
                    }
                }
                trace!("announce future completes");
            }
            // poll sdp rendezvouz for new messages
            Some(sdp_message_str) = fused_sdp_stream.next() => {
                let sdp_message_str: String = sdp_message_str;
                trace!("sdp stream event {:?}", sdp_message_str);
                match serde_json::from_str(&sdp_message_str) {
                    Ok(sdp_msg_parsed) => {
                        offloader.handle_sdp(sdp_msg_parsed);
                    },
                    Err(error) => {
                        error!("error during sdp message parsing, {}", error);
                    }
                }
                trace!("announce sdp completes");
            }
            // poll udp socket
            udp_result = udp_socket.recv_from(&mut udp_buffer) => {
                match udp_result {
                    Ok((n, source)) => {
                        let (subslice, _rest) = udp_buffer.as_slice().split_at(n);
                        trace!("udp socket recv {} from {} with {:?}", n, source, subslice);
                        if n > 0 {
                            match subslice.try_into() {
                                Ok(datagram) => {
                                    let input = Input::SocketReceive(Instant::now(), Box::new(UdpReceive {
                                        source,
                                        destination: local_addr,
                                        contents: datagram,
                                    }));
                                    offloader.handle_input(input);
                                },
                                Err(error) => {
                                    error!("error during udp buffer to datagram transform, {}", error);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                }
                trace!("announce udp completes");
            }
            // poll timeout
            _ = &mut sleep => {
                if fused_task_queue.is_done() && active_tasks.is_empty(){
                    return Ok(())
                }
                trace!("next timeout for offloader");
            }
        }
        trace!("current time {:?}", Instant::now());
        offloader.handle_input(Input::Timeout(Instant::now()));
    }
}
