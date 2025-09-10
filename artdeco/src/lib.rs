use std::{fmt::Display, net::IpAddr, time::Instant};

use futures::{Sink, SinkExt, Stream, StreamExt};
use nid::Nanoid;
use slab::Slab;
use str0m::net::{Protocol, Receive};
use systemstat::{Platform, System};
use tokio::{net::UdpSocket, time::sleep_until};
use tracing::{debug, error, info, trace};

use crate::{
    connection::rtc_connection::RTCConnectionConfig,
    offloader::{Input, Offloader, ProviderAnnounce},
    scheduler::Scheduler,
    task::{Task, TaskResult, Workload, WorkloadResult},
};

pub mod connection;
#[cfg(feature = "nats")]
pub mod nats;
pub mod offloader;
pub mod provider;
pub mod scheduler;
pub mod task;

mod protobuf_gen {
    include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));
}

// copied from str0m chat.rs
pub fn select_host_address() -> IpAddr {
    let system = System::new();
    let networks = system.networks().unwrap();

    for net in networks.values() {
        for n in &net.addrs {
            if let systemstat::IpAddr::V4(v) = n.addr {
                if !v.is_loopback() && !v.is_link_local() && !v.is_broadcast() {
                    return IpAddr::V4(v);
                }
            }
            if let systemstat::IpAddr::V6(v) = n.addr {
                if !v.is_loopback() {
                    return IpAddr::V6(v);
                }
            }
        }
    }

    panic!("Found no usable network interface");
}

pub async fn daemon<S: Sink<WorkloadResult> + Unpin>(
    task_queue: impl Stream<Item = Workload<S>> + Unpin,
    mut provider_stream: impl Stream<Item = String> + Unpin,
    mut sdp_stream: impl Stream<Item = String> + Unpin,
    mut sdp_sink: impl Sink<String, Error = impl Display> + Unpin,
    scheduler: impl Scheduler,
) -> anyhow::Result<()> {
    let host_addr = select_host_address();
    let udp_socket = UdpSocket::bind(format!("{host_addr}:0")).await?;
    let local_address = udp_socket.local_addr().unwrap();
    info!("local address is {}", local_address);

    let rtc_config = RTCConnectionConfig {
        local_host_addr: vec![local_address],
        data_channel_name: "wasimoff".to_owned(),
        local_uuid: Nanoid::new(),
        start: Instant::now(),
    };
    let mut offloader = Offloader::new(rtc_config, scheduler);
    let mut udp_buffer = vec![0; 2000];
    let mut active_tasks = Slab::new();

    let mut fused_task_queue = task_queue.fuse();

    loop {
        let next_timeout = match offloader.poll_output() {
            offloader::Output::Timeout(instant) => {
                trace!("offloader timeout {:?}", instant);
                instant
            }
            offloader::Output::SocketTransmit(transmit) => {
                trace!("offloader socket transmit {:?}", transmit);
                if let Err(error) = udp_socket
                    .send_to(&transmit.contents, transmit.destination)
                    .await
                {
                    error!("error during UDP socket send, {}", error);
                }
                continue;
            }
            offloader::Output::SdpTransmit(sdp_transmit) => {
                trace!("offloader sdp transmit {:?}", sdp_transmit);
                if let Err(error) = sdp_sink.send(sdp_transmit).await {
                    error!("error during send of sdp transmit, {}", error);
                }
                continue;
            }
            offloader::Output::TaskResult(result) => {
                // receive task result
                // lookup response channel from slab by index
                // construct WorkloadResult
                // send result to response channel
                error!("implement task result handling!!!");
                info!("received result: {:?}", result);
                continue;
            }
        };

        tokio::select! {
            // poll task queue
            task = fused_task_queue.next() => {
                if let Some(next_task) = task {
                    debug!("task queue new task");
                    // offload task using offloader
                    // deconstruct Workload, insert channel into slab, create new task
                    let task = next_task.to_task(&mut active_tasks);
                    offloader.handle_task(task);
                }
            }
            // poll announce stream for new providers
            announce = provider_stream.next() => {
                if let Some(announce_str) = announce {
                    let announce_str: String = announce_str;
                    debug!("announce stream event {:?}", announce_str);
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
                            continue;
                        }
                    }
                }
            }
            // poll sdp rendezvouz for new messages
            sdp_message = sdp_stream.next() => {
                if let Some(sdp_message_str) = sdp_message {
                    let sdp_message_str: String = sdp_message_str;
                    debug!("sdp stream event {:?}", sdp_message_str);
                    match serde_json::from_str(&sdp_message_str) {
                        Ok(sdp_msg_parsed) => {
                            offloader.handle_sdp(sdp_msg_parsed);
                        },
                        Err(error) => {
                            error!("error during sdp message parsing, {}", error);
                            continue;
                        }
                    }
                }
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
                                    let input = Input::SocketReceive(Instant::now(), Receive {
                                        proto: Protocol::Udp,
                                            source,
                                            destination: local_address,
                                            contents: datagram,
                                    });
                                    offloader.handle_input(input);
                                },
                                Err(error) => {
                                    error!("error during udp buffer to datagram transform, {}", error);
                                    continue;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            }
            // poll timeout
            _ = sleep_until(next_timeout.into()) => {
                if fused_task_queue.is_done() {
                    return Ok(())
                }
                trace!("next timeout for offloader");
                offloader.handle_input(Input::Timeout(Instant::now()));
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{net::SocketAddr, str::FromStr};

    use nid::Nanoid;
    use str0m::{Candidate, net::Protocol};
    use tracing::Level;

    use crate::connection::rtc_connection::{Sdp, SdpMessage};

    fn setup_logging() {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(Level::DEBUG)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .map_err(|_err| eprintln!("Unable to set global default subscriber"))
            .unwrap();
    }

    #[test]
    fn test_candidate_serde_de() {
        let sdp_string = r#"{"source":"5EM8N6BN9_Fuom8B-lP0C","destination":"_8Ca3Cqgd1ROkLj1p5-Ii","msg":{"Candidate":{"candidate":"a=candidate:4 1 UDP 1686109439 94.134.111.11 23154 typ srflx raddr 0.0.0.0 rport 0","sdpMLineIndex":null,"sdpMid":"7gC","usernameFragment":null}}}"#;
        let _sdp_message: SdpMessage = serde_json::from_str(&sdp_string).unwrap();
    }

    #[test]
    fn test_candidate_serde_se() {
        let address = SocketAddr::from_str("94.134.111.11:23154").unwrap();
        let sdp_message = SdpMessage {
            source: Nanoid::from_str("5EM8N6BN9_Fuom8B-lP0C").unwrap(),
            destination: Nanoid::from_str("_8Ca3Cqgd1ROkLj1p5-Ii").unwrap(),
            msg: Sdp::Candidate(Candidate::host(address, Protocol::Udp).unwrap()),
        };
        let sdp_string = serde_json::to_string(&sdp_message).unwrap();
        println!("{}", sdp_string);
    }
}
