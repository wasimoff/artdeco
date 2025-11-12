use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use nid::Nanoid;
use serde::{Deserialize, Serialize};

use str0m::{
    error::NetError,
    net::{Protocol, Transmit},
};
use stun_proto::types::{TransportType, message};
use tracing::{debug, error, info, trace};

use crate::{
    connection::{
        self, InputInner, RtcConnectionManager,
        rtc_connection::{RTCConnectionConfig, SdpMessage},
    },
    protocol::wasimoff::task::trace_event::EventType,
    provider::{self, ProviderManager},
    scheduler::{self, ProviderState, Scheduler},
    task::{Task, TaskResult},
};

pub enum Input<'a> {
    SocketReceive(Instant, Box<UdpReceive<'a>>),
    Timeout(Instant),
}

pub struct UdpReceive<'a> {
    pub source: SocketAddr,
    pub destination: SocketAddr,
    pub(crate) contents: Receive<'a>,
}

impl<'a> UdpReceive<'a> {
    pub fn new(
        source: SocketAddr,
        destination: SocketAddr,
        data: &'a [u8],
    ) -> Result<Self, NetError> {
        let contents = Receive::try_from(data)?;
        Ok(UdpReceive {
            source,
            destination,
            contents,
        })
    }
}

pub const TIMEOUT: Duration = Duration::from_secs(600);

pub(crate) enum Receive<'a> {
    Stun(message::Message<'a>),
    Str0m(Box<str0m::net::DatagramRecv<'a>>),
}

impl<'a> TryFrom<&'a [u8]> for Receive<'a> {
    type Error = NetError;

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        // try stun first
        if let Ok(msg) = message::Message::from_bytes(value) {
            return Ok(Receive::Stun(msg));
        }

        // then try str0m
        match str0m::net::DatagramRecv::try_from(value) {
            Ok(recv) => Ok(Receive::Str0m(Box::new(recv))),
            Err(err) => Err(err),
        }
    }
}

impl<'a> From<str0m::net::DatagramRecv<'a>> for Receive<'a> {
    fn from(value: str0m::net::DatagramRecv<'a>) -> Self {
        Self::Str0m(Box::new(value))
    }
}

impl From<Transmit> for UdpTransmit {
    fn from(value: Transmit) -> Self {
        if !matches!(value.proto, Protocol::Udp) {
            panic!("can only convert UDP transmits")
        }
        let Transmit {
            proto: _,
            source,
            destination,
            contents,
        } = value;
        Self {
            source,
            destination,
            contents: contents.into(),
        }
    }
}

impl<T: AsRef<[u8]>> From<stun_proto::agent::Transmit<T>> for UdpTransmit {
    fn from(value: stun_proto::agent::Transmit<T>) -> Self {
        if !matches!(value.transport, TransportType::Udp) {
            panic!("can only convert UDP transmits")
        }
        let stun_proto::agent::Transmit {
            data,
            transport: _,
            from,
            to,
        } = value;
        Self {
            source: from,
            destination: to,
            contents: data.as_ref().into(),
        }
    }
}

#[derive(Debug)]
pub struct UdpTransmit {
    pub source: SocketAddr,
    pub destination: SocketAddr,
    pub contents: Vec<u8>,
}

pub enum Output {
    Timeout(Instant),
    SocketTransmit(UdpTransmit),
    SdpTransmit(String),
    TaskResult(TaskResult),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ProviderAnnounceMsg {
    pub id: Nanoid,
    pub concurrency: u32,
    pub tasks: u32,
    pub timestamp: u64,
    #[serde(rename = "activeConnections")]
    pub active_connections: u32,
    #[serde(rename = "maxConnections")]
    pub max_connections: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProviderAnnounce {
    pub announce: ProviderAnnounceMsg,
    pub last: Instant,
}

pub struct Consumer<S> {
    connection_manager: RtcConnectionManager,
    provider_manager: ProviderManager,
    scheduler: S,
    last_instant: Instant,
    ready: bool,
}

impl<S: Scheduler> Consumer<S> {
    pub fn new(rtc_config: RTCConnectionConfig, scheduler: S, stun_server: SocketAddr) -> Self {
        Self {
            connection_manager: RtcConnectionManager::new(rtc_config, stun_server),
            provider_manager: ProviderManager::new(),
            scheduler,
            last_instant: Instant::now(),
            ready: true,
        }
    }

    pub fn handle_sdp(&mut self, sdp_message: SdpMessage) {
        self.connection_manager.handle_sdp(sdp_message);
        self.ready = true;
    }

    pub fn handle_announce(&mut self, announce: ProviderAnnounce) {
        debug!("received announce for {}", announce.announce.id);
        self.scheduler.handle_announce(announce);
        self.ready = true;
        //self.handle_input(input);
    }

    pub fn handle_input(&mut self, input: Input) {
        match input {
            Input::SocketReceive(instant, receive) => {
                self.connection_manager.handle_input(connection::Input {
                    inner: InputInner::SocketReceive(receive),
                    instant,
                });
                self.update_instant(instant);
            }
            Input::Timeout(instant) => {
                self.update_instant(instant);
            }
        }
    }

    fn update_instant(&mut self, instant: Instant) {
        self.connection_manager.handle_input(connection::Input {
            inner: InputInner::Timeout,
            instant,
        });
        self.provider_manager
            .handle_input(provider::Input::Timeout(instant));
        self.scheduler.handle_timeout(instant);
        self.last_instant = instant;
        self.ready = true;
    }

    pub fn handle_task(&mut self, mut task: Task) {
        debug!("scheduling task");
        task.metrics
            .push_trace_event_now(EventType::ArtDecoSchedulerEnter, None);
        self.scheduler.schedule(task);
        self.ready = true;
    }

    pub fn poll_output(&mut self) -> Output {
        let mut next_timeout = self.last_instant + TIMEOUT;
        assert!(self.ready);

        match self.scheduler.poll_output() {
            scheduler::Output::Timeout(instant) => {
                trace!("Timeout from scheduler {:?}", instant);
                next_timeout = next_timeout.min(instant)
            }
            scheduler::Output::Connect(uuid) => {
                trace!("received connection request for {} from scheduler", uuid);
                self.connection_manager.handle_input(connection::Input {
                    inner: InputInner::Connect(uuid),
                    instant: self.last_instant,
                });
                self.provider_manager.create(uuid);
            }
            scheduler::Output::Offload(uuid, mut task) => {
                info!("offloading task to {}", uuid);
                task.metrics
                    .push_trace_event_now(EventType::ArtDecoSchedulerScheduled, None);
                self.provider_manager.offload(task, uuid);
            }
            scheduler::Output::Disconnect(nanoid) => {
                self.connection_manager.handle_input(connection::Input {
                    inner: InputInner::Disconnect(nanoid),
                    instant: self.last_instant,
                })
            }
        }

        match self.provider_manager.poll_output() {
            provider::Output::TaskResult(uuid, mut task_result) => {
                task_result
                    .metrics
                    .push_trace_event_now(EventType::ArtDecoSchedulerResultEnter, None);
                if let Some(mut result) = self.scheduler.handle_taskresult(uuid, task_result) {
                    result
                        .metrics
                        .push_trace_event_now(EventType::ArtDecoSchedulerResultLeave, None);
                    return Output::TaskResult(result);
                }
                next_timeout = self.last_instant;
            }
            provider::Output::ProviderTransmit(uuid, items) => {
                debug!("sending provider transmit to connection {}", uuid);
                if let Err(err) = self.connection_manager.send(uuid, &items) {
                    error!("cannot send to {} because of {}", uuid, err);
                }
            }
            provider::Output::Timeout(instant) => {
                trace!("Timeout from provider {:?}", instant);
                next_timeout = next_timeout.min(instant)
            }
        }

        match self.connection_manager.poll_output() {
            connection::Output::Timeout(instant) => {
                trace!("Timeout from connection {:?}", instant);
                next_timeout = next_timeout.min(instant)
            }
            connection::Output::Message(instant, data_event) => {
                trace!("Received data event {:?}", data_event);
                self.provider_manager
                    .handle_input(provider::Input::ProviderReceive(
                        instant,
                        data_event.destination,
                        data_event.data,
                    ));
                trace!("Timeout from message {:?}", self.last_instant);
                next_timeout = self.last_instant;
            }
            connection::Output::SdpTransmit(sdp_message) => {
                return Output::SdpTransmit(sdp_message);
            }
            connection::Output::UdpTransmit(transmit) => {
                return Output::SocketTransmit(transmit);
            }
            connection::Output::ChannelOpen(nanoid) => {
                self.scheduler.handle_provider_state(
                    nanoid,
                    ProviderState::Connected,
                    self.last_instant,
                );
                trace!("Timeout from channel open {:?}", self.last_instant);
                next_timeout = self.last_instant;
            }
            connection::Output::ChannelClosed(nanoid) => {
                self.scheduler.handle_provider_state(
                    nanoid,
                    ProviderState::Disconnected,
                    self.last_instant,
                );
                trace!("Timeout from channel closed {:?}", self.last_instant);
                next_timeout = self.last_instant;
            }
        }

        self.ready = false;
        Output::Timeout(next_timeout)
    }
}
