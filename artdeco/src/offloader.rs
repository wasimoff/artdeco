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
use tracing::{debug, error, info, trace, warn};

use crate::{
    connection::{
        self, InputInner, RtcConnectionManager,
        rtc_connection::{RTCConnectionConfig, SdpMessage},
    },
    provider::{self, ProviderManager},
    scheduler::{self, ProviderState, Scheduler},
    task::{Task, TaskResult},
};

use lazy_static::lazy_static;

pub enum Input<'a> {
    SocketReceive(Instant, UdpReceive<'a>),
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

lazy_static! {
    pub(crate) static ref TIMEOUT: Instant = Instant::now() + Duration::from_secs(600);
}

pub(crate) enum Receive<'a> {
    Stun(message::Message<'a>),
    Str0m(str0m::net::DatagramRecv<'a>),
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
            Ok(recv) => Ok(Receive::Str0m(recv)),
            Err(err) => Err(err),
        }
    }
}

impl<'a> From<str0m::net::DatagramRecv<'a>> for Receive<'a> {
    fn from(value: str0m::net::DatagramRecv<'a>) -> Self {
        Self::Str0m(value)
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
    // capabilities
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProviderAnnounce {
    pub announce: ProviderAnnounceMsg,
    pub last: Instant,
}

pub struct Offloader<S> {
    connection_manager: RtcConnectionManager,
    provider_manager: ProviderManager,
    scheduler: S,
    last_instant: Instant,
}

impl<S: Scheduler> Offloader<S> {
    pub fn new(rtc_config: RTCConnectionConfig, scheduler: S, stun_server: SocketAddr) -> Self {
        Self {
            connection_manager: RtcConnectionManager::new(rtc_config, stun_server),
            provider_manager: ProviderManager::new(),
            scheduler,
            last_instant: Instant::now(),
        }
    }

    pub fn handle_sdp(&mut self, sdp_message: SdpMessage) {
        self.connection_manager.handle_sdp(sdp_message);
    }

    pub fn handle_announce(&mut self, announce: ProviderAnnounce) {
        self.scheduler.handle_announce(announce);
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
                self.connection_manager.handle_input(connection::Input {
                    inner: InputInner::Timeout,
                    instant,
                });
                self.update_instant(instant);
            }
        }
    }

    fn update_instant(&mut self, instant: Instant) {
        self.provider_manager
            .handle_input(provider::Input::Timeout(instant));
        self.scheduler.handle_timeout(instant);
        self.last_instant = instant;
    }

    pub fn handle_task(&mut self, task: Task) {
        info!("scheduling task");
        self.scheduler.schedule(task);
    }

    pub fn poll_output(&mut self) -> Output {
        let mut next_timeout = *TIMEOUT;
        match self.scheduler.poll_output() {
            scheduler::Output::Timeout(instant) => next_timeout = next_timeout.min(instant),
            scheduler::Output::Connect(uuid) => {
                trace!("received connection request for {} from scheduler", uuid);
                self.connection_manager.handle_input(connection::Input {
                    inner: InputInner::Connect(uuid),
                    instant: self.last_instant,
                });
                self.provider_manager.create(uuid);
            }
            scheduler::Output::Offload(uuid, task) => {
                info!("offloading task to {}", uuid);
                self.provider_manager.offload(task, uuid);
            }
        }

        match self.provider_manager.poll_output() {
            provider::Output::TaskResult(uuid, task_result) => {
                if let Some(result) = self.scheduler.handle_taskresult(uuid, task_result) {
                    return Output::TaskResult(result);
                }
            }
            provider::Output::ProviderTransmit(uuid, items) => {
                debug!("sending provider transmit to connection {}", uuid);
                if let Err(err) = self.connection_manager.send(uuid, &items) {
                    error!("cannot send to {} because of {}", uuid, err);
                }
            }
            provider::Output::Timeout(instant) => next_timeout = next_timeout.min(instant),
        }

        match self.connection_manager.poll_output() {
            connection::Output::Timeout(instant) => next_timeout = next_timeout.min(instant),
            connection::Output::Message(instant, data_event) => {
                trace!("Received data event {:?}", data_event);
                self.provider_manager
                    .handle_input(provider::Input::ProviderReceive(
                        instant,
                        data_event.destination,
                        data_event.data,
                    ));
                return Output::Timeout(self.last_instant);
            }
            connection::Output::SdpTransmit(sdp_message) => {
                return Output::SdpTransmit(sdp_message);
            }
            connection::Output::UdpTransmit(transmit) => {
                return Output::SocketTransmit(transmit);
            }
            connection::Output::ChannelOpen(nanoid) => {
                self.scheduler
                    .handle_provider_state(nanoid, ProviderState::Connected);
                return Output::Timeout(self.last_instant);
            }
            connection::Output::ChannelClosed(nanoid) => {
                self.scheduler
                    .handle_provider_state(nanoid, ProviderState::Disconnected);
                return Output::Timeout(self.last_instant);
            }
        }

        return Output::Timeout(next_timeout);
    }
}
