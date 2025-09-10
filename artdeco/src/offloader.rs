use std::time::{Duration, Instant};

use nid::Nanoid;
use serde::{Deserialize, Serialize};
use str0m::net::{Receive, Transmit};
use tracing::{debug, error, info, trace};

use crate::{
    connection::{
        self, rtc_connection::{RTCConnectionConfig, SdpMessage}, RtcConnectionManager
    },
    provider::{self, ProviderManager},
    scheduler::{self, ProviderState, Scheduler},
    task::Task,
};

pub enum Input<'a> {
    SocketReceive(Instant, Receive<'a>),
    Timeout(Instant),
}

pub enum Output {
    Timeout(Instant),
    SocketTransmit(Transmit),
    SdpTransmit(String),
    TaskStatusUpdate,
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
    pub fn new(rtc_config: RTCConnectionConfig, scheduler: S) -> Self {
        Self {
            connection_manager: RtcConnectionManager::new(rtc_config),
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
    }

    pub fn handle_input(&mut self, input: Input) {
        match input {
            Input::SocketReceive(instant, receive) => {
                self.connection_manager
                    .handle_input(connection::Input::SocketReceive(instant, receive));
            }
            Input::Timeout(instant) => {
                self.connection_manager
                    .handle_input(connection::Input::Timeout(instant));
                self.provider_manager
                    .handle_input(provider::Input::Timeout(instant));
                self.scheduler.handle_timeout(instant);
                self.last_instant = instant;
            }
        }
    }

    pub fn handle_task(&mut self, task: Task) {
        info!("scheduling task");
        self.scheduler.schedule(task);
    }

    pub fn poll_output(&mut self) -> Output {
        let mut next_timeout = self.last_instant + Duration::from_secs(10);
        match self.scheduler.poll_output() {
            scheduler::Output::Timeout(instant) => next_timeout = next_timeout.min(instant),
            scheduler::Output::Connect(uuid) => {
                info!("received connection request for {} from scheduler", uuid);
                self.connection_manager.connect(uuid);
                self.provider_manager.create(uuid);
            }
            scheduler::Output::Offload(uuid, task) => {
                info!("offloading task to {}", uuid);
                self.provider_manager.offload(task, uuid);
            }
        }

        match self.provider_manager.poll_output() {
            provider::Output::TaskStatusUpdate(status) => todo!("task status not yet implemented"),
            provider::Output::ProviderTransmit(uuid, items) => {
                debug!("sending provider transmit to connection {}", uuid);
                if let Err(err) = self.connection_manager.send(uuid, &items) {
                    error!("cannot send to {} because of {}", uuid, err);
                }
            }
            provider::Output::None => trace!("provider none output"),
        }

        match self.connection_manager.poll_output() {
            connection::Output::Timeout(instant) => next_timeout = next_timeout.min(instant),
            connection::Output::Message(data_event) => {
                debug!("Received data event {:?}", data_event);
                self.provider_manager
                    .handle_input(provider::Input::ProviderReceive(
                        data_event.destination,
                        data_event.data,
                    ));
            }
            connection::Output::SdpTransmit(sdp_message) => {
                return Output::SdpTransmit(sdp_message);
            }
            connection::Output::UdpTransmit(transmit) => {
                return Output::SocketTransmit(transmit);
            }
            connection::Output::ChannelOpen(nanoid) => self
                .scheduler
                .handle_provider_state(nanoid, ProviderState::Connected),
            connection::Output::ChannelClosed(nanoid) => self
                .scheduler
                .handle_provider_state(nanoid, ProviderState::Disconnected),
        }

        return Output::Timeout(next_timeout);
    }
}
