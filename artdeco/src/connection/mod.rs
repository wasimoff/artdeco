use std::collections::HashMap;
use std::collections::VecDeque;
use std::time::Duration;
use std::time::Instant;

use anyhow::anyhow;
use nid::Nanoid;
use rtc_connection::RTCConnection;
use rtc_connection::RTCConnectionConfig;

use str0m::net::Receive;
use str0m::net::Transmit;
use tracing::info;

use crate::connection::rtc_connection::SdpMessage;

pub mod rtc_connection;

#[derive(Debug)]
pub struct DataEvent {
    pub destination: Nanoid,
    pub data: Vec<u8>,
}

pub struct DataInput {
    pub source: Nanoid,
    pub data: Vec<u8>,
}

pub enum Output {
    Timeout(Instant),
    Message(DataEvent),
    SdpTransmit(String),
    UdpTransmit(Transmit),
    ChannelOpen(Nanoid),
    ChannelClosed(Nanoid),
}

pub enum Input<'a> {
    SocketReceive(Instant, Receive<'a>),
    Timeout(Instant),
}

pub struct RtcConnectionManager {
    rtc_connections: HashMap<Nanoid, RTCConnection>,
    default_rtc_config: RTCConnectionConfig,
    output_buffer: VecDeque<Output>,
    last_instant: Instant,
}

impl RtcConnectionManager {
    pub fn new(rtc_config: RTCConnectionConfig) -> Self {
        Self {
            rtc_connections: HashMap::new(),
            default_rtc_config: rtc_config,
            output_buffer: VecDeque::new(),
            last_instant: Instant::now(),
        }
    }

    pub fn connect(&mut self, destination: Nanoid) {
        if !self.rtc_connections.contains_key(&destination) {
            let mut rtc_connection = RTCConnection::new(self.default_rtc_config.clone());
            rtc_connection.connect(destination);
            self.rtc_connections.insert(destination, rtc_connection);
        }
        // TODO maybe reconnect necessary?
    }

    pub fn send(&mut self, destination: Nanoid, data: &[u8]) -> anyhow::Result<()> {
        if let Some(con) = self
            .rtc_connections
            .get_mut(&destination)
            .filter(|con| (*con).is_connected())
        {
            con.send_data(data);
            Ok(())
        } else {
            Err(anyhow!(
                "not sending because not connected, {:?}",
                destination
            ))
        }
    }

    pub fn is_connected(&self, destination: Nanoid) -> bool {
        self.rtc_connections
            .get(&destination)
            .map(RTCConnection::is_connected)
            .unwrap_or(false)
    }

    pub fn handle_sdp(&mut self, sdp_message: SdpMessage) {
        if let Some(connection) = self.rtc_connections.get_mut(&sdp_message.source) {
            connection.handle_input(rtc_connection::Receive::Sdp(sdp_message));
        }
    }

    pub fn handle_input(&mut self, input: Input) {
        match input {
            Input::SocketReceive(instant, receive) => {
                let rtc_input =
                    rtc_connection::Receive::Rtc(str0m::Input::Receive(instant, receive));
                if let Some((_, con)) = self
                    .rtc_connections
                    .iter_mut()
                    .find(|(_, c)| c.accepts(&rtc_input))
                {
                    // We found the client that accepts the input.
                    con.handle_input(rtc_input);
                } else {
                    info!("No client accepts UDP input: {:?}", rtc_input);
                }
                self.last_instant = instant;
            }
            Input::Timeout(instant) => {
                for (_, con) in &mut self.rtc_connections {
                    con.handle_input(rtc_connection::Receive::Timeout(instant));
                }
                self.last_instant = instant;
            }
        }
    }

    pub fn poll_output(&mut self) -> Output {
        let mut smallest_timeout = self.last_instant + Duration::from_secs(10);

        // poll all rtc connections
        for (key, connection) in &mut self.rtc_connections {
            match connection.poll_output() {
                rtc_connection::Output::RtcTransmit(transmit) => {
                    self.output_buffer.push_back(Output::UdpTransmit(transmit));
                }
                rtc_connection::Output::SdpTransmit(sdp_message) => {
                    let serialized_sdp =
                        serde_json::to_string(&sdp_message).expect("cannot serialize SdpMessage");
                    self.output_buffer
                        .push_back(Output::SdpTransmit(serialized_sdp))
                }
                rtc_connection::Output::ChannelData(data) => {
                    self.output_buffer.push_back(Output::Message(DataEvent {
                        destination: *key,
                        data,
                    }));
                }
                rtc_connection::Output::Timeout(instant) => {
                    smallest_timeout = smallest_timeout.min(instant);
                }
                rtc_connection::Output::ChannelOpen => {
                    self.output_buffer.push_back(Output::ChannelOpen(*key))
                }
                rtc_connection::Output::ChannelClosed => {
                    self.output_buffer.push_back(Output::ChannelClosed(*key))
                }
            }
        }

        self.output_buffer
            .pop_front()
            .unwrap_or(Output::Timeout(smallest_timeout))
    }
}
