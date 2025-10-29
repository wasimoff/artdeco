use std::borrow::Borrow;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::time::Instant;

use anyhow::anyhow;
use nid::Nanoid;
use rtc_connection::RTCConnection;
use rtc_connection::RTCConnectionConfig;

use str0m::Candidate;
use str0m::net;
use str0m::net::DatagramRecv;
use str0m::net::Protocol;
use stun_proto::agent::HandleStunReply;
use stun_proto::agent::StunAgent;
use stun_proto::agent::StunAgentPollRet;
use stun_proto::types::TransportType;
use stun_proto::types::attribute::XorMappedAddress;
use stun_proto::types::message::BINDING;
use stun_proto::types::message::Message;
use stun_proto::types::message::MessageWriteVec;
use stun_proto::types::message::TransactionId;
use stun_proto::types::prelude::MessageWrite;
use stun_proto::types::prelude::MessageWriteExt;
use tracing::debug;
use tracing::trace;

use crate::connection::rtc_connection::SdpMessage;
use crate::consumer;
use crate::consumer::TIMEOUT;
use crate::consumer::UdpReceive;
use crate::consumer::UdpTransmit;

pub mod packet;
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
    Message(Instant, DataEvent),
    SdpTransmit(String),
    UdpTransmit(UdpTransmit),
    ChannelOpen(Nanoid),
    ChannelClosed(Nanoid),
}

pub enum InputInner<'a> {
    SocketReceive(Box<UdpReceive<'a>>),
    Timeout,
    Connect(Nanoid),
    Disconnect(Nanoid),
}

pub struct Input<'a> {
    pub inner: InputInner<'a>,
    pub instant: Instant,
}

pub struct RtcConnectionManager {
    rtc_connections: HashMap<Nanoid, RTCConnection>,
    default_rtc_config: RTCConnectionConfig,
    output_buffer: VecDeque<Output>,
    last_instant: Instant,
    stun_agent: StunAgent,
    stun_transactions: HashMap<TransactionId, Nanoid>,
    stun_server: SocketAddr,
    local_addr: SocketAddr,
    base_instant: std::time::Instant,
    ready: bool,
}

impl RtcConnectionManager {
    pub fn new(rtc_config: RTCConnectionConfig, stun_server: SocketAddr) -> Self {
        let local_addr = rtc_config.local_host_addr[0];
        let agent = StunAgent::builder(TransportType::Udp, local_addr).build();

        let base_instant = rtc_config.start;
        Self {
            rtc_connections: HashMap::new(),
            default_rtc_config: rtc_config,
            output_buffer: VecDeque::new(),
            last_instant: Instant::now(),
            stun_agent: agent,
            stun_transactions: HashMap::new(),
            stun_server,
            local_addr,
            base_instant,
            ready: true,
        }
    }

    fn new_stun_request(&mut self, con: Nanoid) {
        let msg = Message::builder_request(BINDING, MessageWriteVec::new());
        let transaction_id = msg.transaction_id();
        trace!("new stun transaction {}", transaction_id);
        let _transmit = self
            .stun_agent
            .send_request(
                msg.finish(),
                self.stun_server,
                from_std(self.base_instant, self.last_instant),
            )
            .unwrap();
        self.stun_transactions.insert(transaction_id, con);
        // no need to put transmit into queue since it will be added with the next poll_output anyway
    }

    fn handle_stun_response(&mut self, message: Message) {
        let transaction_id = message.transaction_id();

        if let Some(connection_id) = self.stun_transactions.remove(&transaction_id) {
            trace!("removing transaction {}", transaction_id);
            if let Some(connection) = self.rtc_connections.get_mut(&connection_id) {
                // Extract mapped address from STUN response
                message
                    .iter_attributes()
                    .filter_map(|(_offest, raw_att)| raw_att.borrow().try_into().ok())
                    .map(|xor_mapped_address: XorMappedAddress| {
                        Candidate::server_reflexive(
                            xor_mapped_address.addr(transaction_id),
                            self.local_addr,
                            Protocol::Udp,
                        )
                        .unwrap()
                    })
                    .for_each(|candidate| {
                        connection.add_local_candidate(candidate);
                    });
            }
        } else {
            debug!(
                "Received STUN response for unknown transaction: {}, message: {}",
                transaction_id, message
            );
        }
    }

    pub fn send(&mut self, destination: Nanoid, data: &[u8]) -> anyhow::Result<()> {
        if let Some(con) = self
            .rtc_connections
            .get_mut(&destination)
            .filter(|con| (*con).is_connected())
        {
            self.ready = true;
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
        let Some(connection) = self.rtc_connections.get_mut(&sdp_message.source) else {
            return;
        };

        let receive = rtc_connection::Receive::Sdp(sdp_message);
        if !connection.accepts(&receive) {
            return;
        }

        self.ready = true;
        connection.handle_input(receive);
    }

    pub fn handle_input(&mut self, input: Input) {
        let Input { inner, instant } = input;
        self.last_instant = instant;
        self.ready = true;

        match inner {
            InputInner::SocketReceive(receive) => {
                let UdpReceive {
                    source,
                    destination,
                    contents,
                } = *receive;
                match contents {
                    consumer::Receive::Stun(message) => {
                        // check if stun client can handle message, else forward to rtc
                        match self.stun_agent.handle_stun(message, source) {
                            HandleStunReply::Drop(message)
                            | HandleStunReply::IncomingStun(message) => {
                                trace!("forwarding stun");
                                let datagram_recv =
                                    DatagramRecv::try_from(message.as_bytes()).unwrap();
                                let input = InputInner::SocketReceive(Box::new(UdpReceive {
                                    source,
                                    destination,
                                    contents: consumer::Receive::Str0m(Box::new(datagram_recv)),
                                }));
                                self.handle_input(Input {
                                    inner: input,
                                    instant,
                                });
                            }
                            HandleStunReply::ValidatedStunResponse(message)
                            | HandleStunReply::UnvalidatedStunResponse(message) => {
                                debug!("handling stun from {}", source);
                                self.handle_stun_response(message);
                            }
                        }
                    }
                    consumer::Receive::Str0m(datagram_recv) => {
                        let rtc_input = rtc_connection::Receive::Rtc(str0m::Input::Receive(
                            instant,
                            net::Receive {
                                proto: net::Protocol::Udp,
                                source,
                                destination,
                                contents: *datagram_recv,
                            },
                        ));
                        if let Some((_, con)) = self
                            .rtc_connections
                            .iter_mut()
                            .find(|(_, c)| c.accepts(&rtc_input))
                        {
                            // We found the client that accepts the input.
                            con.handle_input(rtc_input);
                        } else {
                            debug!("No client accepts UDP input: {:?}", rtc_input);
                        }
                    }
                }
            }
            InputInner::Timeout => {
                //do nothing
            }
            InputInner::Connect(destination) => {
                if !self.rtc_connections.contains_key(&destination) {
                    let mut rtc_connection =
                        RTCConnection::new(self.default_rtc_config.clone(), destination);
                    rtc_connection.connect();
                    self.rtc_connections.insert(destination, rtc_connection);
                    self.new_stun_request(destination);
                }
            }
            InputInner::Disconnect(nanoid) => {
                self.rtc_connections.remove(&nanoid);
                self.output_buffer.push_back(Output::ChannelClosed(nanoid));
            }
        }
        // advance time for all connections
        for con in self.rtc_connections.values_mut() {
            con.handle_input(rtc_connection::Receive::Timeout(instant));
        }
    }

    pub fn poll_output(&mut self) -> Output {
        let mut smallest_timeout = self.last_instant + TIMEOUT;
        assert!(self.ready);

        // poll all rtc connections
        for (key, connection) in &mut self.rtc_connections {
            match connection.poll_output() {
                rtc_connection::Output::RtcTransmit(transmit) => {
                    self.output_buffer
                        .push_back(Output::UdpTransmit(transmit.into()));
                }
                rtc_connection::Output::SdpTransmit(sdp_message) => {
                    let serialized_sdp =
                        serde_json::to_string(&sdp_message).expect("cannot serialize SdpMessage");
                    self.output_buffer
                        .push_back(Output::SdpTransmit(serialized_sdp))
                }
                rtc_connection::Output::ChannelData(data) => {
                    self.output_buffer.push_back(Output::Message(
                        self.last_instant,
                        DataEvent {
                            destination: *key,
                            data,
                        },
                    ));
                }
                rtc_connection::Output::Timeout(instant) => {
                    trace!("Timeout from single connection {:?}", instant);
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

        let last_instant = from_std(self.base_instant, self.last_instant);
        trace!("STUN last_instant {}", last_instant);
        match self.stun_agent.poll(last_instant) {
            StunAgentPollRet::TransactionTimedOut(transaction_id)
            | StunAgentPollRet::TransactionCancelled(transaction_id) => {
                self.stun_transactions.remove(&transaction_id);
            }
            StunAgentPollRet::WaitUntil(instant) => {
                if last_instant >= instant
                    && let Some(transmit) = self.stun_agent.poll_transmit(last_instant)
                {
                    trace!("new stun transmission");
                    self.output_buffer
                        .push_back(Output::UdpTransmit(transmit.into()));
                } else {
                    trace!("STUN timeout until {}", instant);
                    let inst = to_std(instant, self.base_instant);
                    trace!("Timeout from stun {:?}", inst);
                    smallest_timeout = smallest_timeout.min(inst);
                }
            }
        }

        if self.output_buffer.is_empty() {
            self.ready = false;
        }
        self.output_buffer
            .pop_front()
            .unwrap_or(Output::Timeout(smallest_timeout))
    }
}

fn to_std(
    sans_io_instant: sans_io_time::Instant,
    base_instant: std::time::Instant,
) -> std::time::Instant {
    let duration_since = std::time::Duration::from_nanos(
        sans_io_instant
            .as_nanos()
            .try_into()
            .expect("Elapsed time too large to fit into Duration"),
    );
    base_instant + duration_since
}

fn from_std(
    base_instant: std::time::Instant,
    target_instant: std::time::Instant,
) -> sans_io_time::Instant {
    let duration_since = target_instant.saturating_duration_since(base_instant);
    sans_io_time::Instant::from_nanos(duration_since.as_nanos() as i64)
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    #[test]
    fn test_from_to_std() {
        use super::*;

        let base = std::time::Instant::now();
        let offset = Duration::from_millis(1000);
        let target = base + offset;

        // Test from_std conversion
        let sans_io_instant = from_std(base, target);
        assert_eq!(sans_io_instant.as_nanos(), offset.as_nanos() as i64);

        // Test to_std conversion
        let converted_back = to_std(sans_io_instant, base);
        assert_eq!(converted_back, target);

        // Test round-trip conversion
        let original = std::time::Instant::now();
        let sans_io = from_std(base, original);
        let back_to_std = to_std(sans_io, base);
        assert_eq!(back_to_std, original);

        // Test with zero duration
        let zero_target = base;
        let zero_sans_io = from_std(base, zero_target);
        let zero_back = to_std(zero_sans_io, base);
        assert_eq!(zero_back, zero_target);
    }
}
