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
use stun_proto::types::message::IntegrityAlgorithm;
use stun_proto::types::message::Message;
use stun_proto::types::message::MessageWriteVec;
use stun_proto::types::message::ShortTermCredentials;
use stun_proto::types::message::TransactionId;
use stun_proto::types::prelude::MessageWrite;
use stun_proto::types::prelude::MessageWriteExt;
use tracing::debug;
use tracing::info;

use crate::connection::rtc_connection::SdpMessage;
use crate::offloader;
use crate::offloader::TIMEOUT;
use crate::offloader::UdpReceive;
use crate::offloader::UdpTransmit;

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
    SocketReceive(UdpReceive<'a>),
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
    local_credentials: ShortTermCredentials,
    stun_server: SocketAddr,
    local_addr: SocketAddr,
}

impl RtcConnectionManager {
    pub fn new(rtc_config: RTCConnectionConfig, stun_server: SocketAddr) -> Self {
        let local_addr = rtc_config.local_host_addr[0];
        let agent = StunAgent::builder(TransportType::Udp, local_addr).build();

        let random_12_string: String = (0..12).map(|_| fastrand::alphanumeric()).collect();
        Self {
            rtc_connections: HashMap::new(),
            default_rtc_config: rtc_config,
            output_buffer: VecDeque::new(),
            last_instant: Instant::now(),
            stun_agent: agent,
            stun_transactions: HashMap::new(),
            local_credentials: ShortTermCredentials::new(random_12_string),
            stun_server,
            local_addr,
        }
    }

    fn new_stun_request(&mut self, con: Nanoid) {
        let mut msg = Message::builder_request(BINDING, MessageWriteVec::new());
        msg.add_message_integrity(
            &self.local_credentials.clone().into(),
            IntegrityAlgorithm::Sha256,
        )
        .unwrap();
        let transaction_id = msg.transaction_id();
        info!("new stun transaction {}", transaction_id);
        let _transmit = self
            .stun_agent
            .send_request(msg.finish(), self.stun_server, self.last_instant)
            .unwrap();
        self.stun_transactions.insert(transaction_id, con);
        // no need to put transmit into queue since it will be added with the next poll_output anyway
    }

    fn handle_stun_response(&mut self, message: Message) {
        let transaction_id = message.transaction_id();

        if let Some(connection_id) = self.stun_transactions.remove(&transaction_id) {
            debug!("removing transaction {}", transaction_id);
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
                "Received STUN response for unknown transaction: {}, message: {:?}",
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
        let Input { inner, instant } = input;
        self.last_instant = instant;
        match inner {
            InputInner::SocketReceive(receive) => {
                let UdpReceive {
                    source,
                    destination,
                    contents,
                } = receive;
                match contents {
                    offloader::Receive::Stun(message) => {
                        // check if stun client can handle message, else forward to rtc
                        match self.stun_agent.handle_stun(message, source) {
                            HandleStunReply::Drop(message)
                            | HandleStunReply::IncomingStun(message) => {
                                info!("forwarding stun");
                                let datagram_recv =
                                    DatagramRecv::try_from(message.as_bytes()).unwrap();
                                let input = InputInner::SocketReceive(UdpReceive {
                                    source,
                                    destination,
                                    contents: offloader::Receive::Str0m(datagram_recv),
                                });
                                self.handle_input(Input {
                                    inner: input,
                                    instant,
                                });
                            }
                            HandleStunReply::ValidatedStunResponse(message)
                            | HandleStunReply::UnvalidatedStunResponse(message) => {
                                info!("handling stun from {}", source);
                                self.handle_stun_response(message);
                            }
                        }
                    }
                    offloader::Receive::Str0m(datagram_recv) => {
                        let rtc_input = rtc_connection::Receive::Rtc(str0m::Input::Receive(
                            instant,
                            net::Receive {
                                proto: net::Protocol::Udp,
                                source,
                                destination,
                                contents: datagram_recv,
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
                            info!("No client accepts UDP input: {:?}", rtc_input);
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
        for (_, con) in &mut self.rtc_connections {
            con.handle_input(rtc_connection::Receive::Timeout(instant));
        }
    }

    pub fn poll_output(&mut self) -> Output {
        let mut smallest_timeout = *TIMEOUT;

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

        match self.stun_agent.poll(self.last_instant) {
            StunAgentPollRet::TransactionTimedOut(transaction_id)
            | StunAgentPollRet::TransactionCancelled(transaction_id) => {
                self.stun_transactions.remove(&transaction_id);
            }
            StunAgentPollRet::WaitUntil(instant) => {
                if self.last_instant >= instant
                    && let Some(transmit) = self.stun_agent.poll_transmit(self.last_instant)
                {
                    debug!("new stun transmission");
                    self.output_buffer
                        .push_back(Output::UdpTransmit(transmit.into()));
                } else {
                    smallest_timeout = smallest_timeout.min(instant)
                }
            }
        }

        self.output_buffer
            .pop_front()
            .unwrap_or(Output::Timeout(smallest_timeout))
    }
}
