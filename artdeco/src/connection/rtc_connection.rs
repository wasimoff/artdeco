use std::{collections::VecDeque, mem, net::SocketAddr, time::Instant};

use nid::Nanoid;
use serde::{Deserialize, Serialize};
use str0m::{
    Candidate, Input, Rtc,
    change::{SdpAnswer, SdpOffer, SdpPendingOffer},
    channel::ChannelId,
};
use tracing::{debug, error, info, trace};

// Import packet fragmentation functionality
use super::packet::{PacketDefragmenter, fragment_message};

#[derive(Default)]
pub enum State {
    Invalid,
    Offer {
        pending: SdpPendingOffer,
    },
    #[default]
    Standby,
    ChannelOpen {
        channel_id: ChannelId,
    },
}

pub enum Destination {
    Socket(SocketAddr),
    Channel(String),
}

pub struct Transmit {
    pub dest: Destination,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub enum Receive<'a> {
    Rtc(Input<'a>),
    Sdp(SdpMessage),
    Timeout(Instant),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SdpMessage {
    pub source: Nanoid,
    pub destination: Nanoid,
    pub msg: Sdp,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Sdp {
    Offer(SdpOffer),
    Answer(SdpAnswer),
    Candidate(Candidate),
}

#[derive(Debug)]
pub enum Output {
    RtcTransmit(str0m::net::Transmit),
    SdpTransmit(SdpMessage),
    Timeout(Instant),
    ChannelData(Vec<u8>),
    ChannelOpen,
    ChannelClosed,
}

// {\"source\":\"5EM8N6BN9_Fuom8B-lP0C\",\"destination\":\"_8Ca3Cqgd1ROkLj1p5-Ii\",\"msg\":{\"Candidate\":{\"candidate\":\"a=candidate:4 1 UDP 1686109439 94.134.111.11 23154 typ srflx raddr 0.0.0.0 rport 0\",\"sdpMLineIndex\":null,\"sdpMid\":\"7gC\",\"usernameFragment\":null}}}

pub struct RTCConnection {
    state: State,
    buffered_outputs: VecDeque<Output>,
    rtc: Rtc,
    pub config: RTCConnectionConfig,
    last: Instant,
    remote_uuid: Nanoid,
    ready: bool,
    defragmenter: PacketDefragmenter,
}

#[derive(Debug, Clone)]
pub struct RTCConnectionConfig {
    pub local_host_addr: Vec<SocketAddr>,
    pub data_channel_name: String,
    pub local_uuid: Nanoid,
    pub start: Instant,
}

impl RTCConnection {
    pub fn new(config: RTCConnectionConfig, remote_uuid: Nanoid) -> Self {
        let buffered_outputs = VecDeque::new();
        let mut rtc = Rtc::builder().build();
        for host_addr in &config.local_host_addr {
            let host_candidate = Candidate::host(*host_addr, "udp").expect("a host candidate");
            rtc.add_local_candidate(host_candidate);
        }

        let start = config.start;

        Self {
            state: State::Standby,
            rtc,
            buffered_outputs,
            config,
            last: start,
            remote_uuid,
            ready: true,
            defragmenter: PacketDefragmenter::new(),
        }
    }

    pub fn add_local_candidate(&mut self, candidate: Candidate) {
        self.rtc.add_local_candidate(candidate.clone());
        debug!("new candidate {}", candidate);
        let sdp_message = SdpMessage {
            source: self.config.local_uuid,
            destination: self.remote_uuid,
            msg: Sdp::Candidate(candidate),
        };
        self.ready = true;
        self.buffered_outputs
            .push_back(Output::SdpTransmit(sdp_message));
    }

    pub fn is_connected(&self) -> bool {
        self.rtc.is_connected()
    }

    pub fn connect(&mut self) {
        let mut change = self.rtc.sdp_api();
        let _channel_id = change.add_channel(self.config.data_channel_name.clone());
        let (offer, pending) = change.apply().unwrap();
        self.buffered_outputs
            .push_back(Output::SdpTransmit(SdpMessage {
                destination: self.remote_uuid,
                source: self.config.local_uuid,
                msg: Sdp::Offer(offer),
            }));

        self.ready = true;
        self.state = State::Offer { pending };
    }

    pub fn accepts(&self, input: &Receive) -> bool {
        match &self.state {
            State::Offer { pending: _ } => {
                if let Receive::Sdp(SdpMessage {
                    source,
                    destination,
                    msg: _,
                }) = input
                {
                    return self.config.local_uuid == *destination && *source == self.remote_uuid;
                }
            }
            State::Standby | State::ChannelOpen { channel_id: _ } => match input {
                Receive::Rtc(rtc_message) => return self.rtc.accepts(rtc_message),
                Receive::Sdp(sdp_message) => {
                    return self.config.local_uuid == sdp_message.destination;
                }
                Receive::Timeout(_instant) => return true,
            },
            _ => {}
        }
        false
    }

    pub fn send_data(&mut self, data: &[u8]) {
        match self.state {
            State::Invalid => {
                error!("Cannot send data in invalid state");
            }
            State::ChannelOpen { channel_id } => {
                trace!("fragmenting and writing to rtc datachannel");

                // Fragment the message
                let fragments = fragment_message(data);

                // Send each fragment
                for fragment in fragments {
                    if let Err(e) = self
                        .rtc
                        .channel(channel_id)
                        .unwrap()
                        .write(true, &fragment.data)
                    {
                        error!("Failed to send fragment to channel: {:?}", e);
                    }
                }
            }
            _ => {
                info!("Channel not yet open, cannot send data");
            }
        }
        self.ready = true;
    }

    pub fn handle_input(&mut self, input: Receive) {
        self.ready = true;
        match input {
            Receive::Rtc(input) => {
                trace!("Handle Rtc input: {:?}", input);
                self.rtc.handle_input(input).unwrap();
            }
            Receive::Sdp(SdpMessage {
                source,
                destination,
                msg,
            }) => {
                assert_eq!(self.config.local_uuid, destination);

                match msg {
                    Sdp::Offer(offer) => {
                        trace!("Handle SdpOffer: {:?}", offer);
                        let answer = self.rtc.sdp_api().accept_offer(offer).unwrap();
                        self.buffered_outputs
                            .push_back(Output::SdpTransmit(SdpMessage {
                                destination: source,
                                source: self.config.local_uuid,
                                msg: Sdp::Answer(answer),
                            }));
                    }
                    Sdp::Answer(answer) => {
                        trace!("Handle SdpAnswer: {:?}", answer);
                        let state = mem::take(&mut self.state);
                        if let State::Offer { pending } = state {
                            assert_eq!(self.remote_uuid, source);
                            if self.rtc.sdp_api().accept_answer(pending, answer).is_ok() {
                                debug!("Connection in standby after successful SDP exchange");
                                assert!(!self.rtc.sdp_api().has_changes());
                                self.state = State::Standby
                            } else {
                                error!("Couldn't accept sdp answer");
                            }
                        } else {
                            error!("no pending sdp offer, but answer received");
                            self.state = state;
                        }
                    }
                    Sdp::Candidate(candidate) => {
                        self.rtc.add_remote_candidate(candidate);
                    }
                }
            }
            Receive::Timeout(instant) => {
                self.last = instant;
                self.rtc.handle_input(Input::Timeout(instant)).unwrap()
            }
        }
    }

    pub fn poll_output(&mut self) -> Output {
        assert!(self.ready);
        if let State::Invalid = self.state {
            panic!("invalid state in poll");
        }
        match self.rtc.poll_output().unwrap() {
            str0m::Output::Timeout(instant) => {
                trace!("timeout in rtc, {:?}", instant);
                self.buffered_outputs.push_back(Output::Timeout(instant));
            }
            str0m::Output::Transmit(transmit) => {
                trace!("pending rtc transmission: {:?}", transmit);
                self.buffered_outputs
                    .push_back(Output::RtcTransmit(transmit))
            }
            str0m::Output::Event(event) => {
                trace!("received str0m event: {:?}", event);
                match event {
                    str0m::Event::ChannelData(channel_data) => {
                        // Process incoming data through the defragmenter
                        if let Err(e) = self.defragmenter.process_bytes(&channel_data.data) {
                            error!("Failed to process channel data: {:?}", e);
                        } else {
                            // Output any complete messages that were assembled
                            while let Some(complete_message) = self.defragmenter.next_message() {
                                self.buffered_outputs
                                    .push_back(Output::ChannelData(complete_message.data));
                            }
                        }
                    }
                    str0m::Event::ChannelOpen(channel_id, _label) => {
                        debug!("rtc channel open, {:?}", channel_id);
                        // will overwrite a previous channel open
                        self.state = State::ChannelOpen { channel_id };
                        self.buffered_outputs.push_back(Output::ChannelOpen);
                    }
                    str0m::Event::ChannelClose(channel_id) => {
                        if let State::ChannelOpen {
                            channel_id: current_channel_id,
                        } = self.state
                            && channel_id == current_channel_id
                        {
                            debug!("rtc channel closed, {:?}", channel_id);
                            self.state = State::Standby;
                            self.buffered_outputs.push_back(Output::ChannelClosed);
                        }
                    }
                    _ => trace!("received unprocessed event: {:?}", event),
                }
            }
        }
        if self.buffered_outputs.is_empty() {
            self.ready = false;
        }
        self.buffered_outputs
            .pop_front()
            .unwrap_or(Output::Timeout(self.last))
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::{Duration, Instant};
    use tracing::Level;

    struct TestRtc {
        rtc: RTCConnection,
        channel_data: Vec<u8>,
    }

    // copied and adjusted from str0m/tests/common.rs
    fn progress(l: &mut TestRtc, r: &mut TestRtc) {
        let (f, t) = if l.rtc.last < r.rtc.last {
            (l, r)
        } else {
            (r, l)
        };

        loop {
            f.rtc.handle_input(Receive::Rtc(Input::Timeout(f.rtc.last)));

            match f.rtc.poll_output() {
                Output::Timeout(v) => {
                    let tick = f.rtc.last + Duration::from_millis(10);
                    f.rtc.last = if v == f.rtc.last { tick } else { tick.min(v) };
                    break;
                }
                Output::RtcTransmit(v) => {
                    let data = v.contents;
                    let input = Input::Receive(
                        f.rtc.last,
                        str0m::net::Receive {
                            proto: v.proto,
                            source: v.source,
                            destination: v.destination,
                            contents: (&*data).try_into().unwrap(),
                        },
                    );
                    t.rtc.handle_input(Receive::Rtc(input));
                }
                Output::SdpTransmit(v) => {
                    t.rtc.handle_input(Receive::Sdp(v));
                }
                Output::ChannelData(mut d) => {
                    f.channel_data.append(&mut d);
                }
                _ => {}
            }
        }
    }

    #[test]
    fn rtc_connection_test_connected() {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(Level::DEBUG)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .map_err(|_err| eprintln!("Unable to set global default subscriber"))
            .unwrap();

        let local_addr_1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001);
        let local_addr_2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8002);

        let uuid_1 = Nanoid::new();
        let uuid_2 = Nanoid::new();

        // Create first RTCConnection instance
        let config_1 = RTCConnectionConfig {
            local_host_addr: vec![local_addr_1],
            data_channel_name: "test_channel".to_string(),
            local_uuid: uuid_1,
            start: Instant::now(),
        };
        let conn_1 = RTCConnection::new(config_1, uuid_2);

        // Create second RTCConnection instance
        let config_2 = RTCConnectionConfig {
            local_host_addr: vec![local_addr_2],
            data_channel_name: "test_channel".to_string(),
            local_uuid: uuid_2,
            start: Instant::now(),
        };
        let conn_2 = RTCConnection::new(config_2, uuid_1);

        let mut l = TestRtc {
            rtc: conn_1,
            channel_data: Vec::new(),
        };
        let mut r = TestRtc {
            rtc: conn_2,
            channel_data: Vec::new(),
        };
        l.rtc.connect();

        for _ in 0..100 {
            if l.rtc.rtc.is_connected() && r.rtc.rtc.is_connected() {
                break;
            }
            progress(&mut l, &mut r);
        }

        assert!(l.rtc.rtc.is_connected());
        assert!(r.rtc.rtc.is_connected());

        for _ in 0..100 {
            if matches!(l.rtc.state, State::ChannelOpen { channel_id: _ })
                && matches!(r.rtc.state, State::ChannelOpen { channel_id: _ })
            {
                break;
            }
            progress(&mut l, &mut r);
        }

        // Verify that both connections are still in ChannelOpen state after data sends
        assert!(matches!(l.rtc.state, State::ChannelOpen { channel_id: _ }));
        assert!(matches!(r.rtc.state, State::ChannelOpen { channel_id: _ }));

        // Verify that connections were created with correct configurations
        assert_eq!(l.rtc.config.local_uuid, uuid_1);
        assert_eq!(r.rtc.config.local_uuid, uuid_2);

        let max = l.rtc.last.max(r.rtc.last);
        l.rtc.last = max;
        r.rtc.last = max;

        // Send data from l to r
        let test_data = b"Hello from l to r!";
        l.rtc.send_data(test_data);

        // Progress the connections to process the data
        for _ in 0..100 {
            progress(&mut l, &mut r);
            if (l.rtc.last - l.rtc.config.start) > Duration::from_secs(10) {
                break;
            }
        }

        // Verify that r received the data
        assert_eq!(
            r.channel_data,
            test_data,
            "received data len {}, sent data len {}",
            r.channel_data.len(),
            test_data.len()
        );

        // Send data from r to l
        // Generate random data larger than 262144 bytes (256KB)
        let mut response_data = vec![0u8; 100_000]; // 300KB
        for (i, byte) in response_data.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }
        r.rtc.send_data(&response_data);

        // Progress the connections to process the response
        for _ in 0..100 {
            progress(&mut l, &mut r);
        }

        // Verify that l received the response data
        if l.channel_data != response_data {
            panic!(
                "len 1 {} len 2 {}",
                l.channel_data.len(),
                response_data.len()
            )
        }
        assert_eq!(
            l.channel_data,
            response_data,
            "received data len {}, sent data len {}",
            l.channel_data.len(),
            response_data.len()
        );
    }
}
