pub mod connection;
pub mod daemon;
pub mod offloader;
pub mod provider;
pub mod scheduler;
pub mod task;

mod protobuf_gen {
    include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));
}

#[cfg(test)]
mod test {
    use std::{net::SocketAddr, str::FromStr};

    use nid::Nanoid;
    use str0m::{Candidate, net::Protocol};

    use crate::connection::rtc_connection::{Sdp, SdpMessage};

    #[test]
    fn test_candidate_serde_de() {
        let sdp_string = r#"{"source":"5EM8N6BN9_Fuom8B-lP0C","destination":"_8Ca3Cqgd1ROkLj1p5-Ii","msg":{"Candidate":{"candidate":"candidate:4 1 UDP 1686109439 94.134.111.11 23154 typ srflx raddr 0.0.0.0 rport 0","sdpMLineIndex":null,"sdpMid":"7gC","usernameFragment":null}}}"#;
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
