use bytes::Bytes;
use bytes::{BufMut, BytesMut};

use std::io::ErrorKind;
use tokio_util::codec::{Decoder, Encoder};

use super::length;
use super::length::read_size_check;
use crate::messages::MQTTMessageConnack;
use crate::messages::MQTTMessageConnect;
use crate::messages::MQTTMessageDisconnect;
use crate::messages::MQTTMessagePingReq;
use crate::messages::MQTTMessagePingResp;
use crate::messages::MQTTMessagePuback;
use crate::messages::MQTTMessagePubcomp;
use crate::messages::MQTTMessagePublish;
use crate::messages::MQTTMessagePubrec;
use crate::messages::MQTTMessagePubrel;
use crate::messages::MQTTMessageSuback;
use crate::messages::MQTTMessageSubscribe;
use crate::messages::MQTTMessageType;
use crate::messages::MQTTMessageUnsuback;
use crate::messages::MQTTMessageUnsubscribe;
use crate::messages::ToBytes;

#[derive(Clone)]
pub enum MQTTMessage {
    Connect(MQTTMessageConnect),
    ConnAck(MQTTMessageConnack),
    Publish(MQTTMessagePublish),
    PubAck(MQTTMessagePuback),
    PubRec(MQTTMessagePubrec),
    PubRel(MQTTMessagePubrel),
    PubComp(MQTTMessagePubcomp),
    Subscribe(MQTTMessageSubscribe),
    SubAck(MQTTMessageSuback),
    Unsubscribe(MQTTMessageUnsubscribe),
    UnsubAck(MQTTMessageUnsuback),
    PingReq(MQTTMessagePingReq),
    PingResp(MQTTMessagePingResp),
    Disconnect(MQTTMessageDisconnect),
}

pub struct MqttCodec {
    logger: slog::Logger,
    /// Maximum permitted remaining-length.  Any packet claiming a larger size
    /// is rejected with `InvalidData` before `buf.reserve` is called, preventing
    /// unauthenticated bulk memory allocation.
    max_packet_size: usize,
}

impl MqttCodec {
    pub fn new(logger: slog::Logger, max_packet_size: usize) -> MqttCodec {
        MqttCodec {
            logger,
            max_packet_size,
        }
    }
}

impl Encoder<MQTTMessage> for MqttCodec {
    type Error = std::io::Error;

    fn encode(&mut self, message: MQTTMessage, buf: &mut BytesMut) -> Result<(), Self::Error> {
        // Call to_bytes() on each concrete type (monomorphized — enables inlining
        // and allocation elision for small fixed-size messages on the hot path).
        // Client→server-only variants are not expected here; they fall through to
        // the error log.
        let bytes: Option<Vec<u8>> = match &message {
            MQTTMessage::Publish(m) => Some(m.to_bytes()),
            MQTTMessage::ConnAck(m) => Some(m.to_bytes()),
            MQTTMessage::SubAck(m) => Some(m.to_bytes()),
            MQTTMessage::UnsubAck(m) => Some(m.to_bytes()),
            MQTTMessage::PingResp(m) => Some(m.to_bytes()),
            MQTTMessage::PubAck(m) => Some(m.to_bytes()),
            MQTTMessage::PubRec(m) => Some(m.to_bytes()),
            MQTTMessage::PubRel(m) => Some(m.to_bytes()),
            MQTTMessage::PubComp(m) => Some(m.to_bytes()),
            _ => None,
        };

        match bytes {
            Some(b) => {
                buf.reserve(b.len());
                buf.put(Bytes::from(b));
            }
            None => slog::error!(self.logger, "Unknown msg to send"),
        }

        Ok(())
    }
}

impl Decoder for MqttCodec {
    type Item = MQTTMessage;
    type Error = std::io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.is_empty() {
            return Ok(None);
        }

        let msgtype = buf[0] >> 4;

        // Read the variable-length remaining-length field (bytes 1..).
        let (len, lensize) = match read_size_check(&buf[1..]) {
            Ok(Some(v)) => v,
            Ok(None) => return Ok(None), // need more data
            Err(()) => {
                slog::warn!(self.logger, "Decoder: Invalid message length");
                return Err(std::io::Error::new(
                    ErrorKind::InvalidData,
                    "Invalid message length",
                ));
            }
        };

        // Reject packets exceeding the configured limit *before* reserving
        // memory, so an unauthenticated client cannot force large allocations.
        if len > self.max_packet_size {
            slog::warn!(
                self.logger,
                "Decoder: packet remaining-length {} exceeds max_packet_size {}",
                len,
                self.max_packet_size,
            );
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "Decoder: packet exceeds maximum allowed size",
            ));
        }
        // Belt-and-suspenders: also enforce the absolute MQTT protocol cap.
        if len > length::MAX_LENGTH {
            slog::warn!(
                self.logger,
                "Decoder: packet exceeds MQTT protocol max length"
            );
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "Decoder: packet exceeds MQTT protocol max length",
            ));
        }

        // Total frame = fixed-header byte (1) + length-field bytes + remaining length.
        let frame_len = 1 + lensize + len;

        if buf.len() < frame_len {
            buf.reserve(frame_len - buf.len());
            return Ok(None);
        }

        let msg = buf.split_to(frame_len);

        // Reduce per-type boilerplate: construct, parse, log on error.
        macro_rules! decode_msg {
            ($variant:ident, $ty:ty, $name:literal) => {{
                let mut m = <$ty>::new();
                match m.from_bytes(&msg) {
                    Ok(_) => Ok(Some(MQTTMessage::$variant(m))),
                    Err(err) => {
                        slog::warn!(self.logger, concat!($name, " decode: {}"), err);
                        Err(err)
                    }
                }
            }};
        }

        use std::convert::TryFrom;
        match MQTTMessageType::try_from(msgtype as i32) {
            Ok(MQTTMessageType::Connect) => decode_msg!(Connect, MQTTMessageConnect, "CONNECT"),
            Ok(MQTTMessageType::Subscribe) => {
                decode_msg!(Subscribe, MQTTMessageSubscribe, "SUBSCRIBE")
            }
            Ok(MQTTMessageType::Unsubscribe) => {
                decode_msg!(Unsubscribe, MQTTMessageUnsubscribe, "UNSUBSCRIBE")
            }
            Ok(MQTTMessageType::Publish) => decode_msg!(Publish, MQTTMessagePublish, "PUBLISH"),
            Ok(MQTTMessageType::PingReq) => decode_msg!(PingReq, MQTTMessagePingReq, "PINGREQ"),
            Ok(MQTTMessageType::PubAck) => decode_msg!(PubAck, MQTTMessagePuback, "PUBACK"),
            Ok(MQTTMessageType::PubRec) => decode_msg!(PubRec, MQTTMessagePubrec, "PUBREC"),
            Ok(MQTTMessageType::PubRel) => decode_msg!(PubRel, MQTTMessagePubrel, "PUBREL"),
            Ok(MQTTMessageType::PubComp) => decode_msg!(PubComp, MQTTMessagePubcomp, "PUBCOMP"),
            Ok(MQTTMessageType::Disconnect) => {
                Ok(Some(MQTTMessage::Disconnect(MQTTMessageDisconnect {})))
            }
            // Server→client-only types (ConnAck, SubAck, UnsubAck, PingResp) and
            // any unknown control-packet type are rejected.
            Ok(_) | Err(()) => {
                slog::warn!(self.logger, "Decoder: Unknown msg received");
                Err(std::io::Error::new(
                    ErrorKind::InvalidData,
                    "Unknown msg received",
                ))
            }
        }
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.decode(buf)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::{MQTTMessagePublish, ToBytes};
    use tokio_util::codec::Decoder;

    fn discard_logger() -> slog::Logger {
        slog::Logger::root(slog::Discard, slog::o!())
    }

    fn make_codec() -> MqttCodec {
        MqttCodec::new(discard_logger(), 1_048_576)
    }

    fn make_codec_max(max: usize) -> MqttCodec {
        MqttCodec::new(discard_logger(), max)
    }

    /// Feed `data` into the decoder repeatedly until it returns Ok(None) or
    /// Err.  The only thing we assert is that it does NOT panic.
    fn decode_no_panic(data: &[u8]) {
        let mut codec = make_codec();
        let mut buf = BytesMut::from(data);
        loop {
            match codec.decode(&mut buf) {
                Ok(None) => break,
                Ok(Some(_)) => {
                    if buf.is_empty() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    }

    // ── round-trip helpers ────────────────────────────────────────────────────

    /// Encode a PUBLISH via `to_bytes` then decode it; returns the decoded
    /// message or None if decoding did not produce a complete message.
    fn roundtrip_publish(
        topic: &str,
        payload: &[u8],
        qos: u8,
        retain: bool,
    ) -> Option<MQTTMessagePublish> {
        let mut msg = MQTTMessagePublish::new();
        msg.set_topic(topic.to_string());
        msg.set_message(payload.to_vec());
        msg.set_qos(qos);
        msg.set_retain(retain);
        if qos > 0 {
            msg.set_identifier(42);
        }

        let encoded = msg.to_bytes();
        let mut codec = make_codec();
        let mut buf = BytesMut::from(encoded.as_slice());
        match codec.decode(&mut buf) {
            Ok(Some(MQTTMessage::Publish(m))) => Some(m),
            _ => None,
        }
    }

    // ── Task 3: to_bytes topic-length fix ─────────────────────────────────────

    #[test]
    fn roundtrip_short_topic() {
        let d = roundtrip_publish("sensor/temp", b"23.5", 0, false).unwrap();
        assert_eq!(d.get_topic(), "sensor/temp");
        assert_eq!(d.get_message(), b"23.5");
    }

    /// Topic exactly 256 bytes — the old `<< 8` bug always produced 0 here.
    #[test]
    fn roundtrip_256_byte_topic() {
        let topic = "a".repeat(256);
        let d = roundtrip_publish(&topic, b"payload", 0, false)
            .expect("decode failed for 256-byte topic");
        assert_eq!(d.get_topic().as_str(), topic.as_str());
    }

    /// Topic 300 bytes.
    #[test]
    fn roundtrip_300_byte_topic() {
        let topic = "x/".repeat(150);
        let d =
            roundtrip_publish(&topic, b"data", 0, false).expect("decode failed for 300-byte topic");
        assert_eq!(d.get_topic().as_str(), topic.as_str());
    }

    #[test]
    fn roundtrip_qos1_preserves_identifier() {
        let d = roundtrip_publish("t/qos1", b"hello", 1, false).unwrap();
        assert_eq!(d.get_qos(), 1);
        assert_eq!(d.get_identifier(), 42);
    }

    #[test]
    fn roundtrip_retain_flag() {
        let d = roundtrip_publish("t/retain", b"sticky", 0, true).unwrap();
        assert!(d.get_retain());
    }

    // ── Task 2: max_packet_size cap (pre-auth allocation) ─────────────────────

    fn encode_varint(mut v: usize) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let mut byte = (v & 0x7F) as u8;
            v >>= 7;
            if v > 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if v == 0 {
                break;
            }
        }
        out
    }

    /// A packet claiming one byte over the limit must be rejected (Err), not
    /// accepted or cause a large allocation.
    #[test]
    fn oversized_packet_rejected() {
        let max = 128usize;
        let mut codec = make_codec_max(max);
        let claimed = max + 1;
        let mut frame = vec![0x30u8]; // PUBLISH header
        frame.extend_from_slice(&encode_varint(claimed));
        frame.extend_from_slice(&[0u8; 4]); // tiny payload, far less than claimed
        let mut buf = BytesMut::from(frame.as_slice());
        assert!(
            codec.decode(&mut buf).is_err(),
            "expected Err for oversized packet"
        );
    }

    /// A packet claiming exactly the limit must not be rejected by the size check
    /// (it may return Ok(None) if the payload hasn't arrived yet).
    #[test]
    fn at_limit_packet_not_rejected() {
        let max = 128usize;
        let mut codec = make_codec_max(max);
        let claimed = max;
        let mut frame = vec![0x30u8];
        frame.extend_from_slice(&encode_varint(claimed));
        // partial payload only — triggers "need more data" path
        let mut buf = BytesMut::from(frame.as_slice());
        let result = codec.decode(&mut buf);
        assert!(
            matches!(result, Ok(None)),
            "expected Ok(None) for partial at-limit frame, got error"
        );
    }
}
