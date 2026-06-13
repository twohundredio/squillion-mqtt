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
}

impl MqttCodec {
    pub fn new(logger: slog::Logger) -> MqttCodec {
        MqttCodec { logger }
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

        if len > length::MAX_LENGTH {
            slog::warn!(self.logger, "Message exceeds max length");
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "Decoder: Message exceeds max length",
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
