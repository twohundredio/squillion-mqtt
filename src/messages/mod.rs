pub mod codec;
mod length;
mod reader;
mod validation;

use length::encode_length;
use reader::Reader;
use serde::Serialize;
use std::convert::TryFrom;
use std::io::ErrorKind;
use validation::{client_id_valid, publish_topic_valid, qos_valid, topic_filter_valid};

/// Common trait for all message types that can be serialised onto the wire.
pub trait ToBytes {
    fn to_bytes(&self) -> Vec<u8>;
}

pub enum MQTTMessageType {
    Connect = 1,
    ConnAck = 2,
    Publish = 3,
    PubAck = 4,
    PubRec = 5,
    PubRel = 6,
    PubComp = 7,
    Subscribe = 8,
    SubAck = 9,
    Unsubscribe = 10,
    UnsubAck = 11,
    PingReq = 12,
    PingResp = 13,
    Disconnect = 14,
}

impl TryFrom<i32> for MQTTMessageType {
    type Error = ();

    fn try_from(v: i32) -> Result<Self, Self::Error> {
        match v {
            x if x == MQTTMessageType::Connect as i32 => Ok(MQTTMessageType::Connect),
            x if x == MQTTMessageType::ConnAck as i32 => Ok(MQTTMessageType::ConnAck),
            x if x == MQTTMessageType::Publish as i32 => Ok(MQTTMessageType::Publish),
            x if x == MQTTMessageType::PubAck as i32 => Ok(MQTTMessageType::PubAck),
            x if x == MQTTMessageType::PubRec as i32 => Ok(MQTTMessageType::PubRec),
            x if x == MQTTMessageType::PubRel as i32 => Ok(MQTTMessageType::PubRel),
            x if x == MQTTMessageType::PubComp as i32 => Ok(MQTTMessageType::PubComp),
            x if x == MQTTMessageType::Subscribe as i32 => Ok(MQTTMessageType::Subscribe),
            x if x == MQTTMessageType::SubAck as i32 => Ok(MQTTMessageType::SubAck),
            x if x == MQTTMessageType::Unsubscribe as i32 => Ok(MQTTMessageType::Unsubscribe),
            x if x == MQTTMessageType::UnsubAck as i32 => Ok(MQTTMessageType::UnsubAck),
            x if x == MQTTMessageType::PingReq as i32 => Ok(MQTTMessageType::PingReq),
            x if x == MQTTMessageType::PingResp as i32 => Ok(MQTTMessageType::PingResp),
            x if x == MQTTMessageType::Disconnect as i32 => Ok(MQTTMessageType::Disconnect),
            _ => Err(()),
        }
    }
}

pub enum MQTTQos {
    QOS0 = 0,
    QOS1 = 1,
    QOS2 = 2,
}

pub const MQTT311_VERSION: u8 = 4;

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum ReturnCode {
    Accepted = 0,
    UnacceptableVersion = 1,
    IdentifierRejected = 2,
    ServerUnavailable = 3,
    BadUsernameOrPassword = 4,
    NotAuthorized = 5,
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct MQTTMessageConnack {
    return_code: ReturnCode,
    session_present: bool,
}

impl MQTTMessageConnack {
    pub fn new() -> MQTTMessageConnack {
        MQTTMessageConnack {
            return_code: ReturnCode::Accepted,
            session_present: false,
        }
    }

    pub fn set_return_code(&mut self, return_code: ReturnCode) {
        self.return_code = return_code;
    }

    pub fn get_return_code(&self) -> ReturnCode {
        self.return_code
    }

    pub fn set_session_present(&mut self, session_present: bool) {
        self.session_present = session_present;
    }
}

impl ToBytes for MQTTMessageConnack {
    fn to_bytes(&self) -> Vec<u8> {
        // Type, length
        let mut encoded: Vec<u8> = vec![((MQTTMessageType::ConnAck as u8) << 4), 2];

        let mut flags: u8 = 0;
        if self.session_present {
            flags |= 1;
        }
        encoded.push(flags);
        encoded.push(self.return_code as u8);

        encoded
    }
}

#[derive(Clone, Serialize)]
pub struct MQTTMessagePublish {
    identifier: u16,
    topic: String,
    message: Vec<u8>,
    retain: bool,
    dup: bool,
    qos: u8,
}

impl MQTTMessagePublish {
    pub fn new() -> MQTTMessagePublish {
        MQTTMessagePublish {
            identifier: 0,
            topic: String::new(),
            message: Vec::new(),
            retain: false,
            dup: false,
            qos: MQTTQos::QOS0 as u8,
        }
    }

    pub fn get_dup(&self) -> bool {
        self.dup
    }

    pub fn set_dup(&mut self, dup: bool) {
        self.dup = dup;
    }

    pub fn get_retain(&self) -> bool {
        self.retain
    }

    pub fn set_retain(&mut self, retain: bool) {
        self.retain = retain;
    }

    pub fn set_qos(&mut self, qos: u8) {
        self.qos = qos;
    }

    pub fn get_qos(&self) -> u8 {
        self.qos
    }

    pub fn set_topic(&mut self, topic: String) {
        self.topic = topic;
    }

    pub fn get_topic(&self) -> &String {
        &self.topic
    }

    pub fn set_message(&mut self, message: Vec<u8>) {
        self.message = message;
    }

    pub fn get_message(&self) -> &Vec<u8> {
        &self.message
    }

    pub fn get_identifier(&self) -> u16 {
        self.identifier
    }

    pub fn set_identifier(&mut self, identifier: u16) {
        self.identifier = identifier;
    }

    pub fn from_bytes(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        let mut r = Reader::new(buf);

        // Fixed header byte: dup/qos/retain flags
        let fixed = r.read_u8()?;
        self.dup = (fixed & 0x8) == 0x8;
        self.qos = (fixed & 0x6) >> 1;
        if !qos_valid(self.qos) {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Publish message invalid qos",
            ));
        }
        if self.dup && self.qos == 0 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Publish message invalid dup set and qos 0",
            ));
        }
        self.retain = (fixed & 1) == 1;

        // Variable-length remaining-length field
        let (len, _lensize) = r.read_varint()?;

        // Topic (u16-prefixed string)
        let topiclen = r.read_u16()? as usize;
        let tb = r.read_bytes(topiclen)?.to_vec();
        if !publish_topic_valid(&tb) {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Publish message invalid topic",
            ));
        }
        self.topic = String::from_utf8(tb).map_err(|_| {
            std::io::Error::new(ErrorKind::InvalidData, "Publish invalid topic UTF-8")
        })?;

        // Packet identifier (QoS > 0 only)
        let id_size: usize;
        if self.qos > 0 {
            let id = r.read_u16()?;
            id_size = 2;
            if id == 0 {
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "Publish zero identifier",
                ));
            }
            self.identifier = id;
        } else {
            id_size = 0;
        }

        // Payload: everything after topic (+ optional id)
        let msglen = len.checked_sub(topiclen + id_size + 2).ok_or_else(|| {
            std::io::Error::new(ErrorKind::InvalidData, "Publish length underflow")
        })?;
        self.message = r.read_bytes(msglen)?.to_vec();

        r.expect_end("Publish message length incorrect")
    }
}

impl ToBytes for MQTTMessagePublish {
    fn to_bytes(&self) -> Vec<u8> {
        let mut encoded: Vec<u8> = Vec::new();

        let mut flags: u8 = (MQTTMessageType::Publish as u8) << 4;
        if self.retain {
            flags |= 1;
        }
        let mut id_size = 0;
        if self.qos > 0 {
            flags |= self.qos << 1;
            id_size = 2;
        }
        encoded.push(flags);

        let topiclen = self.topic.len();

        let len: usize = topiclen + self.message.len() + id_size + 2;
        encoded.append(encode_length(len).as_mut());

        // Topic length prefix (big-endian u16)
        encoded.push((topiclen >> 8) as u8);
        encoded.push((topiclen & 0xff) as u8);

        // Topic
        encoded.append(Vec::from(self.topic.as_bytes()).as_mut());

        // Packet identifier (QoS > 0 only)
        if self.qos > 0 {
            encoded.push((self.identifier >> 8) as u8);
            encoded.push((self.identifier & 0xff) as u8);
        }

        // Payload
        encoded.append(self.message.clone().as_mut());

        encoded
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct MQTTMessageSubscribe {
    identifier: u16,
    topics: Vec<(String, u8)>,
}

impl MQTTMessageSubscribe {
    pub fn new() -> MQTTMessageSubscribe {
        MQTTMessageSubscribe {
            identifier: 0,
            topics: Vec::new(),
        }
    }

    pub fn get_topics(&self) -> &Vec<(String, u8)> {
        &self.topics
    }

    pub fn get_identifier(&self) -> u16 {
        self.identifier
    }

    pub fn from_bytes(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        let mut r = Reader::new(buf);

        // Fixed header
        let fixed = r.read_u8()?;
        if (fixed & 0xf) != 0x2 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Subscribe invalid flags",
            ));
        }

        // Remaining-length varint (consumed but we rely on Reader::is_empty
        // rather than manual countdown to avoid signed-overflow bugs)
        let (_len, _lensize) = r.read_varint()?;

        // Packet identifier
        let id = r.read_u16()?;
        if id == 0 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Subscribe zero identifier",
            ));
        }
        self.identifier = id;

        // One or more topic-filter + QoS pairs
        while !r.is_empty() {
            let topic_length = r.read_u16()? as usize;
            let topic_bytes = r.read_bytes(topic_length)?.to_vec();
            if !topic_filter_valid(&topic_bytes) {
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "Subscribe invalid topic",
                ));
            }
            let topic = String::from_utf8(topic_bytes).map_err(|_| {
                std::io::Error::new(ErrorKind::InvalidData, "Subscribe invalid topic UTF-8")
            })?;

            let qos = r.read_u8()?;
            if !qos_valid(qos) {
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "Subscribe invalid qos",
                ));
            }

            self.topics.push((topic, qos));
        }

        if self.topics.is_empty() {
            return Err(std::io::Error::new(ErrorKind::Other, "Subscribe no topics"));
        }

        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct MQTTMessageSuback {
    identifier: u16,
    response: Vec<u8>,
}

impl MQTTMessageSuback {
    pub fn new() -> MQTTMessageSuback {
        MQTTMessageSuback {
            identifier: 0,
            response: Vec::new(),
        }
    }

    pub fn set_identifier(&mut self, id: u16) {
        self.identifier = id;
    }

    pub fn add_response(&mut self, response: u8) {
        self.response.push(response);
    }
}

impl ToBytes for MQTTMessageSuback {
    fn to_bytes(&self) -> Vec<u8> {
        let mut encoded: Vec<u8> = vec![((MQTTMessageType::SubAck as u8) << 4)];

        // Length
        let len: usize = 2 + self.response.len();
        encoded.append(encode_length(len).as_mut());

        // Packet ID
        encoded.push((self.identifier >> 8) as u8);
        encoded.push((self.identifier & 0xff) as u8);

        // Return codes
        for rc in self.response.iter() {
            encoded.push(*rc);
        }

        encoded
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct MQTTMessagePingResp {}

impl MQTTMessagePingResp {
    pub fn new() -> MQTTMessagePingResp {
        MQTTMessagePingResp {}
    }
}

impl ToBytes for MQTTMessagePingResp {
    fn to_bytes(&self) -> Vec<u8> {
        // Type, length (0 remaining bytes)
        vec![((MQTTMessageType::PingResp as u8) << 4), 0]
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct MQTTMessagePingReq {}

impl MQTTMessagePingReq {
    pub fn new() -> MQTTMessagePingReq {
        MQTTMessagePingReq {}
    }

    pub fn from_bytes(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        let mut r = Reader::new(buf);

        // Fixed header
        let fixed = r.read_u8()?;
        if (fixed & 0xf) != 0x0 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Pingreq invalid flags",
            ));
        }

        // Remaining-length varint (must encode 0 for a valid PINGREQ)
        let (_len, _lensize) = r.read_varint()?;

        r.expect_end("Pingreq message length incorrect")
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct MQTTMessageDisconnect {}

impl MQTTMessageDisconnect {
    pub fn new() -> MQTTMessageDisconnect {
        MQTTMessageDisconnect {}
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct MQTTMessageUnsubscribe {
    identifier: u16,
    topics: Vec<String>,
}

impl MQTTMessageUnsubscribe {
    pub fn new() -> MQTTMessageUnsubscribe {
        MQTTMessageUnsubscribe {
            identifier: 0,
            topics: Vec::new(),
        }
    }

    pub fn get_topics(&self) -> &Vec<String> {
        &self.topics
    }

    pub fn get_identifier(&self) -> u16 {
        self.identifier
    }

    pub fn from_bytes(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        let mut r = Reader::new(buf);

        // Fixed header
        let fixed = r.read_u8()?;
        if (fixed & 0xf) != 0x2 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Unsubscribe invalid flags",
            ));
        }

        // Remaining-length varint
        let (_len, _lensize) = r.read_varint()?;

        // Packet identifier
        let id = r.read_u16()?;
        self.identifier = id;

        // One or more topic filters
        while !r.is_empty() {
            let topic_length = r.read_u16()? as usize;
            let topic_bytes = r.read_bytes(topic_length)?.to_vec();
            if !topic_filter_valid(&topic_bytes) {
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "Unsubscribe invalid topic",
                ));
            }
            let topic = String::from_utf8(topic_bytes).map_err(|_| {
                std::io::Error::new(ErrorKind::InvalidData, "Unsubscribe invalid topic UTF-8")
            })?;
            self.topics.push(topic);
        }

        if self.topics.is_empty() {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Unsubscribe no topics",
            ));
        }

        Ok(())
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct MQTTMessageUnsuback {
    identifier: u16,
}

impl MQTTMessageUnsuback {
    pub fn new() -> MQTTMessageUnsuback {
        MQTTMessageUnsuback { identifier: 0 }
    }

    pub fn set_identifier(&mut self, id: u16) {
        self.identifier = id;
    }
}

impl ToBytes for MQTTMessageUnsuback {
    fn to_bytes(&self) -> Vec<u8> {
        // Type, remaining length (2), packet id
        vec![
            ((MQTTMessageType::UnsubAck as u8) << 4),
            2,
            ((self.identifier >> 8) as u8),
            ((self.identifier & 0xff) as u8),
        ]
    }
}

#[derive(Clone, Serialize)]
pub struct MQTTMessageConnect {
    client: String,
    username: String,
    password: String,
    will: bool,
    willtopic: String,
    willmsg: String,
    willqos: u8,
    willretain: bool,
    version: u8,
    keepalive: u16,
    clean_session: bool,
}

impl MQTTMessageConnect {
    pub fn new() -> MQTTMessageConnect {
        MQTTMessageConnect {
            client: String::new(),
            username: String::new(),
            password: String::new(),
            will: false,
            willtopic: String::new(),
            willmsg: String::new(),
            willqos: 0,
            willretain: false,
            version: 0,
            keepalive: 0,
            clean_session: false,
        }
    }

    pub fn get_client(&self) -> &String {
        &self.client
    }

    pub fn get_username(&self) -> &String {
        &self.username
    }

    pub fn get_password(&self) -> &String {
        &self.password
    }

    pub fn has_clean_session(&self) -> bool {
        self.clean_session
    }

    pub fn has_will(&self) -> bool {
        self.will
    }

    pub fn will_topic(&self) -> &String {
        &self.willtopic
    }

    pub fn will_message(&self) -> &String {
        &self.willmsg
    }

    pub fn will_qos(&self) -> u8 {
        self.willqos
    }

    pub fn will_retain(&self) -> bool {
        self.willretain
    }

    pub fn keep_alive(&self) -> u16 {
        self.keepalive
    }

    pub fn get_version(&self) -> u8 {
        self.version
    }

    pub fn from_bytes(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        let mut r = Reader::new(buf);

        // Fixed header: type (Connect=1) in top 4 bits, flags must be 0
        let fixed = r.read_u8()?;
        if (fixed & 0x0F) != 0 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Connect invalid header flags",
            ));
        }

        // Remaining-length varint
        let (_len, _lensize) = r.read_varint()?;

        // Protocol name: 2-byte length + "MQTT" (4 bytes) = 6 bytes total
        r.skip(6)?;

        // Protocol version
        self.version = r.read_u8()?;

        // Connect flags
        let flags = r.read_u8()?;
        if flags & 0x01 == 0x01 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Connect message invalid flags",
            ));
        }
        self.clean_session = (flags & 0x02) == 0x02;

        // Keep-alive (seconds)
        self.keepalive = r.read_u16()?;

        // Client ID
        let client_len = r.read_u16()? as usize;
        let client_id_bytes = r.read_bytes(client_len)?.to_vec();
        if !client_id_valid(&client_id_bytes) {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Connect message invalid client id",
            ));
        }
        self.client = String::from_utf8(client_id_bytes).map_err(|_| {
            std::io::Error::new(ErrorKind::InvalidData, "Connect client id invalid UTF-8")
        })?;

        // Will
        let willretain = (flags & 0x20) == 0x20;
        let willqos = (flags & 0x18) >> 3;
        if flags & 0x04 == 0x04 {
            self.will = true;
            if !qos_valid(willqos) {
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "Connect message invalid qos",
                ));
            }
            self.willqos = willqos;
            self.willretain = willretain;

            let willtopic_len = r.read_u16()? as usize;
            let willtopicbytes = r.read_bytes(willtopic_len)?.to_vec();
            if !publish_topic_valid(&willtopicbytes) {
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "Connect message invalid will topic",
                ));
            }
            self.willtopic = String::from_utf8(willtopicbytes).map_err(|_| {
                std::io::Error::new(ErrorKind::InvalidData, "Connect will topic invalid UTF-8")
            })?;

            let willmsg_len = r.read_u16()? as usize;
            let willmsg_bytes = r.read_bytes(willmsg_len)?;
            self.willmsg = String::from_utf8(willmsg_bytes.to_vec()).map_err(|_| {
                std::io::Error::new(ErrorKind::InvalidData, "Connect will message invalid UTF-8")
            })?;
        } else if willqos != 0 || willretain {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Connect message invalid will flags",
            ));
        }

        // Username
        if flags & 0x80 == 0x80 {
            self.username = r.read_mqtt_string_prefixed()?;
        }

        // Password
        if flags & 0x40 == 0x40 {
            self.password = r.read_mqtt_string_prefixed()?;
        }

        r.expect_end("Connect message length incorrect")
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct MQTTMessagePuback {
    identifier: u16,
}

impl MQTTMessagePuback {
    pub fn new() -> MQTTMessagePuback {
        MQTTMessagePuback { identifier: 0 }
    }

    pub fn set_identifier(&mut self, id: u16) {
        self.identifier = id;
    }

    pub fn get_identifier(&self) -> u16 {
        self.identifier
    }

    pub fn from_bytes(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        let mut r = Reader::new(buf);

        let fixed = r.read_u8()?;
        if (fixed & 0xf) != 0x0 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Puback invalid flags",
            ));
        }

        let (len, _lensize) = r.read_varint()?;
        if len != 2 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Puback invalid length",
            ));
        }

        self.identifier = r.read_u16()?;
        r.expect_end("Puback message length incorrect")
    }
}

impl ToBytes for MQTTMessagePuback {
    fn to_bytes(&self) -> Vec<u8> {
        // Type, remaining length (2), packet id
        vec![
            ((MQTTMessageType::PubAck as u8) << 4),
            2,
            ((self.identifier >> 8) as u8),
            ((self.identifier & 0xff) as u8),
        ]
    }
}

#[derive(Copy, Clone, Serialize)]
pub struct MQTTMessagePubrec {
    identifier: u16,
}

impl MQTTMessagePubrec {
    pub fn new() -> MQTTMessagePubrec {
        MQTTMessagePubrec { identifier: 0 }
    }

    pub fn set_identifier(&mut self, id: u16) {
        self.identifier = id;
    }

    pub fn get_identifier(&self) -> u16 {
        self.identifier
    }

    pub fn from_bytes(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        let mut r = Reader::new(buf);

        let fixed = r.read_u8()?;
        if (fixed & 0xf) != 0x0 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Pubrec invalid flags",
            ));
        }

        let (len, _lensize) = r.read_varint()?;
        if len != 2 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Pubrec invalid length",
            ));
        }

        self.identifier = r.read_u16()?;
        r.expect_end("Pubrec message length incorrect")
    }
}

impl ToBytes for MQTTMessagePubrec {
    fn to_bytes(&self) -> Vec<u8> {
        // Type, remaining length (2), packet id
        vec![
            ((MQTTMessageType::PubRec as u8) << 4),
            2,
            ((self.identifier >> 8) as u8),
            ((self.identifier & 0xff) as u8),
        ]
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct MQTTMessagePubrel {
    identifier: u16,
}

impl MQTTMessagePubrel {
    pub fn new() -> MQTTMessagePubrel {
        MQTTMessagePubrel { identifier: 0 }
    }

    pub fn set_identifier(&mut self, id: u16) {
        self.identifier = id;
    }

    pub fn get_identifier(&self) -> u16 {
        self.identifier
    }

    pub fn from_bytes(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        let mut r = Reader::new(buf);

        let fixed = r.read_u8()?;
        if (fixed & 0xf) != 0x2 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Pubrel invalid flags",
            ));
        }

        let (len, _lensize) = r.read_varint()?;
        if len != 2 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Pubrel invalid length",
            ));
        }

        self.identifier = r.read_u16()?;
        r.expect_end("Pubrel message length incorrect")
    }
}

impl ToBytes for MQTTMessagePubrel {
    fn to_bytes(&self) -> Vec<u8> {
        // PUBREL requires fixed-header flags = 0b0010 per MQTT 3.1.1 §3.6.1
        vec![
            ((MQTTMessageType::PubRel as u8) << 4) | 0x2,
            2,
            ((self.identifier >> 8) as u8),
            ((self.identifier & 0xff) as u8),
        ]
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct MQTTMessagePubcomp {
    identifier: u16,
}

impl MQTTMessagePubcomp {
    pub fn new() -> MQTTMessagePubcomp {
        MQTTMessagePubcomp { identifier: 0 }
    }

    pub fn set_identifier(&mut self, id: u16) {
        self.identifier = id;
    }

    pub fn get_identifier(&self) -> u16 {
        self.identifier
    }

    pub fn from_bytes(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        let mut r = Reader::new(buf);

        let fixed = r.read_u8()?;
        if (fixed & 0xf) != 0x0 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Pubcomp invalid flags",
            ));
        }

        let (len, _lensize) = r.read_varint()?;
        if len != 2 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Pubcomp invalid length",
            ));
        }

        self.identifier = r.read_u16()?;
        r.expect_end("Pubcomp message length incorrect")
    }
}

impl ToBytes for MQTTMessagePubcomp {
    fn to_bytes(&self) -> Vec<u8> {
        // Type, remaining length (2), packet id
        vec![
            ((MQTTMessageType::PubComp as u8) << 4),
            2,
            ((self.identifier >> 8) as u8),
            ((self.identifier & 0xff) as u8),
        ]
    }
}
