pub mod codec;
mod length;
mod validation;

use length::encode_length;
use length::read_size;
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
        let mut idx: usize = 0;

        self.dup = (buf[0] & 0x8) == 0x8;
        self.qos = (buf[0] & 0x6) >> 1;
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
        self.retain = (buf[0] & 1) == 1;

        idx += 1;

        let (len, lensize) = read_size(&buf[1..]);
        idx += lensize;

        let topiclen: u16 = ((buf[idx] as u16) << 8) | (buf[idx + 1] as u16);
        idx += 2;

        let topicstart = idx as usize;
        let topicend = topicstart + topiclen as usize;
        let tb = (buf[topicstart..topicend]).to_vec();
        if !publish_topic_valid(&tb) {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Publish message invalid topic",
            ));
        }
        let topic = String::from_utf8(tb).unwrap();
        self.topic = topic;
        idx += topiclen as usize;

        let mut id_size = 0;
        if self.qos > 0 {
            let id: u16 = ((buf[idx] as u16) << 8) | (buf[idx + 1] as u16);
            idx += 2;
            id_size = 2;
            if id == 0 {
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "Publish zero identifier",
                ));
            }
            self.identifier = id;
        }

        let msglen: u32 = (len as u32) - (topiclen as u32) - (id_size as u32) - 2;
        let msgstart = idx;
        let msgend = msgstart + msglen as usize;
        let message = (buf[msgstart..msgend]).to_vec();
        self.message = message;
        idx += msglen as usize;

        if idx != buf.len() {
            Err(std::io::Error::new(
                ErrorKind::Other,
                "Publish message length incorrect",
            ))
        } else {
            Ok(())
        }
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

        // Topic length prefix
        encoded.push((topiclen << 8) as u8);
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
        let mut idx: usize = 0;

        // Check flags
        if (buf[idx] & 0xf) != 0x2 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Subscribe invalid flags",
            ));
        }
        idx += 1;

        let (mut len, lensize) = read_size(&buf[1..]);
        idx += lensize;

        let id: u16 = ((buf[idx] as u16) << 8) | (buf[idx + 1] as u16);
        idx += 2;
        len -= 2;
        if id == 0 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Subscribe zero identifier",
            ));
        }
        self.identifier = id;

        while len > 0 {
            let topic_length: u16 = ((buf[idx] as u16) << 8) | (buf[idx + 1] as u16);
            idx += 2;

            let topicend: usize = idx + topic_length as usize;
            let topic_bytes = (buf[idx..topicend]).to_vec();
            if !topic_filter_valid(&topic_bytes) {
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "Subscribe invalid topic",
                ));
            }
            let topic = String::from_utf8(topic_bytes).unwrap();
            idx += topic_length as usize;

            let qos = buf[idx as usize];
            idx += 1;
            if !qos_valid(qos) {
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "Subscribe invalid qos",
                ));
            }

            len = len - 3 - topic_length as usize;

            self.topics.push((topic, qos));
        }

        if self.topics.is_empty() {
            return Err(std::io::Error::new(ErrorKind::Other, "Subscribe no topics"));
        }

        if idx != buf.len() {
            Err(std::io::Error::new(
                ErrorKind::Other,
                "Subscribe message length incorrect",
            ))
        } else {
            Ok(())
        }
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
        let mut idx: usize = 0;

        // Check flags
        if (buf[idx] & 0xf) != 0x0 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Pingreq invalid flags",
            ));
        }
        idx += 1;

        let (_len, lensize) = read_size(&buf[1..]);
        idx += lensize;

        if idx != buf.len() {
            Err(std::io::Error::new(
                ErrorKind::Other,
                "Pingreq message length incorrect",
            ))
        } else {
            Ok(())
        }
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
        let mut idx: usize = 0;

        // Check flags
        if (buf[idx] & 0xf) != 0x2 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Unsubscribe invalid flags",
            ));
        }
        idx += 1;

        let (mut len, lensize) = read_size(&buf[1..]);
        idx += lensize;

        let id: u16 = ((buf[idx] as u16) << 8) | (buf[idx + 1] as u16);
        idx += 2;
        len -= 2;
        self.identifier = id;

        while len > 0 {
            let topic_length: u16 = ((buf[idx] as u16) << 8) | (buf[idx + 1] as u16);
            idx += 2;

            let topicend: usize = idx + topic_length as usize;
            let topic_bytes = (buf[idx..topicend]).to_vec();
            if !topic_filter_valid(&topic_bytes) {
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "Unsubscribe invalid topic",
                ));
            }
            let topic = String::from_utf8(topic_bytes).unwrap();
            idx += topic_length as usize;

            len = len - 2 - topic_length as usize;

            self.topics.push(topic);
        }

        if self.topics.is_empty() {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Unsubscribe no topics",
            ));
        }

        if idx != buf.len() {
            Err(std::io::Error::new(
                ErrorKind::Other,
                "Unsubscribe message length incorrect",
            ))
        } else {
            Ok(())
        }
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
        let mut idx: usize = 0;

        let flags = buf[idx] & 0x0F;
        if flags != 0 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Connect invalid header flags",
            ));
        }
        idx += 1;

        let (_len, lensize) = read_size(&buf[1..]);
        idx += lensize;

        // Skip header
        idx += 6;

        let version = buf[idx];
        self.version = version;
        idx += 1;

        let flags = buf[idx];
        idx += 1;

        // Clean session
        if flags & 0x02 == 0x02 {
            self.clean_session = true;
        }

        // Keep alive
        let keepalive = ((buf[idx] as u16) << 8) | (buf[idx + 1] as u16);
        self.keepalive = keepalive;
        idx += 2;

        // Client ID
        let client_len = ((buf[idx] as usize) << 8) | (buf[idx + 1] as usize);
        idx += 2;
        let client_id_bytes = (buf[idx..idx + client_len]).to_vec();
        if !client_id_valid(&client_id_bytes) {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Connect message invalid client id",
            ));
        }
        let client = String::from_utf8(client_id_bytes).unwrap();
        self.client = client;
        idx += client_len;

        // Will flag
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

            let willtopic_len = ((buf[idx] as usize) << 8) | (buf[idx + 1] as usize);
            idx += 2;

            let willtopicbytes = (buf[idx..idx + willtopic_len]).to_vec();
            if !publish_topic_valid(&willtopicbytes) {
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "Connect message invalid will topic",
                ));
            }
            let willtopic = String::from_utf8(willtopicbytes).unwrap();
            self.willtopic = willtopic;

            idx += willtopic_len;

            let willmsg_len = ((buf[idx] as usize) << 8) | (buf[idx + 1] as usize);
            idx += 2;

            let willmsg = String::from_utf8((buf[idx..idx + willmsg_len]).to_vec()).unwrap();
            self.willmsg = willmsg;

            idx += willmsg_len;
        } else if (willqos != 0) || willretain {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Connect message invalid will flags",
            ));
        }

        // Username
        if flags & 0x80 == 0x80 {
            let username_len = ((buf[idx] as usize) << 8) | (buf[idx + 1] as usize);
            idx += 2;

            let username = String::from_utf8((buf[idx..idx + username_len]).to_vec()).unwrap();
            self.username = username;

            idx += username_len;
        }

        // Password
        if flags & 0x40 == 0x40 {
            let password_len = ((buf[idx] as usize) << 8) | (buf[idx + 1] as usize);
            idx += 2;

            let password = String::from_utf8((buf[idx..idx + password_len]).to_vec()).unwrap();
            self.password = password;

            idx += password_len;
        }

        if flags & 0x01 == 0x01 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Connect message invalid flags",
            ));
        }

        if idx != buf.len() {
            Err(std::io::Error::new(
                ErrorKind::Other,
                "Connect message length incorrect",
            ))
        } else {
            Ok(())
        }
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
        let mut idx: usize = 0;

        // Check flags
        if (buf[idx] & 0xf) != 0x0 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Puback invalid flags",
            ));
        }
        idx += 1;

        let (len, lensize) = read_size(&buf[1..]);
        idx += lensize;
        if len != 2 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Puback invalid length",
            ));
        }

        let id: u16 = ((buf[idx] as u16) << 8) | (buf[idx + 1] as u16);
        idx += 2;
        self.identifier = id;

        if idx != buf.len() {
            Err(std::io::Error::new(
                ErrorKind::Other,
                "Puback message length incorrect",
            ))
        } else {
            Ok(())
        }
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
        let mut idx: usize = 0;

        // Check flags
        if (buf[idx] & 0xf) != 0x0 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Pubrec invalid flags",
            ));
        }
        idx += 1;

        let (len, lensize) = read_size(&buf[1..]);
        idx += lensize;
        if len != 2 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Pubrec invalid length",
            ));
        }

        let id: u16 = ((buf[idx] as u16) << 8) | (buf[idx + 1] as u16);
        idx += 2;
        self.identifier = id;

        if idx != buf.len() {
            Err(std::io::Error::new(
                ErrorKind::Other,
                "Pubrec message length incorrect",
            ))
        } else {
            Ok(())
        }
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
        let mut idx: usize = 0;

        // Check flags
        if (buf[idx] & 0xf) != 0x2 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Pubrel invalid flags",
            ));
        }
        idx += 1;

        let (len, lensize) = read_size(&buf[1..]);
        idx += lensize;
        if len != 2 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Pubrel invalid length",
            ));
        }

        let id: u16 = ((buf[idx] as u16) << 8) | (buf[idx + 1] as u16);
        idx += 2;
        self.identifier = id;

        if idx != buf.len() {
            Err(std::io::Error::new(
                ErrorKind::Other,
                "Pubrel message length incorrect",
            ))
        } else {
            Ok(())
        }
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
        let mut idx: usize = 0;

        // Check flags
        if (buf[idx] & 0xf) != 0x0 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Pubcomp invalid flags",
            ));
        }
        idx += 1;

        let (len, lensize) = read_size(&buf[1..]);
        idx += lensize;
        if len != 2 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "Pubcomp invalid length",
            ));
        }

        let id: u16 = ((buf[idx] as u16) << 8) | (buf[idx + 1] as u16);
        idx += 2;
        self.identifier = id;

        if idx != buf.len() {
            Err(std::io::Error::new(
                ErrorKind::Other,
                "Pubcomp message length incorrect",
            ))
        } else {
            Ok(())
        }
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
