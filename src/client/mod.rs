use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio_util::codec::Framed;

use crate::broker::BrokerId;
use crate::messages::codec::MqttCodec;

pub mod clientworker;

pub struct WillMessage {
    topic: String,
    qos: u8,
    retain: bool,
    message: Vec<u8>,
}

impl WillMessage {
    pub fn new(topic: String, message: Vec<u8>, qos: u8, retain: bool) -> WillMessage {
        WillMessage {
            topic,
            qos,
            retain,
            message,
        }
    }

    pub fn get_message(&self) -> &Vec<u8> {
        &self.message
    }

    pub fn get_topic(&self) -> &String {
        &self.topic
    }

    pub fn get_retain(&self) -> bool {
        self.retain
    }

    pub fn get_qos(&self) -> u8 {
        self.qos
    }
}

pub struct Client<T>
where
    T: AsyncRead + AsyncWrite + std::marker::Unpin,
{
    broker_id: BrokerId,
    id: String,
    src_address: String,
    pub stream: Framed<T, MqttCodec>,
    keep_alive: u16,
    will: Option<WillMessage>,
}

impl<T> Client<T>
where
    T: AsyncRead + AsyncWrite + std::marker::Unpin,
{
    pub fn new(
        stream: Framed<T, MqttCodec>,
        src_address: String,
        broker_id: BrokerId,
        id: String,
        keep_alive: u16,
        will: Option<WillMessage>,
    ) -> Client<T> {
        Client {
            broker_id,
            id,
            src_address,
            stream,
            keep_alive,
            will,
        }
    }

    pub fn get_source_address(&self) -> &String {
        &self.src_address
    }

    pub fn has_will(&self) -> bool {
        self.will.is_some()
    }

    pub fn get_will(&self) -> &Option<WillMessage> {
        &self.will
    }

    pub fn get_keep_alive(&self) -> u16 {
        self.keep_alive
    }

    pub fn get_id(&self) -> &String {
        &self.id
    }

    pub fn get_broker_id(&self) -> &BrokerId {
        &self.broker_id
    }
}
