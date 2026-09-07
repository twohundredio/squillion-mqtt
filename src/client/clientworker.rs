use std::collections::HashMap;

use std::time::Duration;

use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;

use futures::stream::StreamExt;
use futures::SinkExt;

use tokio_util::time::{delay_queue, DelayQueue};

use crate::messages;

use crate::messages::MQTTMessagePingResp;
use crate::messages::MQTTMessagePublish;

use crate::messages::codec::MQTTMessage;

use crate::client::Client;

use crate::sessions::sessionworker::SessionMessage;
use crate::sessions::sessionworker::SessionTx;

use crate::config;

lazy_static! {
    static ref CLIENTS_CONNECTED_GUAGE: prometheus::IntGaugeVec = register_int_gauge_vec!(
        "mqtt_clients_connected",
        "Total number of connections made.",
        &[
            "instance_hostname",
            "instance_uuid",
            "tenant_id",
            "broker_id"
        ]
    )
    .unwrap();
}

/// Shorthand for the transmit half of the message channel.
pub type ClientTx = futures::channel::mpsc::UnboundedSender<MQTTMessage>;

/// Shorthand for the receive half of the message channel.
type ClientRx = futures::channel::mpsc::UnboundedReceiver<MQTTMessage>;

#[derive(PartialEq)]
enum ConnectionStatus {
    Connected,
    Disconnect,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
enum ClientTimers {
    KeepAlive,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
enum ClientResult {
    Finished,
}

#[derive(Default)]
struct Settings {
    validation: bool,
    policy: bool,
}

pub struct MQTTClientWorker<T>
where
    T: AsyncRead + AsyncWrite + std::marker::Unpin + std::marker::Send,
{
    logger: slog::Logger,
    id: String,
    client_rx: ClientRx,
    client_tx: ClientTx,
    session_tx: SessionTx,
    client: Client<T>,
    status: ConnectionStatus,
    timers: DelayQueue<ClientTimers>,
    timer_keys: HashMap<ClientTimers, (u64, delay_queue::Key)>,

    settings: Settings,
}

impl<T> MQTTClientWorker<T>
where
    T: AsyncRead + AsyncWrite + std::marker::Unpin + std::marker::Send,
{
    pub fn new(
        logger: slog::Logger,
        client: Client<T>,
        session_tx: SessionTx,
    ) -> MQTTClientWorker<T> {
        let (client_tx, client_rx) = futures::channel::mpsc::unbounded();

        let logger = logger.new(slog::o!("session" => client.get_id().to_string(),
                     "_src_address" => client.get_source_address().clone()));

        MQTTClientWorker {
            logger,
            id: client.get_id().to_string(),
            client_rx,
            client_tx,
            session_tx,
            client,
            status: ConnectionStatus::Connected,
            timers: DelayQueue::new(),
            timer_keys: HashMap::new(),
            settings: Settings::default(),
        }
    }

    pub fn get_tx(&mut self) -> &ClientTx {
        &self.client_tx
    }

    fn load_settings(&mut self) {
        self.settings.validation =
            config::get_bool("enable_validation").unwrap_or_default();

        self.settings.policy = config::get_bool("enable_policy").unwrap_or_default();
    }

    pub async fn run_loop(&mut self) {
        let guage_clients_connected = CLIENTS_CONNECTED_GUAGE.with_label_values(&[
            config::get_hostname(),
            config::get_uuid(),
            &self
                .client
                .broker_id
                .tenant_id
                .clone()
                .unwrap_or("".to_string()),
            &self.client.broker_id.broker_id,
        ]);

        self.load_settings();

        let mut running: bool = true;

        slog::info!(
            self.logger,
            "Client connect: session id {}",
            self.id;
            "user_visable" => true
        );

        guage_clients_connected.inc();

        self.set_keepalive_timer();

        while running {
            let result = tokio::select! {
                client_rx_event = self.client_rx.next() => {
                    if let Some(e) = client_rx_event {
                        self.client_rx_recv_event(e).await
                    } else {
                        Err("Session rx closed".to_string())
                    }
                },
                client = self.client.stream.next() => {
                    if let Some(event_result) = client {
                        if let Ok(e) = event_result {
                            self.client_recv_event(e).await
                        } else {
                            slog::debug!(self.logger, "Client recv error event");
                            Err("Client recv error event".to_string())
                        }
                    } else {
                        Err("Client closed".to_string())
                    }
                },
                timer = self.timers.next(), if !self.timers.is_empty()  => {
                    if let Some(timer_event) = timer  {
                        self.timer_event(timer_event).await
                    } else {
                        // None event
                        slog::error!(self.logger, "Timer error event");
                        Err("Timer error event".to_string())
                    }
                },
            };

            if let Err(_error) = result {
                running = false;
            }
            if self.status == ConnectionStatus::Disconnect {
                running = false;
            }
        }

        if self.status != ConnectionStatus::Disconnect && self.client.has_will() {
            self.send_will().await;
        }

        slog::info!(
            self.logger,
            "Client disconnect: session id {}",
            self.id;
            "user_visable" => true
        );

        guage_clients_connected.dec();
    }

    pub async fn client_rx_recv_event(&mut self, message: MQTTMessage) -> Result<(), String> {
        match message {
            MQTTMessage::ConnAck(msg) => {
                if msg.get_return_code() != messages::ReturnCode::Accepted {
                    self.status = ConnectionStatus::Disconnect;
                }
                self.send_client_message(MQTTMessage::ConnAck(msg)).await
            }
            MQTTMessage::Publish(msg) => self.send_client_message(MQTTMessage::Publish(msg)).await,
            MQTTMessage::PubAck(msg) => self.send_client_message(MQTTMessage::PubAck(msg)).await,
            MQTTMessage::PubRec(msg) => self.send_client_message(MQTTMessage::PubRec(msg)).await,
            MQTTMessage::PubRel(msg) => self.send_client_message(MQTTMessage::PubRel(msg)).await,
            MQTTMessage::PubComp(msg) => self.send_client_message(MQTTMessage::PubComp(msg)).await,
            MQTTMessage::SubAck(msg) => self.send_client_message(MQTTMessage::SubAck(msg)).await,
            MQTTMessage::UnsubAck(msg) => {
                self.send_client_message(MQTTMessage::UnsubAck(msg)).await
            }
            MQTTMessage::Disconnect(_msg) => {
                // TODO: Handle better
                // Session has told us to disconnect
                Err("Session Disconnect".to_string())
            }
            _ => {
                slog::error!(self.logger, "Unknown msg over internal channel");
                Err("Unknown msg over internal channel".to_string())
            }
        }
    }

    pub async fn client_recv_event(&mut self, message: MQTTMessage) -> Result<(), String> {
        match message {
            MQTTMessage::Publish(msg) => self.publish_message(msg).await,
            MQTTMessage::PubAck(msg) => self.puback_message(msg).await,
            MQTTMessage::PubRec(msg) => self.pubrec_message(msg).await,
            MQTTMessage::PubRel(msg) => self.pubrel_message(msg).await,
            MQTTMessage::PubComp(msg) => self.pubcomp_message(msg).await,
            MQTTMessage::Subscribe(msg) => self.subscribe_message(msg).await,
            MQTTMessage::Unsubscribe(msg) => self.unsubscribe_message(msg).await,
            MQTTMessage::PingReq(msg) => self.ping_request_message(msg).await,
            MQTTMessage::Disconnect(msg) => {
                if self.disconnect_message(msg).await.is_err() {
                    slog::error!(self.logger, "Error disconnecting");
                }
                Err("Client Disconnect".to_string())
            }
            _ => {
                slog::error!(self.logger, "Unknown message recevied");
                Err("Unknown message from client recevied".to_string())
            }
        }
    }

    async fn timer_event(
        &mut self,
        timer: delay_queue::Expired<ClientTimers>,
    ) -> Result<(), String> {
        self.timer_keys.remove(timer.get_ref());

        match self.timer_fired(timer.get_ref()) {
            Ok(ClientResult::Finished) => Err("Client Timeout".to_string()),
            Err(_) => Err("Client Timer Error".to_string()),
        }
    }

    fn reset_keepalive(&mut self) {
        if self.client.get_keep_alive() > 0 {
            self.reset_timer(ClientTimers::KeepAlive);
        }
    }

    fn set_keepalive_timer(&mut self) {
        let ka = self.client.get_keep_alive() as u64;
        if ka > 0 {
            let ka = (ka + (ka >> 2)) * 1000;
            self.add_timer(ClientTimers::KeepAlive, ka);
        }
    }

    fn keepalive_timer(&mut self) -> Result<ClientResult, &'static str> {
        Ok(ClientResult::Finished)
    }

    fn timer_fired(&mut self, key: &ClientTimers) -> Result<ClientResult, &'static str> {
        match key {
            ClientTimers::KeepAlive => self.keepalive_timer(),
        }
    }

    fn add_timer(&mut self, key: ClientTimers, ms: u64) {
        let delay = self.timers.insert(key.clone(), Duration::from_millis(ms));
        self.timer_keys.insert(key, (ms, delay));
    }

    fn reset_timer(&mut self, key: ClientTimers) {
        if let Some((ms, cache_key)) = self.timer_keys.get(&key) {
            self.timers.reset(cache_key, Duration::from_millis(*ms));
        }
    }

    fn remove_timer(&mut self, key: ClientTimers) {
        if let Some((_ms, cache_key)) = self.timer_keys.get(&key) {
            self.timers.remove(cache_key);
            self.timer_keys.remove(&key);
        }
    }

    async fn publish_message(
        &mut self,
        message: messages::MQTTMessagePublish,
    ) -> Result<(), String> {
        slog::debug!(
            self.logger,
            "Client {} Received publish to {}",
            self.id,
            message.get_topic()
        );
        self.reset_keepalive();

        self.send_message_session(MQTTMessage::Publish(message))
            .await
    }

    async fn puback_message(&mut self, message: messages::MQTTMessagePuback) -> Result<(), String> {
        debug!(
            "Client {} Received puback id {}",
            self.id,
            message.get_identifier()
        );
        self.reset_keepalive();

        self.send_message_session(MQTTMessage::PubAck(message))
            .await
    }

    async fn pubrec_message(&mut self, message: messages::MQTTMessagePubrec) -> Result<(), String> {
        debug!(
            "Client {} Received pubrec id {}",
            self.id,
            message.get_identifier()
        );
        self.reset_keepalive();

        self.send_message_session(MQTTMessage::PubRec(message))
            .await
    }

    async fn pubrel_message(&mut self, message: messages::MQTTMessagePubrel) -> Result<(), String> {
        debug!(
            "Client {} Received pubrel id {}",
            self.id,
            message.get_identifier()
        );
        self.reset_keepalive();

        self.send_message_session(MQTTMessage::PubRel(message))
            .await
    }

    async fn pubcomp_message(
        &mut self,
        message: messages::MQTTMessagePubcomp,
    ) -> Result<(), String> {
        debug!(
            "Client {} Received pubcomp id {}",
            self.id,
            message.get_identifier()
        );
        self.reset_keepalive();

        self.send_message_session(MQTTMessage::PubComp(message))
            .await
    }

    async fn subscribe_message(
        &mut self,
        message: messages::MQTTMessageSubscribe,
    ) -> Result<(), String> {
        debug!(
            "Client {} subscribe from id {}",
            self.id,
            message.get_identifier()
        );

        self.reset_keepalive();

        self.send_message_session(MQTTMessage::Subscribe(message))
            .await
    }

    async fn unsubscribe_message(
        &mut self,
        message: messages::MQTTMessageUnsubscribe,
    ) -> Result<(), String> {
        self.reset_keepalive();

        self.send_message_session(MQTTMessage::Unsubscribe(message))
            .await
    }

    async fn ping_request_message(
        &mut self,
        _message: messages::MQTTMessagePingReq,
    ) -> Result<(), String> {
        debug!("Client {} PINGREQ received", self.id);
        self.reset_keepalive();

        let pingresp = MQTTMessagePingResp::new();

        self.send_client_message(MQTTMessage::PingResp(pingresp))
            .await
    }

    async fn disconnect_message(
        &mut self,
        _message: messages::MQTTMessageDisconnect,
    ) -> Result<(), String> {
        debug!("Client {} Disconnect received", self.id);
        self.remove_timer(ClientTimers::KeepAlive);

        self.status = ConnectionStatus::Disconnect;

        Ok(())
    }

    async fn send_client_message(&mut self, message: MQTTMessage) -> Result<(), String> {
        if let Err(e) = self.client.stream.send(message).await {
            return Err(format!("Unable to send to client: {}", e));
        }

        Ok(())
    }

    async fn send_message_session(&mut self, message: MQTTMessage) -> Result<(), String> {
        if let Err(e) = self.session_tx.send(SessionMessage::Client(message)).await {
            return Err(format!("Unable to send to session: {}", e));
        }

        Ok(())
    }

    async fn send_will(&mut self) {
        // Function looks odd - something to do with ws errors not
        // implementing Sync - Errors cannot move between threads
        let mut willmsg: Option<MQTTMessagePublish> = None;
        if let Some(will) = self.client.get_will() {
            let mut msg = MQTTMessagePublish::new();
            msg.set_topic(will.get_topic().clone());
            msg.set_message(will.get_message().clone().as_bytes().to_vec());
            msg.set_retain(will.get_retain());
            msg.set_qos(will.get_qos());

            willmsg = Some(msg);
        }

        if let Some(msg) = willmsg {
            if self
                .send_message_session(MQTTMessage::Publish(msg))
                .await
                .is_err()
            {
                slog::warn!(self.logger, "Unable to send will message");
            }
        }
    }
}

impl<T> Drop for MQTTClientWorker<T>
where
    T: AsyncRead + AsyncWrite + std::marker::Unpin + std::marker::Send,
{
    fn drop(&mut self) {
        slog::debug!(self.logger, "Client {} cleanup complete", self.id);
    }
}
