use futures::channel::oneshot;
use std::collections::HashMap;
use std::collections::HashSet;

use std::sync::Arc;
use std::sync::Mutex;

use deadpool_sqlite::Pool;

use futures::stream::StreamExt;
use futures::SinkExt;

use crate::messages;

use crate::messages::MQTTMessageDisconnect;

use crate::messages::MQTTMessageConnack;
use crate::messages::MQTTMessagePuback;
use crate::messages::MQTTMessagePubcomp;
use crate::messages::MQTTMessagePublish;
use crate::messages::MQTTMessagePubrec;
use crate::messages::MQTTMessagePubrel;
use crate::messages::MQTTMessageSuback;

use crate::messages::MQTTMessageUnsuback;

use crate::messages::codec::MQTTMessage;
use crate::messages::MQTTQos;

use crate::broker::router;
use crate::broker::router::MessageRouterTx;
use crate::broker::router::RouterMessage;
use crate::client::clientworker::ClientTx;

use crate::broker::BrokerId;
use crate::config;
use crate::sessions::SessionError;
use crate::sessions::SessionState;

use crate::sessions::persist::get_session_persist_provider;
use crate::sessions::persist::SessionPersistProvider;

lazy_static! {
    static ref PUBLISH_SENT_COUNTER: prometheus::IntCounterVec = register_int_counter_vec!(
        "mqtt_publish_sent",
        "Total number of publish messages sent.",
        &[
            "instance_hostname",
            "instance_uuid",
            "tenant_id",
            "broker_id",
            "session_id"
        ]
    )
    .unwrap();
    static ref PUBLISH_RECEIVED_COUNTER: prometheus::IntCounterVec = register_int_counter_vec!(
        "mqtt_publish_received",
        "Total number of publish messages received.",
        &[
            "instance_hostname",
            "instance_uuid",
            "tenant_id",
            "broker_id",
            "session_id"
        ]
    )
    .unwrap();
}

pub struct NewClient {
    pub msg: messages::MQTTMessageConnect,
    pub client: ClientTx,
}

pub struct SessionInternalMessage {
    pub session: router::SessionId,
    pub message: MQTTMessage,
}

pub enum SessionMessage {
    Internal(SessionInternalMessage),
    Client(MQTTMessage),
    NewClient(NewClient),
}

/// Shorthand for the transmit half of the message channel.
pub type SessionTx = futures::channel::mpsc::UnboundedSender<SessionMessage>;

/// Shorthand for the receive half of the message channel.
type SessionRx = futures::channel::mpsc::UnboundedReceiver<SessionMessage>;

pub struct MQTTSession {
    logger: slog::Logger,
    broker_id: BrokerId,
    id: String,
    session_state: Arc<Mutex<SessionState>>,
    session_rx: SessionRx,
    router_tx: MessageRouterTx,
    subscriptions: HashMap<String, bool>,
    client: Option<ClientTx>,
    clean: bool,
    generation: i64,

    internal_id: i32,

    next_message_id: u16,
    qos_outgoing: HashSet<u16>,
    qos_outgoing_messages: Vec<MQTTMessage>,
    qos_incoming: HashSet<u16>,

    persist: Box<dyn SessionPersistProvider>,

    counter_publish_received: prometheus::IntCounter,
    counter_publish_sent: prometheus::IntCounter,
}

impl MQTTSession {
    pub fn new(
        logger: slog::Logger,
        broker_id: BrokerId,
        id: String,
        session_state: Arc<Mutex<SessionState>>,
        session_rx: SessionRx,
        router_tx: MessageRouterTx,
        database_pool: Option<Pool>,
    ) -> MQTTSession {
        let logger = logger.new(slog::o!("session" => id.clone()));

        let counter_publish_received = PUBLISH_RECEIVED_COUNTER.with_label_values(&[
            config::get_hostname(),
            config::get_uuid(),
            &broker_id.tenant_id.clone().unwrap_or("".to_string()),
            &broker_id.broker_id,
            &id,
        ]);

        let counter_publish_sent = PUBLISH_SENT_COUNTER.with_label_values(&[
            config::get_hostname(),
            config::get_uuid(),
            &broker_id.tenant_id.clone().unwrap_or("".to_string()),
            &broker_id.broker_id,
            &id,
        ]);

        MQTTSession {
            logger,
            broker_id: broker_id.clone(),
            session_state,
            session_rx,
            router_tx,
            id,
            subscriptions: HashMap::new(),
            client: None,
            clean: true,
            generation: 0,
            internal_id: 0,
            next_message_id: 1,

            qos_outgoing: HashSet::new(),
            qos_outgoing_messages: Vec::new(),
            qos_incoming: HashSet::new(),

            persist: get_session_persist_provider(broker_id, database_pool),

            counter_publish_received,
            counter_publish_sent,
        }
    }

    async fn create_persistent_connection(&mut self) -> Result<u8, SessionError> {
        self.persist.create_persistent_connection().await
    }

    async fn persist_session(&mut self, id: String) -> Result<(i32, i64, bool, u16), SessionError> {
        self.persist.persist_session(id).await
    }

    async fn load_persistent_subscriptions(
        &mut self,
        session_id: i32,
    ) -> Result<Vec<String>, SessionError> {
        self.persist.load_persistent_subscriptions(session_id).await
    }

    async fn persist_update_session_generation(
        &mut self,
        internal_id: i32,
        generation: i64,
    ) -> Result<i32, SessionError> {
        self.persist
            .persist_update_session_generation(internal_id, generation)
            .await
    }

    async fn persist_qos_incoming_load(
        &mut self,
        session_id: i32,
    ) -> Result<HashSet<u16>, SessionError> {
        self.persist.persist_qos_incoming_load(session_id).await
    }

    async fn persist_qos_incoming_store(
        &mut self,
        session_id: i32,
        mid: u16,
    ) -> Result<i32, SessionError> {
        self.persist
            .persist_qos_incoming_store(session_id, mid)
            .await
    }

    async fn persist_qos_incoming_delete(
        &mut self,
        session_id: i32,
        mid: u16,
    ) -> Result<u16, SessionError> {
        self.persist
            .persist_qos_incoming_delete(session_id, mid)
            .await
    }

    async fn persist_qos_incoming_clear(&mut self, session_id: i32) -> Result<i32, SessionError> {
        self.persist.persist_qos_incoming_clear(session_id).await
    }

    async fn init_persist_session(&mut self) -> Result<(), SessionError> {
        let (internal_id, generation, clean, next_mid) =
            self.persist_session(self.id.clone()).await?;

        slog::info!(
            self.logger,
            "Persistent session. id {}, generation {}, clean {}",
            internal_id,
            generation,
            clean
        );

        self.internal_id = internal_id;
        self.generation = generation;
        self.clean = clean;
        self.next_message_id = next_mid;

        if !self.clean {
            let subscriptions = self.load_persistent_subscriptions(self.internal_id).await?;

            for topic in subscriptions {
                self.subscriptions.insert(topic, true);
            }

            let qosin_mids = self.persist_qos_incoming_load(self.internal_id).await?;
            self.qos_incoming = qosin_mids;

            let qosmessages = self
                .persist
                .persistent_qosout_load(self.internal_id)
                .await?;
            for msg in &qosmessages {
                match msg {
                    MQTTMessage::Publish(m) => {
                        self.qos_outgoing.insert(m.get_identifier());
                    }
                    MQTTMessage::PubRec(m) => {
                        self.qos_outgoing.insert(m.get_identifier());
                    }
                    _ => {
                        return Err(SessionError::Persist(
                            "Error loading persistent state - unkown qos message".to_string(),
                        ));
                    }
                };
            }
            self.qos_outgoing_messages = qosmessages;
        }

        Ok(())
    }

    pub async fn run_loop(&mut self) {
        slog::debug!(
            self.logger,
            "Starting session {} broker {}",
            self.id,
            self.broker_id
        );

        let mut running: bool = true;

        match self.create_persistent_connection().await {
            Ok(_) => {
                if let Err(e) = self.init_persist_session().await {
                    slog::error!(self.logger, "{}", e);
                    running = false;
                }
            }
            Err(e) => {
                slog::error!(self.logger, "{}", e);
                running = false;
            }
        };

        while running {
            let session_event = self.session_rx.next().await;
            if let Some(v) = session_event {
                let result = match v {
                    SessionMessage::Internal(internal_message) => {
                        if internal_message.session.session_generation == self.generation {
                            match internal_message.message {
                                MQTTMessage::Publish(msg) => {
                                    self.internal_publish_message(msg).await
                                }
                                _ => {
                                    error!("Unknown msg over internal channel");
                                    Ok(())
                                }
                            }
                        } else {
                            Ok(())
                        }
                    }
                    SessionMessage::Client(client_message) => match client_message {
                        MQTTMessage::Publish(msg) => self.publish_message(msg).await,
                        MQTTMessage::PubAck(msg) => self.puback_message(msg).await,
                        MQTTMessage::PubRel(msg) => self.pubrel_message(msg).await,
                        MQTTMessage::PubRec(msg) => self.pubrec_message(msg).await,
                        MQTTMessage::PubComp(msg) => self.pubcomp_message(msg).await,
                        MQTTMessage::Subscribe(msg) => self.subscribe_message(msg).await,
                        MQTTMessage::Unsubscribe(msg) => self.unsubscribe_message(msg).await,
                        _ => {
                            error!("Unknown msg over internal channel");
                            Ok(())
                        }
                    },
                    SessionMessage::NewClient(new_client) => {
                        self.new_client_connected(new_client).await
                    }
                };
                match result {
                    Ok(_) => (),
                    Err(e) => match e {
                        SessionError::Client => (),
                        SessionError::Router => {
                            slog::debug!(self.logger, "RouterError");
                            running = false
                        }
                        SessionError::Persist(_) => {
                            slog::error!(self.logger, "{}", e);
                            running = false
                        }
                    },
                };
            }
        }

        // Disconnect client if connected
        if self.client.is_some() {
            if let Err(_e) = self
                .send_client_message(MQTTMessage::Disconnect(MQTTMessageDisconnect::new()))
                .await
            {
                slog::debug!(self.logger, "Failed to send disconnect on session close");
            }
        }
    }

    async fn new_client_connected(&mut self, new_client: NewClient) -> Result<(), SessionError> {
        if self.client.is_some()
            && self
                .send_client_message(MQTTMessage::Disconnect(MQTTMessageDisconnect::new()))
                .await
                .is_err()
        {
            self.client = None;
        }
        self.client = None;

        let mut connack = MQTTMessageConnack::new();
        let clean_result = if new_client.msg.has_clean_session() {
            self.clean_session().await
        } else {
            Ok(())
        };
        connack.set_session_present(!self.clean);
        self.clean = false;

        // Send connack
        self.client = Some(new_client.client);
        if clean_result.is_ok() {
            connack.set_return_code(messages::ReturnCode::Accepted);
        } else {
            connack.set_return_code(messages::ReturnCode::ServerUnavailable);
        }

        let client_result = self
            .send_client_message(MQTTMessage::ConnAck(connack))
            .await;

        self.send_queued_messages().await?;

        if clean_result.is_ok() {
            client_result
        } else {
            clean_result
        }
    }

    fn qos_outgoing_message_position(&self, mid: &u16) -> Option<usize> {
        if self.qos_outgoing.contains(mid) {
            self.qos_outgoing_messages.iter().position(|x| match x {
                MQTTMessage::Publish(m) => m.get_identifier() == *mid,
                MQTTMessage::PubRec(m) => m.get_identifier() == *mid,
                _ => false,
            })
        } else {
            None
        }
    }

    async fn qos_outgoing_set_dup(&mut self, mid: u16) -> Result<u16, SessionError> {
        if let Some(pos) = self.qos_outgoing_message_position(&mid) {
            if let Some(elem) = self.qos_outgoing_messages.get(pos) {
                match elem {
                    MQTTMessage::Publish(m) => {
                        if !m.get_dup() {
                            self.persist
                                .persist_qos_outgoing_update_dup(
                                    self.internal_id,
                                    m.get_identifier(),
                                    true,
                                )
                                .await?;

                            let mut r = m.clone();
                            r.set_dup(true);
                            self.qos_outgoing_messages[pos] = MQTTMessage::Publish(r);
                        }
                    }
                    _ => {
                        return Err(SessionError::Persist(
                            "QoS message not found to set dup".to_string(),
                        ))
                    }
                }
            }

            Ok(mid)
        } else {
            Err(SessionError::Persist(
                "QoS message not stored to set dup".to_string(),
            ))
        }
    }

    async fn qos_outgoing_next_mid(&mut self) -> Result<u16, SessionError> {
        let mid = self.next_message_id;
        self.next_message_id += 1;
        self.persist
            .persist_update_session_mid(self.internal_id, self.next_message_id)
            .await?;
        Ok(mid)
    }

    async fn qos_outgoing_insert(&mut self, message: MQTTMessage) -> Result<u16, SessionError> {
        match message {
            MQTTMessage::Publish(mut m) => {
                let mid = self.qos_outgoing_next_mid().await?;

                if !self.qos_outgoing.contains(&mid) {
                    m.set_identifier(mid);
                    let pubmessage = MQTTMessage::Publish(m);

                    self.persist
                        .persist_qos_outgoing_store(self.internal_id, mid, &pubmessage)
                        .await?;

                    self.qos_outgoing.insert(mid);
                    self.qos_outgoing_messages.push(pubmessage)
                } else {
                    return Err(SessionError::Persist(
                        "QoS outgoing mid duplicate found".to_string(),
                    ));
                }

                Ok(mid)
            }
            MQTTMessage::PubRec(m) => {
                let mid = m.get_identifier();
                if let Some(pos) = self.qos_outgoing_message_position(&mid) {
                    match self.qos_outgoing_messages[pos] {
                        MQTTMessage::Publish(_) => {
                            self.persist
                                .persist_qos_outgoing_delete(self.internal_id, mid)
                                .await?;

                            self.qos_outgoing_messages.remove(pos);

                            let pubrecmessage = MQTTMessage::PubRec(m);

                            self.persist
                                .persist_qos_outgoing_store(self.internal_id, mid, &pubrecmessage)
                                .await?;

                            self.qos_outgoing_messages.push(pubrecmessage);
                        }
                        MQTTMessage::PubRec(_) => (),
                        _ => {
                            return Err(SessionError::Persist(
                                "Uknown qos outgoing message in queue".to_string(),
                            ))
                        }
                    };

                    Ok(mid)
                } else {
                    let pubrecmessage = MQTTMessage::PubRec(m);

                    self.persist
                        .persist_qos_outgoing_store(self.internal_id, mid, &pubrecmessage)
                        .await?;

                    self.qos_outgoing.insert(mid);
                    self.qos_outgoing_messages.push(pubrecmessage);
                    Ok(mid)
                }
            }
            _ => Err(SessionError::Persist(
                "Uknown qos outgoing message".to_string(),
            )),
        }
    }

    async fn qos_outgoing_remove(&mut self, mid: &u16) -> Result<u16, SessionError> {
        if let Some(pos) = self.qos_outgoing_message_position(mid) {
            self.persist
                .persist_qos_outgoing_delete(self.internal_id, *mid)
                .await?;

            self.qos_outgoing_messages.remove(pos);
            self.qos_outgoing.remove(mid);
        }

        Ok(*mid)
    }

    async fn qos_incoming_store(&mut self, mid: u16) -> Result<u16, SessionError> {
        self.persist_qos_incoming_store(self.internal_id, mid)
            .await?;
        self.qos_incoming.insert(mid);
        Ok(mid)
    }

    fn qos_incoming_received(&mut self, mid: u16) -> bool {
        self.qos_incoming.contains(&mid)
    }

    async fn qos_incoming_remove(&mut self, mid: &u16) -> Result<u16, SessionError> {
        self.persist_qos_incoming_delete(self.internal_id, *mid)
            .await?;
        self.qos_incoming.remove(mid);
        Ok(*mid)
    }

    async fn send_queued_messages(&mut self) -> Result<(), SessionError> {
        for pos in 0..self.qos_outgoing_messages.len() {
            let message = self.qos_outgoing_messages[pos].clone();
            match message {
                MQTTMessage::Publish(m) => {
                    let mid = m.get_identifier();
                    let qos = m.get_qos();
                    let dup = m.get_dup();
                    self.send_client_message(MQTTMessage::Publish(m)).await?;

                    if (qos == MQTTQos::QOS1 as u8 || qos == MQTTQos::QOS2 as u8) && !dup {
                        self.qos_outgoing_set_dup(mid).await?;
                    }
                }
                MQTTMessage::PubRec(m) => {
                    let mid = m.get_identifier();
                    let mut pubrel = MQTTMessagePubrel::new();
                    pubrel.set_identifier(mid);
                    self.send_client_message(MQTTMessage::PubRel(pubrel))
                        .await?;
                }
                _ => return Err(SessionError::Persist("Unkown queued message".to_string())),
            }
        }

        Ok(())
    }

    async fn internal_publish_message(
        &mut self,
        mut message: messages::MQTTMessagePublish,
    ) -> Result<(), SessionError> {
        debug!("Client {} Publish over internal channel", self.id);

        self.counter_publish_sent.inc();

        let mut mid = message.get_identifier();
        message.set_dup(false);
        let qos = message.get_qos();

        if qos == MQTTQos::QOS1 as u8 || qos == MQTTQos::QOS2 as u8 {
            debug!("Client {} publish send qos {}", self.id, qos);

            mid = self
                .qos_outgoing_insert(MQTTMessage::Publish(message.clone()))
                .await?;

            message.set_identifier(mid);
        }

        if self.client.is_some() {
            self.send_client_message(MQTTMessage::Publish(message))
                .await?;

            if qos == MQTTQos::QOS1 as u8 || qos == MQTTQos::QOS2 as u8 {
                self.qos_outgoing_set_dup(mid).await?;
            }
        }

        Ok(())
    }

    async fn puback_message(
        &mut self,
        message: messages::MQTTMessagePuback,
    ) -> Result<(), SessionError> {
        debug!(
            "Session {} Received puback id {}",
            self.id,
            message.get_identifier()
        );

        self.qos_outgoing_remove(&message.get_identifier()).await?;

        Ok(())
    }

    async fn pubrel_message(
        &mut self,
        message: messages::MQTTMessagePubrel,
    ) -> Result<(), SessionError> {
        debug!(
            "Session {} Received pubrel id {}",
            self.id,
            message.get_identifier()
        );

        let mid = message.get_identifier();
        self.qos_incoming_remove(&mid).await?;

        let mut pubcomp = MQTTMessagePubcomp::new();
        pubcomp.set_identifier(mid);
        self.send_client_message(MQTTMessage::PubComp(pubcomp))
            .await
    }

    async fn pubrec_message(
        &mut self,
        message: messages::MQTTMessagePubrec,
    ) -> Result<(), SessionError> {
        debug!(
            "Session {} Received pubrel id {}",
            self.id,
            message.get_identifier()
        );

        let mid = self
            .qos_outgoing_insert(MQTTMessage::PubRec(message))
            .await?;

        let mut pubrel = MQTTMessagePubrel::new();
        pubrel.set_identifier(mid);
        self.send_client_message(MQTTMessage::PubRel(pubrel)).await
    }

    async fn pubcomp_message(
        &mut self,
        message: messages::MQTTMessagePubcomp,
    ) -> Result<(), SessionError> {
        debug!(
            "Session {} Received pubcomp id {}",
            self.id,
            message.get_identifier()
        );

        self.qos_outgoing_remove(&message.get_identifier()).await?;

        Ok(())
    }

    async fn publish_message(
        &mut self,
        mut message: messages::MQTTMessagePublish,
    ) -> Result<(), SessionError> {
        slog::debug!(
            self.logger,
            "Client {} Received publish to {}",
            self.id,
            message.get_topic()
        );

        self.counter_publish_received.inc();

        let qos = message.get_qos();
        let mid = message.get_identifier();

        if qos == MQTTQos::QOS2 as u8 && self.qos_incoming_received(mid) {
            let mut pubrec = MQTTMessagePubrec::new();
            pubrec.set_identifier(mid);
            self.send_client_message(MQTTMessage::PubRec(pubrec))
                .await?;

            return Ok(());
        }

        message.set_dup(false);

        let msg = router::RouterPublishMessage {
            session: router::SessionId {
                session: self.id.clone(),
                session_generation: self.generation,
                internal_id: self.internal_id,
            },
            message,
        };
        self.send_message_router(router::RouterMessage::Publish(msg))
            .await?;

        if qos == MQTTQos::QOS1 as u8 {
            let mut puback = MQTTMessagePuback::new();
            puback.set_identifier(mid);
            if self
                .send_client_message(MQTTMessage::PubAck(puback))
                .await
                .is_err()
            {
                slog::debug!(self.logger, "puback failed send for message {}", mid);
            }
        } else if qos == MQTTQos::QOS2 as u8 {
            self.qos_incoming_store(mid).await?;

            let mut pubrec = MQTTMessagePubrec::new();
            pubrec.set_identifier(mid);
            if self
                .send_client_message(MQTTMessage::PubRec(pubrec))
                .await
                .is_err()
            {
                slog::debug!(self.logger, "pubrec failed send for message {}", mid);
            }
        }

        Ok(())
    }

    async fn subscribe_message(
        &mut self,
        message: messages::MQTTMessageSubscribe,
    ) -> Result<(), SessionError> {
        let mut client_error = false;
        let mut router_error = false;
        let mut suback = MQTTMessageSuback::new();
        let mut retained_messages: Vec<MQTTMessagePublish> = Vec::new();
        suback.set_identifier(message.get_identifier());

        for (topic, qos) in message.get_topics() {
            debug!(
                "Client {} Received Subscribe to {} id {}",
                self.id,
                topic,
                message.get_identifier()
            );
            let response = match self.subscribe(topic, *qos).await {
                Err(_) => {
                    router_error = true;
                    0x80
                }
                Ok(mut response) => {
                    if response.ret != 0x80 {
                        retained_messages.append(&mut response.retained_messages);
                        *qos
                    } else {
                        0x80
                    }
                }
            };

            suback.add_response(response);
        }

        if self
            .send_client_message(MQTTMessage::SubAck(suback))
            .await
            .is_ok()
        {
            for msg in retained_messages {
                if let Err(_e) = self.send_client_message(MQTTMessage::Publish(msg)).await {
                    client_error = true;
                    break;
                }
            }
        }

        if router_error {
            Err(SessionError::Router)
        } else if client_error {
            Err(SessionError::Client)
        } else {
            Ok(())
        }
    }

    async fn subscribe(
        &mut self,
        topic: &str,
        qos: u8,
    ) -> Result<router::RouterPublishSubscribeResponse, SessionError> {
        self.subscriptions.insert(topic.to_string(), true);

        let (conn_tx, conn_rx): (router::RouterSubResponseTx, router::RouterSubResponseRx) =
            oneshot::channel();

        let msg = router::RouterPublishSubscribe {
            session: router::SessionId {
                session: self.id.clone(),
                session_generation: self.generation,
                internal_id: self.internal_id,
            },
            topic: topic.to_string(),
            qos,
            response_tx: conn_tx,
        };
        self.send_message_router(router::RouterMessage::Subscribe(msg))
            .await?;

        if let Ok(response) = conn_rx.await {
            return Ok(response);
        } else {
            slog::warn!(
                self.logger,
                "Session {} Unable to receive subscribe response",
                self.id,
            );
        }

        Err(SessionError::Router)
    }

    async fn unsubscribe_message(
        &mut self,
        message: messages::MQTTMessageUnsubscribe,
    ) -> Result<(), SessionError> {
        let mut ack = MQTTMessageUnsuback::new();
        ack.set_identifier(message.get_identifier());

        for topic in message.get_topics() {
            debug!(
                "Client {} unsubscribe from {} id {}",
                self.id,
                topic,
                message.get_identifier()
            );
        }
        self.unsubscribe(message.get_topics()).await?;
        self.send_client_message(MQTTMessage::UnsubAck(ack)).await
    }

    async fn send_client_message(&mut self, message: MQTTMessage) -> Result<(), SessionError> {
        if self.client.is_some() {
            if let Err(_e) = self.client.as_mut().unwrap().send(message).await {
                self.client = None;
                Err(SessionError::Client)
            } else {
                Ok(())
            }
        } else {
            Err(SessionError::Client)
        }
    }

    async fn send_message_router(&mut self, message: RouterMessage) -> Result<(), SessionError> {
        if let Err(_e) = self.router_tx.send(message).await {
            Err(SessionError::Router)
        } else {
            Ok(())
        }
    }

    async fn clean_session(&mut self) -> Result<(), SessionError> {
        self.persist_qos_incoming_clear(self.internal_id).await?;
        self.qos_incoming.clear();

        self.persist
            .persist_qos_outgoing_clear(self.internal_id)
            .await?;
        self.qos_outgoing.clear();
        self.qos_outgoing_messages.clear();

        self.unsubscribe_all().await?;
        self.clean = true;
        self.generation += 1;
        self.next_message_id = 1;
        match self
            .persist_update_session_generation(self.internal_id, self.generation)
            .await
        {
            Err(e) => Err(e),
            Ok(_) => Ok(()),
        }
    }

    async fn unsubscribe_all(&mut self) -> Result<(), SessionError> {
        let mut topics: Vec<String> = Vec::new();

        for topic in self.subscriptions.keys() {
            topics.push(topic.to_string());
        }
        self.unsubscribe(&topics).await?;

        self.subscriptions.clear();

        Ok(())
    }

    async fn unsubscribe(&mut self, topics: &[String]) -> Result<(), SessionError> {
        for topic in topics {
            self.subscriptions.remove(topic);
        }
        let msg = router::RouterPublisUnsubscribe {
            session: router::SessionId {
                session: self.id.clone(),
                session_generation: self.generation,
                internal_id: self.internal_id,
            },
            topics: topics.to_vec(),
        };
        self.send_message_router(router::RouterMessage::Unsubscribe(msg))
            .await
    }
}

impl Drop for MQTTSession {
    fn drop(&mut self) {
        slog::debug!(self.logger, "Session {} Cleanup", self.id);

        let mut ss = self.session_state.lock().unwrap();
        ss.running = false;
    }
}
