// Topic management

use crate::broker::persist::get_persist_provider;
use crate::broker::persist::PersistProvider;
use crate::broker::BrokerError;
use crate::broker::BrokerId;
use crate::config;
use crate::messages::MQTTMessagePublish;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;

use deadpool_sqlite::Pool;

use super::router::SessionId;

lazy_static! {
    static ref SUBSCRIPTIONS_GUAGE: prometheus::IntGaugeVec = register_int_gauge_vec!(
        "mqtt_subscriptions",
        "Total number of subscriptions.",
        &[
            "instance_hostname",
            "instance_uuid",
            "tenant_id",
            "broker_id"
        ]
    )
    .unwrap();
}

lazy_static! {
    static ref RETAINED_MESSAGES_GUAGE: prometheus::IntGaugeVec = register_int_gauge_vec!(
        "mqtt_retained_messages",
        "Total number of subscriptions.",
        &[
            "instance_hostname",
            "instance_uuid",
            "tenant_id",
            "broker_id"
        ]
    )
    .unwrap();
}

pub struct Subscriptions {
    topics: Topic,
    guage_subscriptions: prometheus::IntGauge,
    guage_retained_messages: prometheus::IntGauge,
    logger: slog::Logger,
    persist: Box<dyn PersistProvider>,
}

#[derive(Clone)]
struct TopicEntry {
    pub session: SessionId,
    pub qos: u8,
}

#[derive(Clone)]
struct Topic {
    topics: HashMap<String, Topic>,
    internal_id: Option<i32>,
    clients: HashMap<String, TopicEntry>,
    message: Option<MQTTMessagePublish>,
}

impl Subscriptions {
    pub fn new(broker_id: BrokerId, logger: slog::Logger, database_pool: Option<Pool>) -> Self {
        let guage_subscriptions = SUBSCRIPTIONS_GUAGE.with_label_values(&[
            config::get_hostname(),
            config::get_uuid(),
            &broker_id.tenant_id.clone().unwrap_or("".to_string()),
            &broker_id.broker_id,
        ]);

        let guage_retained_messages = RETAINED_MESSAGES_GUAGE.with_label_values(&[
            config::get_hostname(),
            config::get_uuid(),
            &broker_id.tenant_id.clone().unwrap_or("".to_string()),
            &broker_id.broker_id,
        ]);

        Subscriptions {
            topics: Topic::new(None),
            guage_subscriptions,
            guage_retained_messages,
            logger,
            persist: get_persist_provider(broker_id, database_pool),
        }
    }

    pub async fn create_persistent_connection(&mut self) -> Result<u8, BrokerError> {
        self.persist.create_persistent_connection().await
    }

    pub async fn init_persistent_store(&mut self) -> Result<u8, BrokerError> {
        self.persist.init_persistent_store().await
    }

    async fn load_persistent_message(
        &mut self,
        message_id: i32,
    ) -> Result<MQTTMessagePublish, BrokerError> {
        self.persist.load_persistent_message(message_id).await
    }

    async fn load_persistent_subscriptions(
        &mut self,
        topic_id: i32,
    ) -> Result<Vec<(SessionId, u8)>, BrokerError> {
        self.persist.load_persistent_subscriptions(topic_id).await
    }

    pub async fn load_persistent_state(&mut self) -> Result<u8, BrokerError> {
        let mut retained_count = 0;
        let mut subscription_count = 0;

        let topics = self.persist.load_persistent_topics().await?;
        let topic_count = topics.len();

        for t in topics {
            let topic_internal_id: i32 = t.topic_id;
            let topic_retained_message_id: Option<i32> = t.retained_message_id;
            let topic_name: std::string::String = t.topic_name;

            let mut topic = Topic::new(Some(topic_internal_id));
            if let Some(message_id) = topic_retained_message_id {
                let message = self.load_persistent_message(message_id).await?;
                topic.message = Some(message);
                retained_count += 1;
            }

            let subscriptions = self
                .load_persistent_subscriptions(topic_internal_id)
                .await?;
            for (session, qos) in subscriptions {
                topic.set_subscribed(session, qos);
                subscription_count += 1;
            }

            self.insert_topic(&topic_name, topic);
        }

        /* Set counts */
        self.guage_subscriptions.set(subscription_count);
        self.guage_retained_messages.set(retained_count);

        slog::info!(
            self.logger,
            "Persistent broker loaded. Topic count {}. Subscription count {}. Retained messages {}.",
            topic_count,
            subscription_count,
            retained_count
        );

        Ok(0)
    }

    async fn persist_topic(&mut self, topic: String) -> Result<i32, BrokerError> {
        self.persist.persist_topic(topic).await
    }

    async fn persist_delete_topic(&mut self, topic_id: i32) -> Result<i32, BrokerError> {
        self.persist.persist_delete_topic(topic_id).await
    }

    fn insert_topic(&mut self, topic: &str, topic_item: Topic) {
        let t: Vec<_> = topic.split('/').collect();
        let len = t.len();
        let _internal_id = 0_i32;

        slog::debug!(self.logger, "intert topic {}", topic);

        let mut tm = &mut self.topics;
        let mut count = 1;
        for level in &t {
            let is_last = len == count;
            count += 1;
            tm = match tm.topics.entry(level.to_string()) {
                Vacant(entry) => {
                    if !is_last {
                        entry.insert(Topic::new(None))
                    } else {
                        entry.insert(topic_item);
                        break;
                    }
                }
                Occupied(entry) => {
                    let e = entry.into_mut();
                    if is_last {
                        // Want to keep existing topics
                        e.internal_id = topic_item.internal_id;
                        e.clients = topic_item.clients;
                        e.message = topic_item.message;
                        break;
                    }
                    e
                }
            };
        }
    }

    async fn get_topic(&mut self, topic: &str) -> Result<&mut Topic, BrokerError> {
        let t: Vec<_> = topic.split('/').collect();
        let len = t.len();
        let mut internal_id = 0_i32;

        let new_topic = {
            let mut new_topic = false;
            let mut tm = &mut self.topics;
            let mut count = 1;
            for level in &t {
                let is_last = len == count;
                count += 1;
                tm = match tm.topics.entry(level.to_string()) {
                    Vacant(entry) => {
                        if !is_last {
                            entry.insert(Topic::new(None))
                        } else {
                            slog::debug!(self.logger, "new topic {}", topic);
                            new_topic = true;
                            break;
                        }
                    }
                    Occupied(entry) => {
                        let e = entry.into_mut();
                        if is_last && e.get_internal_id().is_none() {
                            new_topic = true;
                        }
                        e
                    }
                };
            }
            new_topic
        };

        if new_topic {
            internal_id = self.persist_topic(topic.to_string()).await?;
        }

        let mut tm = &mut self.topics;
        let mut count = 1;
        for level in &t {
            let is_last = len == count;
            count += 1;
            tm = match tm.topics.entry(level.to_string()) {
                Vacant(entry) => {
                    if !is_last {
                        slog::error!(self.logger, "new topic not in last entry!");
                    }
                    entry.insert(Topic::new(Some(internal_id)))
                }
                Occupied(entry) => {
                    let e = entry.into_mut();
                    if is_last && new_topic {
                        e.set_internal_id(Some(internal_id));
                    }
                    e
                }
            };
        }

        Ok(tm)
    }

    async fn persist_subscribe(
        &mut self,
        session_id: i32,
        topic_id: i32,
        qos: u8,
    ) -> Result<bool, BrokerError> {
        self.persist
            .persist_subscribe(session_id, topic_id, qos)
            .await
    }

    async fn persist_update_subscribe(
        &mut self,
        session_id: i32,
        topic_id: i32,
        qos: u8,
    ) -> Result<bool, BrokerError> {
        self.persist
            .persist_update_subscribe(session_id, topic_id, qos)
            .await
    }

    pub async fn subscribe(
        &mut self,
        session: SessionId,
        topic: &str,
        qos: u8,
    ) -> Result<u8, BrokerError> {
        let tm = self.get_topic(topic).await?;
        let topic_id = tm.get_internal_id().unwrap();
        match tm.get_subscribed(&session) {
            Some(subscribed_qos) => {
                if subscribed_qos != qos {
                    // Update
                    if let Ok(_result) = self
                        .persist_update_subscribe(session.internal_id, topic_id, qos)
                        .await
                    {
                        let tm = self.get_topic(topic).await?;
                        tm.set_subscribed(session, qos);
                        Ok(qos)
                    } else {
                        Ok(subscribed_qos)
                    }
                } else {
                    // Ok
                    Ok(qos)
                }
            }
            None => {
                // New
                let _result = self
                    .persist_subscribe(session.internal_id, topic_id, qos)
                    .await?;
                let tm = self.get_topic(topic).await?;
                if tm.set_subscribed(session, qos) {
                    // New subscription
                    self.guage_subscriptions.inc();
                    Ok(qos)
                } else {
                    Ok(0x80)
                }
            }
        }
    }

    async fn persist_retain(
        &mut self,
        topic_id: i32,
        msg: &MQTTMessagePublish,
    ) -> Result<i32, BrokerError> {
        self.persist.persist_retain(topic_id, msg).await
    }

    async fn persist_delete_retain(&mut self, topic_id: i32) -> Result<(), BrokerError> {
        self.persist.persist_delete_retain(topic_id).await
    }

    pub async fn retain(&mut self, msg: MQTTMessagePublish) -> Result<(), BrokerError> {
        let topic = msg.get_topic();
        let mut tm = self.get_topic(topic).await?;

        let topic_id = tm.get_internal_id().unwrap();
        let is_retained_message = tm.message.is_some();

        self.persist_delete_retain(topic_id).await?;
        if !msg.get_message().is_empty() {
            self.persist_retain(topic_id, &msg).await?;
        }

        tm = self.get_topic(topic).await?;
        if msg.get_message().is_empty() {
            tm.message = None;

            let to_delete = tm.message.is_none() && (tm.get_num_subscribed() == 0);
            if to_delete {
                let _result = self.persist_delete_topic(topic_id).await?;
                let tm = self.get_topic(topic).await?;
                tm.set_internal_id(None);
            }

            if is_retained_message {
                self.guage_retained_messages.dec();
            }
        } else {
            tm.message = Some(msg);
            if !is_retained_message {
                self.guage_retained_messages.inc();
            }
        }

        Ok(())
    }

    pub fn get_retained_messages(&mut self, topic: String) -> Vec<MQTTMessagePublish> {
        let t: Vec<_> = topic.split('/').collect();
        let mut messages: Vec<MQTTMessagePublish> = Vec::new();

        let mut tm = Some(&mut self.topics);

        for level in t {
            tm = match tm {
                Some(entry) => entry.topics.get_mut(level),
                None => None,
            };
        }
        if let Some(entry) = tm {
            if let Some(m) = &entry.message {
                messages.push(m.clone());
            }
        }

        messages
    }

    async fn persist_unsubscribe(
        &mut self,
        session_id: i32,
        topic_id: i32,
    ) -> Result<bool, BrokerError> {
        self.persist.persist_unsubscribe(session_id, topic_id).await
    }

    pub async fn unsubscribe(
        &mut self,
        session: &SessionId,
        topic: &str,
    ) -> Result<(), BrokerError> {
        let t: Vec<_> = topic.split('/').collect();

        let mut tm = Some(&mut self.topics);

        for level in t {
            tm = match tm {
                Some(entry) => entry.topics.get_mut(level),
                None => None,
            };
        }
        if let Some(entry) = tm {
            if entry.set_unsubscribed(&session.session) {
                self.guage_subscriptions.dec();
                let topic_id = entry.get_internal_id().unwrap();
                let to_delete = entry.message.is_none() && (entry.get_num_subscribed() == 0);

                let _result = self
                    .persist_unsubscribe(session.internal_id, topic_id)
                    .await?;
                if to_delete {
                    let _result = self.persist_delete_topic(topic_id).await?;
                    let tm = self.get_topic(topic).await?;
                    tm.set_internal_id(None);
                }
            }
        }

        Ok(())
    }

    pub fn get_subscribed(&self, topic: &str) -> HashMap<SessionId, u8> {
        let t: Vec<_> = topic.split('/').collect();

        let mut clients: HashMap<SessionId, u8> = HashMap::new();

        let starttopic = &self.topics;

        let mut topicmatchs: Vec<&Topic> = vec![starttopic];

        for level in t {
            let mut nexttopics: Vec<&Topic> = Vec::new();

            for tm in &topicmatchs {
                if let Some(entry) = tm.topics.get("#") {
                    for s in entry.clients.values() {
                        clients.insert(s.session.clone(), s.qos);
                    }
                };
                if let Some(entry) = tm.topics.get("+") {
                    nexttopics.push(entry);
                };

                if let Some(entry) = tm.topics.get(level) {
                    nexttopics.push(entry);
                }
            }

            topicmatchs = nexttopics;
            if topicmatchs.is_empty() {
                break;
            }
        }

        for tm in &topicmatchs {
            for s in tm.clients.values() {
                clients.insert(s.session.clone(), s.qos);
            }
        }

        clients
    }
}

impl Topic {
    pub fn new(internal_id: Option<i32>) -> Self {
        Topic {
            topics: HashMap::new(),
            internal_id,
            clients: HashMap::new(),
            message: None,
        }
    }

    pub fn set_internal_id(&mut self, internal_id: Option<i32>) {
        self.internal_id = internal_id;
    }

    pub fn get_internal_id(&self) -> Option<i32> {
        self.internal_id
    }

    pub fn get_num_subscribed(&self) -> usize {
        self.clients.len()
    }

    pub fn set_subscribed(&mut self, session: SessionId, qos: u8) -> bool {
        let session_name = session.session.clone();
        let entry = TopicEntry { session, qos };
        // Updated entry - false, New entry - true
        self.clients.insert(session_name, entry).is_none()
    }

    pub fn set_unsubscribed(&mut self, client: &str) -> bool {
        self.clients.remove(client).is_some()
    }

    pub fn get_subscribed(&self, session: &SessionId) -> Option<u8> {
        self.clients.get(&session.session).map(|entry| entry.qos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! aw {
        ($e:expr) => {
            tokio_test::block_on($e)
        };
    }

    pub fn initialize() {
        config::load_test_config().unwrap();
    }

    pub fn is_subscribed(
        subscrptions: &Subscriptions,
        client: String,
        topic: &str,
    ) -> Result<bool, bool> {
        if subscrptions
            .get_subscribed(topic)
            .contains_key(&get_session_id(&client))
        {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn get_session_id(session: &str) -> SessionId {
        SessionId {
            session: session.to_string(),
            internal_id: 1,
            session_generation: 12345,
        }
    }

    #[test]
    fn test_subscribe_single() {
        initialize();
        let root = slog::Logger::root(slog::Discard, slog::o!());
        let mut s = Subscriptions::new(BrokerId::test_broker(), root, None);
        assert_eq!(
            is_subscribed(&s, String::from("user1"), &String::from("test")),
            Ok(false)
        );
        assert_eq!(
            aw!(s.subscribe(get_session_id("user1"), &String::from("test"), 0)),
            Ok(0)
        );
        assert_eq!(
            is_subscribed(&s, String::from("user1"), &String::from("test")),
            Ok(true)
        );

        assert_eq!(
            is_subscribed(&s, String::from("user1"), &String::from("topic2")),
            Ok(false)
        );

        assert_eq!(
            aw!(s.subscribe(get_session_id("user1"), &String::from("test/topic1"), 0)),
            Ok(0)
        );
        assert_eq!(
            is_subscribed(&s, String::from("user1"), &String::from("test/topic1")),
            Ok(true)
        );
    }

    #[test]
    fn test_subscribe_single_wildcard() {
        initialize();
        let root = slog::Logger::root(slog::Discard, slog::o!());
        let mut s = Subscriptions::new(BrokerId::test_broker(), root, None);
        assert_eq!(
            is_subscribed(&s, String::from("user1"), &String::from("test")),
            Ok(false)
        );
        assert_eq!(
            aw!(s.subscribe(get_session_id("user1"), &String::from("#"), 0)),
            Ok(0)
        );
        assert_eq!(
            is_subscribed(&s, String::from("user1"), &String::from("test")),
            Ok(true)
        );
        assert_eq!(
            is_subscribed(&s, String::from("user1"), &String::from("topic/topic1")),
            Ok(true)
        );
    }

    #[test]
    fn test_unsubscribe_single() {
        initialize();
        let root = slog::Logger::root(slog::Discard, slog::o!());
        let mut s = Subscriptions::new(BrokerId::test_broker(), root, None);

        assert_eq!(
            is_subscribed(&s, String::from("user1"), &String::from("test")),
            Ok(false)
        );
        assert_eq!(
            aw!(s.subscribe(get_session_id("user1"), &String::from("test"), 0)),
            Ok(0)
        );
        assert_eq!(
            is_subscribed(&s, String::from("user1"), &String::from("test")),
            Ok(true)
        );
        assert_eq!(
            aw!(s.unsubscribe(&get_session_id("user1"), &String::from("test"))),
            Ok(())
        );
        assert_eq!(
            is_subscribed(&s, String::from("user1"), &String::from("test")),
            Ok(false)
        );
    }

    #[test]
    fn test_subscribe_two() {
        initialize();
        let root = slog::Logger::root(slog::Discard, slog::o!());
        let mut s = Subscriptions::new(BrokerId::test_broker(), root, None);

        assert_eq!(
            aw!(s.subscribe(get_session_id("user1"), &String::from("test/topic1"), 0)),
            Ok(0)
        );
        assert_eq!(
            is_subscribed(&s, String::from("user1"), &String::from("test/topic1")),
            Ok(true)
        );
        assert_eq!(
            is_subscribed(&s, String::from("user1"), &String::from("test/topic2")),
            Ok(false)
        );
        assert_eq!(
            is_subscribed(&s, String::from("user1"), &String::from("test")),
            Ok(false)
        );
    }

    #[test]
    fn test_subscribe_two_wildcard() {
        initialize();
        let root = slog::Logger::root(slog::Discard, slog::o!());
        let mut s = Subscriptions::new(BrokerId::test_broker(), root, None);

        assert_eq!(
            aw!(s.subscribe(get_session_id("user1"), &String::from("topics/#"), 0)),
            Ok(0)
        );
        assert_eq!(
            is_subscribed(&s, String::from("user1"), &String::from("test")),
            Ok(false)
        );
        assert_eq!(
            is_subscribed(&s, String::from("user1"), &String::from("topics/topic1")),
            Ok(true)
        );
        assert_eq!(
            is_subscribed(&s, String::from("user1"), &String::from("topics/xyz/abc")),
            Ok(true)
        );
    }

    #[test]
    fn test_subscribe_single_level_wildcard() {
        initialize();
        let root = slog::Logger::root(slog::Discard, slog::o!());
        let mut s = Subscriptions::new(BrokerId::test_broker(), root, None);

        assert_eq!(
            aw!(s.subscribe(get_session_id("user1"), &String::from("sport/tennis/+"), 0)),
            Ok(0)
        );
        assert_eq!(
            is_subscribed(
                &s,
                String::from("user1"),
                &String::from("sport/tennis/player1")
            ),
            Ok(true)
        );
        assert_eq!(
            is_subscribed(
                &s,
                String::from("user1"),
                &String::from("sport/tennis/player2")
            ),
            Ok(true)
        );
        assert_eq!(
            is_subscribed(
                &s,
                String::from("user1"),
                &String::from("sport/tennis/player1/ranking")
            ),
            Ok(false)
        );
    }

    #[test]
    fn test_subscribe_single_level_wildcard_two() {
        initialize();
        let root = slog::Logger::root(slog::Discard, slog::o!());
        let mut s = Subscriptions::new(BrokerId::test_broker(), root, None);

        assert_eq!(
            aw!(s.subscribe(get_session_id("user1"), &String::from("sport/+/player1"), 0)),
            Ok(0)
        );
        assert_eq!(
            aw!(s.subscribe(get_session_id("user2"), &String::from("sport/#"), 0)),
            Ok(0)
        );
        assert_eq!(
            is_subscribed(
                &s,
                String::from("user1"),
                &String::from("sport/tennis/player1")
            ),
            Ok(true)
        );
        assert_eq!(
            is_subscribed(
                &s,
                String::from("user1"),
                &String::from("sport/tennis/player2")
            ),
            Ok(false)
        );
        assert_eq!(
            is_subscribed(
                &s,
                String::from("user1"),
                &String::from("sport/golf/player1")
            ),
            Ok(true)
        );
        assert_eq!(
            is_subscribed(
                &s,
                String::from("user1"),
                &String::from("sport/tennis/player1/ranking")
            ),
            Ok(false)
        );
    }
}
