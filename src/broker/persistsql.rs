use async_trait::async_trait;

use super::router::SessionId;
use crate::broker::persist::PersistProvider;
use crate::broker::persist::PersistedTopic;
use crate::broker::BrokerError;
use crate::broker::BrokerId;
use crate::messages::MQTTMessagePublish;
use deadpool_sqlite::rusqlite::named_params;
use deadpool_sqlite::Pool;

pub struct PersistTopicSQL {
    broker_id: BrokerId,
    pool: Pool,
}

impl PersistTopicSQL {
    pub fn new(broker_id: BrokerId, database_pool: Pool) -> PersistTopicSQL {
        PersistTopicSQL {
            broker_id,
            pool: database_pool,
        }
    }

    async fn create_db(&mut self) -> Result<u8, BrokerError> {
        let query = "
            CREATE TABLE sessions (
                id INTEGER NOT NULL PRIMARY KEY,
                name text NOT NULL,
                generation bigint NOT NULL,
                next_mid integer NOT NULL DEFAULT (1)
            );

            CREATE TABLE messages (
                id INTEGER NOT NULL PRIMARY KEY,
                qos smallint NOT NULL,
                topic text NOT NULL,
                payload bytea NOT NULL
            );

            CREATE TABLE topics (
                id INTEGER PRIMARY KEY,
                topic text NOT NULL,
                retained_message_id integer NULL,
                FOREIGN KEY (retained_message_id) REFERENCES messages (id)
            );

            CREATE TABLE subscriptions (
                session_id INTEGER NOT NULL,
                topic_id integer NOT NULL,
                qos smallint NOT NULL,
                FOREIGN KEY (session_id) REFERENCES sessions (id),
                FOREIGN KEY (topic_id) REFERENCES topics (id)
            );

            CREATE TABLE qosin (
                qosin_id INTEGER PRIMARY KEY,
                session_id integer NOT NULL,
                received timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                mid integer NOT NULL,
                FOREIGN KEY (session_id) REFERENCES sessions (id)
            );

            CREATE TABLE qosout (
                qosout_id INTEGER PRIMARY KEY,
                session_id integer NOT NULL,
                message_id integer,
                received timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                type integer NOT NULL,
                mid integer NOT NULL,
                dup boolean NOT NULL DEFAULT(FALSE),
                FOREIGN KEY (session_id) REFERENCES sessions (id),
                FOREIGN KEY (message_id) REFERENCES messages (id)
            );

            CREATE TABLE version (
                version integer NOT NULL
            );

            INSERT INTO version(version) VALUES (1);
            ";

        let client = &self
            .pool
            .get()
            .await
            .map_err(|_| BrokerError::PersistError("No database client".to_owned()))?;

        client
            .interact(move |client| {
                client.execute_batch(query).map_err(|result| {
                    BrokerError::PersistError(format!("Error creating database: {:?}", result))
                })?;

                Ok(0)
            })
            .await
            .map_err(|_| BrokerError::PersistError("Failed to get sqlite client".to_owned()))?
    }

    async fn get_database_version(&mut self) -> Option<u32> {
        if let Ok(client) = &self.pool.get().await {
            match client
                .interact(move |client| {
                    client
                        .query_row("SELECT * FROM version", [], |row| row.get::<_, u32>(0))
                        .ok()
                })
                .await
            {
                Ok(ret) => ret,
                _ => None,
            }
        } else {
            None
        }
    }
}

#[async_trait]
impl PersistProvider for PersistTopicSQL {
    async fn create_persistent_connection(&mut self) -> Result<u8, BrokerError> {
        Ok(0)
    }

    async fn init_persistent_store(&mut self) -> Result<u8, BrokerError> {
        match self.get_database_version().await {
            Some(1) => (),
            None => {
                self.create_db().await?;
            }
            Some(_val) => {
                return Err(BrokerError::PersistError(
                    "Unknown database version".to_owned(),
                ));
            }
        };

        Ok(0)
    }

    async fn load_persistent_topics(&mut self) -> Result<Vec<PersistedTopic>, BrokerError> {
        let client = &self
            .pool
            .get()
            .await
            .map_err(|_| BrokerError::PersistError("Failed to get db client".to_owned()))?;

        client
            .interact(|client| {
                let mut stmt = client
                    .prepare("SELECT id,topic,retained_message_id FROM topics")
                    .map_err(|err| BrokerError::PersistError(format!("{:}", err)))?;
                let mut topics = Vec::new();

                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, i32>(0),
                            row.get::<_, std::string::String>(1),
                            row.get::<_, Option<i32>>(2),
                        ))
                    })
                    .map_err(|_| {
                        BrokerError::PersistError("Failed to load persistent topics".to_owned())
                    })?;

                for row in rows {
                    let r = row.unwrap();
                    let topic_internal_id: i32 = r.0.unwrap();
                    let topic_name: std::string::String = r.1.unwrap();
                    let topic_retained_message_id: Option<i32> = r.2.unwrap();

                    let topic = PersistedTopic {
                        topic_id: topic_internal_id,
                        topic_name,
                        retained_message_id: topic_retained_message_id,
                    };

                    topics.push(topic);
                }
                Ok(topics)
            })
            .await
            .map_err(|_| BrokerError::PersistError("Failed to get sqlite client".to_owned()))?
    }

    async fn load_persistent_message(
        &mut self,
        message_id: i32,
    ) -> Result<MQTTMessagePublish, BrokerError> {
        let client = &self
            .pool
            .get()
            .await
            .map_err(|_| BrokerError::PersistError("Failed to get db client".to_owned()))?;

        client
            .interact(move |client| {
                if let Ok(row) = client.query_row(
                    "SELECT id,qos,topic,payload FROM messages WHERE id = :id",
                    named_params! {
                    ":id": message_id,
                    },
                    |row| {
                        Ok((
                            row.get::<_, i32>(0),
                            row.get::<_, i16>(1),
                            row.get::<_, std::string::String>(2),
                            row.get::<_, Vec<u8>>(3),
                        ))
                    },
                ) {
                    // Row 0 is id - message_internal_id = rows.get(0);
                    let qos: i16 = row.1.unwrap();
                    let topic: std::string::String = row.2.unwrap();
                    let data: Vec<u8> = row.3.unwrap();

                    let mut message = MQTTMessagePublish::new();
                    message.set_topic(topic);
                    message.set_qos(qos as u8);
                    message.set_message(data);

                    Ok(message)
                } else {
                    Err(BrokerError::PersistError(
                        "Could not load persistent messages".to_owned(),
                    ))
                }
            })
            .await
            .map_err(|_| BrokerError::PersistError("Failed to get sqlite client".to_owned()))?
    }

    async fn load_persistent_subscriptions(
        &mut self,
        topic_id: i32,
    ) -> Result<Vec<(SessionId, u8)>, BrokerError> {
        let mut result: Vec<(SessionId, u8)> = Vec::new();

        let client = &self
            .pool
            .get()
            .await
            .map_err(|_| BrokerError::PersistError("Failed to get db client".to_owned()))?;

        client
            .interact(move |client| {
                let mut stmt = client
                    .prepare( "SELECT sessions.id, sessions.name, sessions.generation, subscriptions.qos FROM subscriptions
                    INNER JOIN sessions ON sessions.id = subscriptions.session_id
                    WHERE subscriptions.topic_id = :topic_id",
                    )
                    .map_err(|err| BrokerError::PersistError(format!("{:}", err)))?;

                 let rows = stmt.query_map([&topic_id], |row| {
                    Ok((
                        row.get::<_, i32>(0),
                        row.get::<_, std::string::String>(1),
                        row.get::<_, i64>(2),
                        row.get::<_, i16>(3),
                    ))
                }).map_err(|_| {
                    BrokerError::PersistError("Could not load persistent subscriptions".to_owned())
                })?;

                for row in rows {
                    let r = row.unwrap();
                    let session_internal_id: i32 = r.0.unwrap();
                    let session_name: std::string::String = r.1.unwrap();
                    let session_generation: i64 = r.2.unwrap();
                    let qos: i16 = r.3.unwrap();

                    let session = SessionId {
                        session: session_name,
                        internal_id: session_internal_id,
                        session_generation,
                    };

                    result.push((session, qos as u8));
                }

                Ok(result)

            })
            .await
            .map_err(|_| BrokerError::PersistError("Failed to get sqlite client".to_owned()))?
    }

    async fn persist_topic(&mut self, topic: String) -> Result<i32, BrokerError> {
        let client = &self
            .pool
            .get()
            .await
            .map_err(|_| BrokerError::PersistError("Failed to get db client".to_owned()))?;

        client
            .interact(move |client| {
                let topic_id = client
                    .query_row(
                        "INSERT INTO topics(topic) VALUES (:topic) RETURNING id",
                        named_params! {
                        ":topic": topic,
                        },
                        |row| row.get::<_, i32>(0),
                    )
                    .map_err(|err| {
                        BrokerError::PersistError(format!("Persist topic failed: {:?}", err))
                    })?;

                // Returns topic id
                Ok(topic_id)
            })
            .await
            .map_err(|_| BrokerError::PersistError("Failed to get sqlite client".to_owned()))?
    }

    async fn persist_delete_topic(&mut self, topic_id: i32) -> Result<i32, BrokerError> {
        let client = &self
            .pool
            .get()
            .await
            .map_err(|_| BrokerError::PersistError("Failed to get db client".to_owned()))?;

        client
            .interact(move |client| {
                if let Ok(_result) = client.execute(
                    "DELETE FROM topics WHERE id = :id",
                    named_params! {
                    ":id": topic_id,
                    },
                ) {
                    Ok(topic_id)
                } else {
                    Err(BrokerError::PersistError(
                        "Persist delete topic failed".to_owned(),
                    ))
                }
            })
            .await
            .map_err(|_| BrokerError::PersistError("Failed to get sqlite client".to_owned()))?
    }

    async fn persist_subscribe(
        &mut self,
        session_id: i32,
        topic_id: i32,
        qos: u8,
    ) -> Result<bool, BrokerError> {
        let client = &self
            .pool
            .get()
            .await
            .map_err(|_| BrokerError::PersistError("Failed to get db client".to_owned()))?;

        client
            .interact(move |client| {
                match client.execute(
                    "INSERT INTO subscriptions(session_id, topic_id, qos) VALUES (:session_id, :topic_id, :qos)",
                    named_params! {
                    ":session_id": session_id,
                    ":topic_id": topic_id,
                    ":qos": (qos as i16),
                    },
                ) {
                    Ok(_) => Ok(true),
                    Err(_reason) => Err(BrokerError::PersistError(
                        "Persist subscribe failed".to_owned(),
                    )),
                }
            })
            .await
            .map_err(|_| BrokerError::PersistError("Failed to get sqlite client".to_owned()))?
    }

    async fn persist_update_subscribe(
        &mut self,
        session_id: i32,
        topic_id: i32,
        qos: u8,
    ) -> Result<bool, BrokerError> {
        let client = &self
            .pool
            .get()
            .await
            .map_err(|_| BrokerError::PersistError("Failed to get db client".to_owned()))?;

        client
            .interact(move |client| {
                match client.execute(
                    "UPDATE subscriptions SET qos = :qos WHERE session_id = :session_id AND topic_id = :topic_id",
                    named_params! {
                    ":session_id": session_id,
                    ":topic_id": topic_id,
                    ":qos": qos as i16,
                    },
                ) {
                    Ok(_) => Ok(true),
                    Err(_reason) => Err(BrokerError::PersistError(
                        "Update subscribe failed".to_owned(),
                    )),
                }
            })
            .await
            .map_err(|_| BrokerError::PersistError("Failed to get sqlite client".to_owned()))?
    }

    async fn persist_unsubscribe(
        &mut self,
        session_id: i32,
        topic_id: i32,
    ) -> Result<bool, BrokerError> {
        let client = &self
            .pool
            .get()
            .await
            .map_err(|_| BrokerError::PersistError("Failed to get db client".to_owned()))?;

        client
            .interact(move |client| {
                match client.execute(
                    "DELETE FROM subscriptions WHERE session_id = :session_id AND topic_id = :topic_id",
                    named_params! {
                    ":session_id": session_id,
                    ":topic_id": topic_id,
                    },
                ) {
                    Ok(_) => Ok(true),
                    Err(_reason) => Err(BrokerError::PersistError("Unsubscribe failed".to_owned())),
                }
            })
            .await
            .map_err(|_| BrokerError::PersistError("Failed to get sqlite client".to_owned()))?
    }

    async fn persist_retain(
        &mut self,
        topic_id: i32,
        msg: &MQTTMessagePublish,
    ) -> Result<i32, BrokerError> {
        let client = &self
            .pool
            .get()
            .await
            .map_err(|_| BrokerError::PersistError("Failed to get db client".to_owned()))?;

        let qos = msg.get_qos();
        let topic = msg.get_topic().clone();
        let message = msg.get_message().clone();
        client
            .interact(move |client| {
                if let Ok(retid) = client.query_row(
                    "INSERT INTO messages(qos, topic, payload) VALUES (:qos, :topic, :payload) RETURNING id",
                    named_params! {
                    ":qos": (qos as i16),
                    ":topic": topic,
                    ":payload": message,
                    },
                    |row| row.get::<_, i32>(0),
                ) {
                    // And then check that we got back the same string we sent over.
                    if client
                        .execute(
                            "UPDATE topics SET retained_message_id = :retained_message_id WHERE id = :id",
                            &[(":retained_message_id", &retid), (":id", &topic_id)],
                        )
                        .is_err()
                    {
                        Err(BrokerError::PersistError(
                            "Message retain failed".to_owned(),
                        ))
                    } else {
                        Ok(retid)
                    }
                } else {
                    Err(BrokerError::PersistError(
                        "Message retain failed".to_owned(),
                    ))
                }
            })
            .await
            .map_err(|_| BrokerError::PersistError("Failed to get sqlite client".to_owned()))?
    }

    async fn persist_delete_retain(&mut self, topic_id: i32) -> Result<(), BrokerError> {
        let client = &self
            .pool
            .get()
            .await
            .map_err(|_| BrokerError::PersistError("Failed to get db client".to_owned()))?;

        client
            .interact(move |client| {
                if let Ok(transaction) = client.transaction() {
                    let message_id: Option<i32>;

                    if let Ok(mid) = transaction.query_row(
                        "SELECT retained_message_id FROM topics WHERE id = :id",
                        &[(":id", &topic_id)],
                        |row| row.get(0),
                    ) {
                        message_id = mid;
                    } else {
                        return Err(BrokerError::PersistError(
                            "Delete retained message getting message id failed".to_owned(),
                        ));
                    }

                    if transaction
                        .execute(
                            "UPDATE topics SET retained_message_id = NULL WHERE id = :id",
                            &[(":id", &topic_id)],
                        )
                        .is_err()
                    {
                        return Err(BrokerError::PersistError(
                            "Delete retained message from topic failed".to_owned(),
                        ));
                    }

                    if let Some(mid) = message_id {
                        if let Ok(_rows) = transaction
                            .execute("DELETE FROM messages WHERE id = :id", &[(":id", &mid)])
                        {
                            // And then check that we got back the same string we sent over.
                        } else {
                            return Err(BrokerError::PersistError(
                                "Delete retained message failed".to_owned(),
                            ));
                        }
                    }

                    transaction.commit().map_err(|_| {
                        BrokerError::PersistError(
                            "Delete retained message commit failed".to_owned(),
                        )
                    })?;

                    Ok(())
                } else {
                    Err(BrokerError::PersistError(
                        "Delete retained message unable to start transaction".to_owned(),
                    ))
                }
            })
            .await
            .map_err(|_| BrokerError::PersistError("Failed to get sqlite client".to_owned()))?
    }
}
