use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use std::sync::Mutex;

use futures::channel::oneshot;

use futures::stream::StreamExt;

use futures::SinkExt;

use std::time::Duration;

use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::codec::Framed;

use native_tls::{Identity, TlsAcceptor};
use ws_stream_tungstenite::*;

use crate::messages;
use crate::messages::codec::MQTTMessage;
use crate::messages::codec::MqttCodec;
use crate::messages::MQTTMessageConnack;

use crate::client::Client;
use crate::client::WillMessage;

use crate::broker::BrokerId;

use crate::auth::AuthRequest;
use crate::auth::AuthResponse;
use crate::auth::AuthService;
use crate::auth::AuthTx;

use crate::broker::BrokerMessage;
use crate::broker::BrokerTx;
use crate::broker::MqttBroker;
use crate::broker::{SessionRequest, SessionRequestRx, SessionRequestTx};

use crate::client::clientworker::MQTTClientWorker;
use crate::sessions::sessionworker::NewClient;
use crate::sessions::sessionworker::SessionMessage;

use deadpool_sqlite::Pool;

use crate::config;

lazy_static! {
    static ref CONNECT_COUNTER: prometheus::Counter = register_counter!(opts!(
        "mqtt_connect",
        "Total number of connections made.",
        labels! {"handler" => "all",}
    ))
    .unwrap();
}

pub struct MqttServer {
    logger: slog::Logger,
    auth: AuthService,
    brokers: Arc<Mutex<HashMap<BrokerId, BrokerTx>>>,
    database_pool: Option<Pool>,
}

impl MqttServer {
    pub fn new(logger: slog::Logger) -> MqttServer {
        let auth_service = AuthService::new(logger.clone());

        MqttServer {
            logger,
            auth: auth_service,
            brokers: Arc::new(Mutex::new(HashMap::new())),
            database_pool: None,
        }
    }

    pub async fn listen(&mut self) {
        let listeners = config::get_listeners();
        self.auth.process().unwrap();

        let mut listener_handles = vec![];
        for listener in listeners {
            let server_broker_map = self.brokers.clone();
            let server_logger = self.logger.clone();
            let server_auth_tx = self.auth.get_tx().unwrap().clone();
            let server_database_pool = self.database_pool.clone();

            let task = tokio::spawn(async move {
                create_listener(
                    server_logger,
                    server_broker_map,
                    server_auth_tx,
                    server_database_pool,
                    listener,
                )
                .await;
            });

            listener_handles.push(task);
        }

        // Set ready
        config::set_readyz();

        futures::future::join_all(listener_handles).await;
    }
}

fn create_tls_identity(tlscrt: Option<String>, tlskey: Option<String>) -> Result<Identity, String> {
    const PASSWORD: &str = "nosecret";

    let mut server_cert_file = File::open(&tlscrt.unwrap()).map_err(|f| format!("Error: {}", f))?;
    let mut server_cert = vec![];
    server_cert_file
        .read_to_end(&mut server_cert)
        .map_err(|f| format!("Error: {}", f))?;
    let cert = openssl::x509::X509::from_pem(&server_cert).map_err(|f| format!("Error: {}", f))?;

    let mut server_key_file = File::open(&tlskey.unwrap()).map_err(|f| format!("Error: {}", f))?;
    let mut server_key = vec![];
    server_key_file
        .read_to_end(&mut server_key)
        .map_err(|f| format!("Error: {}", f))?;
    let key = openssl::rsa::Rsa::private_key_from_pem(&server_key)
        .map_err(|f| format!("Error: {}", f))?;
    let pkey = openssl::pkey::PKey::from_rsa(key).map_err(|f| format!("Error: {}", f))?;

    let pkcs12 = openssl::pkcs12::Pkcs12::builder()
        .build(PASSWORD, "", &*pkey, &cert)
        .map_err(|f| format!("Error: {}", f))?;

    // The DER-encoded bytes of the archive
    let der = pkcs12.to_der().map_err(|f| format!("Error: {}", f))?;
    let identity = Identity::from_pkcs12(&der, PASSWORD).map_err(|f| format!("Error: {}", f))?;

    Ok(identity)
}

pub async fn create_listener(
    mut server_logger: slog::Logger,
    server_broker_map: Arc<Mutex<HashMap<BrokerId, BrokerTx>>>,
    auth_tx: AuthTx,
    database_pool: Option<Pool>,
    listener_config: config::ListenerConfig,
) {
    server_logger = server_logger.new(slog::o!("_connection_type" => "tlswsmqtt"));
    let listen_logger = server_logger.clone();
    let mut tls_acceptor = None;

    let tls = listener_config.tlskey.is_some() && listener_config.tlscrt.is_some();
    let ws = listener_config.websocket;

    // TLS identity
    if tls {
        match create_tls_identity(listener_config.tlscrt, listener_config.tlskey) {
            Ok(identity) => {
                let acceptor = TlsAcceptor::new(identity).unwrap();
                tls_acceptor = Some(tokio_native_tls::TlsAcceptor::from(acceptor));
            }
            Err(err) => {
                slog::error!(listen_logger, "Error loading tls: {}", err);
                return;
            }
        }
    }

    let addr = format!("[::]:{}", listener_config.port);
    let listener = TcpListener::bind(&addr).await.unwrap();

    let config_str = format!(
        "TCP {}{}",
        if tls { "TLS " } else { "" },
        if ws { "WS " } else { "" }
    );

    slog::info!(server_logger, "MQTT {}Listening on {}", config_str, addr);

    // Pull out a stream of sockets for incoming connections
    let server = {
        async move {
            let mut incoming = TcpListenerStream::new(listener);
            while let Some(conn) = incoming.next().await {
                match conn {
                    Err(e) => {
                        slog::error!(listen_logger, "accept failed = {:?}", e);
                    }
                    Ok(sock) => {
                        if let Ok(peer_address) = sock.peer_addr() {
                            let src_address = peer_address.to_string();
                            let tls_acceptor = tls_acceptor.clone();

                            let connection_auth_tx = auth_tx.clone();
                            let broker_map = server_broker_map.clone();
                            let logger = listen_logger.clone();
                            let pool = database_pool.clone();

                            tokio::spawn(async move {
                                if tls {
                                    match tls_acceptor.unwrap().accept(sock).await {
                                        Ok(tls_stream) => {
                                            if ws {
                                                if let Ok(s) = async_tungstenite::accept_async(
                                                    async_tungstenite::tokio::TokioAdapter::new(
                                                        tls_stream,
                                                    ),
                                                )
                                                .await
                                                {
                                                    let ws_stream = WsStream::new(s);

                                                    connect_stream(
                                                        ws_stream,
                                                        src_address,
                                                        logger,
                                                        broker_map,
                                                        connection_auth_tx,
                                                        pool,
                                                    )
                                                    .await;
                                                } else {
                                                    slog::warn!(
                                                        logger,
                                                        "TLS Websocket accept error"
                                                    );
                                                }
                                            } else {
                                                connect_stream(
                                                    tls_stream,
                                                    src_address,
                                                    logger,
                                                    broker_map,
                                                    connection_auth_tx,
                                                    pool,
                                                )
                                                .await;
                                            }
                                        }
                                        Err(e) => {
                                            slog::warn!(logger, "TLS accept error: {}", e);
                                        }
                                    }
                                } else if ws {
                                    if let Ok(s) = async_tungstenite::accept_async(
                                        async_tungstenite::tokio::TokioAdapter::new(sock),
                                    )
                                    .await
                                    {
                                        let ws_stream = WsStream::new(s);

                                        connect_stream(
                                            ws_stream,
                                            src_address,
                                            logger,
                                            broker_map,
                                            connection_auth_tx,
                                            pool,
                                        )
                                        .await;
                                    } else {
                                        slog::warn!(logger, "Websocket accept error");
                                    }
                                } else {
                                    connect_stream(
                                        sock,
                                        src_address,
                                        logger,
                                        broker_map,
                                        connection_auth_tx,
                                        pool,
                                    )
                                    .await;
                                }
                            });
                        }
                    }
                }
            }
        }
    };

    server.await;

    slog::info!(server_logger, "MQTT {}listener stopped", config_str);
}

async fn connect_stream<T>(
    stream: T,
    src_address: String,
    logger: slog::Logger,
    server_broker_map: Arc<Mutex<HashMap<BrokerId, BrokerTx>>>,
    auth_tx: AuthTx,
    database_pool: Option<Pool>,
) where
    T: AsyncRead + AsyncWrite + std::marker::Unpin + std::marker::Send,
{
    let max_packet_size = config::get_max_packet_size();
    let framed_stream = Framed::new(stream, MqttCodec::new(logger.clone(), max_packet_size));

    let client_connect = connect(&logger, framed_stream, src_address, auth_tx).await;

    if let Some((connect_msg, client)) = client_connect {
        let broker_id = client.get_broker_id();
        let logger = logger.new(slog::o!("tenant" => broker_id.tenant_id.clone(),
             "broker" => broker_id.broker_id.clone()));

        let broker = get_broker(
            logger.clone(),
            server_broker_map,
            client.get_broker_id(),
            database_pool,
        );
        if let Ok(mut broker_tx) = broker {
            let (session_auth_tx, session_auth_rx): (SessionRequestTx, SessionRequestRx) =
                oneshot::channel();

            let session_request = SessionRequest::new(client.get_id().to_string(), session_auth_tx);

            if let Err(e) = broker_tx
                .send(BrokerMessage::GetSessionTx(session_request))
                .await
            {
                slog::warn!(logger, "Error sending client to broker: {}", e);
            } else if let Ok(session_response) = session_auth_rx.await {
                if let Some(mut session_tx) = session_response.session {
                    let mut client_worker =
                        MQTTClientWorker::new(logger.clone(), client, session_tx.clone());
                    let client_tx = client_worker.get_tx().clone();
                    let new_client = NewClient {
                        msg: connect_msg,
                        client: client_tx,
                    };
                    if session_tx
                        .send(SessionMessage::NewClient(new_client))
                        .await
                        .is_ok()
                    {
                        client_worker.run_loop().await;
                    } else {
                        slog::warn!(logger, "Unable to send new client to session");
                    }
                } else {
                    slog::warn!(logger, "No session_tx");
                }
            }
        }
    }
}

pub fn get_broker(
    logger: slog::Logger,
    brokers: Arc<Mutex<HashMap<BrokerId, BrokerTx>>>,
    id: &BrokerId,
    database_pool: Option<Pool>,
) -> Result<BrokerTx, &'static str> {
    match brokers.lock().unwrap().entry(id.clone()) {
        Vacant(entry) => {
            slog::debug!(logger, "New broker {}", id);

            let mut broker = MqttBroker::new(logger, id.clone(), database_pool);
            let broker_tx = broker.get_tx().clone();

            tokio::spawn(async move {
                broker.run_loop().await;
            });
            entry.insert(broker_tx.clone());
            Ok(broker_tx)
        }
        Occupied(entry) => {
            slog::debug!(logger, "Existing broker {}", id);
            Ok(entry.into_mut().clone())
        }
    }
}

type ConnAuthTx = oneshot::Sender<AuthResponse>;
type ConnAuthRx = oneshot::Receiver<AuthResponse>;

async fn connect<T>(
    logger: &slog::Logger,
    mut stream: Framed<T, MqttCodec>,
    src_address: String,
    mut auth_tx: AuthTx,
) -> Option<(messages::MQTTMessageConnect, Client<T>)>
where
    T: AsyncRead + AsyncWrite + std::marker::Unpin,
{
    let timeout_secs = config::get_connect_timeout();
    let next_frame = tokio::time::timeout(Duration::from_secs(timeout_secs), stream.next());

    let result = match next_frame.await {
        Ok(r) => r,
        Err(_elapsed) => {
            slog::debug!(
                logger,
                "Client {} did not send CONNECT within {}s — closing",
                src_address,
                timeout_secs,
            );
            return None;
        }
    };
    match result {
        Some(Ok(message)) => {
            match message {
                MQTTMessage::Connect(msg) => {
                    CONNECT_COUNTER.inc();

                    slog::debug!(logger, "Client Received connect");

                    if msg.get_version() != messages::MQTT311_VERSION {
                        let mut connack = MQTTMessageConnack::new();
                        connack.set_return_code(messages::ReturnCode::UnacceptableVersion);
                        if let Err(e) = stream.send(MQTTMessage::ConnAck(connack)).await {
                            slog::error!(logger, "Unable to send CONNACK to client: {}", e);
                        }
                        return None;
                    }

                    if msg.get_client().is_empty() {
                        let mut connack = MQTTMessageConnack::new();
                        connack.set_return_code(messages::ReturnCode::IdentifierRejected);
                        if let Err(e) = stream.send(MQTTMessage::ConnAck(connack)).await {
                            slog::error!(logger, "Unable to send CONNACK to client: {}", e);
                        }
                        return None;
                    }

                    let will = if msg.has_will() {
                        let will = WillMessage::new(
                            msg.will_topic().clone(),
                            msg.will_message().clone(),
                            msg.will_qos(),
                            msg.will_retain(),
                        );
                        Some(will)
                    } else {
                        None
                    };

                    let (conn_auth_tx, conn_auth_rx): (ConnAuthTx, ConnAuthRx) = oneshot::channel();

                    let auth = AuthRequest::new(
                        msg.get_username().to_string(),
                        msg.get_password().to_string(),
                        conn_auth_tx,
                    );

                    match auth_tx.send(auth).await {
                        Ok(_) => {
                            if let Ok(auth_response) = conn_auth_rx.await {
                                if auth_response.return_code == messages::ReturnCode::Accepted {
                                    let broker = BrokerId {
                                        tenant_id: auth_response.tenant,
                                        broker_id: "default".to_string(),
                                    };
                                    let client = Client::new(
                                        stream,
                                        src_address,
                                        broker,
                                        msg.get_client().to_string(),
                                        msg.keep_alive(),
                                        will,
                                    );
                                    return Some((msg, client));
                                } else {
                                    slog::warn!(
                                        logger,
                                        "Client auth failed. Client: {}. Session: {}",
                                        msg.get_username(),
                                        msg.get_client()
                                    );

                                    let mut connack = MQTTMessageConnack::new();
                                    connack.set_return_code(auth_response.return_code);
                                    if let Err(e) = stream.send(MQTTMessage::ConnAck(connack)).await
                                    {
                                        slog::error!(
                                            logger,
                                            "Unable to send CONNACK to client: {}",
                                            e
                                        );
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            slog::error!(logger, "Unable to send request to auth server: {}", err);

                            let mut connack = MQTTMessageConnack::new();
                            connack.set_return_code(messages::ReturnCode::ServerUnavailable);
                            if let Err(e) = stream.send(MQTTMessage::ConnAck(connack)).await {
                                slog::error!(logger, "Unable to send CONNACK to client: {}", e);
                            }
                        }
                    };
                }
                _ => error!("Unknown message recevied"),
            };
        }
        Some(Err(_)) => {
            slog::debug!(logger, "Client Error receiving message");
        }
        None => {
            slog::debug!(logger, "Client None message");
        }
    };

    None
}
