use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

use std::env;
use std::sync::Mutex;

lazy_static! {
    static ref GLOBALCONFIG: GlobalConfig = GlobalConfig::new();
}

pub fn generate_uuid() -> String {
    let rand_string: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(30)
        .map(char::from)
        .collect();

    rand_string
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct UsersConfig {
    pub username: String,
    pub password: String,
    pub tenant: Option<String>,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct ListenerConfig {
    pub port: u16,
    #[serde(default = "default_bool_false")]
    pub websocket: bool,
    pub tlscrt: Option<String>,
    pub tlskey: Option<String>,
}

#[derive(Deserialize, Debug, Default)]
struct SystemConfig {
    #[serde(default = "default_resource")]
    mqtt_database_host: String,
    #[serde(default = "default_resource")]
    mqtt_database_db: String,
    #[serde(default = "default_resource")]
    mqtt_database_username: String,
    #[serde(default = "default_resource")]
    mqtt_database_password: String,

    #[serde(default = "default_bool_false")]
    logging_remote: bool,

    #[serde(default = "default_resource")]
    logging_endpoint: String,

    auth_method: String,
    #[serde(default = "default_resource")]
    password_file: String,

    persist_method: String,
    #[serde(default = "default_resource")]
    persist_data_store: String,

    /// Maximum MQTT packet size accepted from any client (bytes).
    /// Packets claiming a remaining-length above this value are rejected before
    /// the payload bytes are read, preventing pre-auth memory-exhaustion.
    /// Default: 1 MiB (1_048_576).
    #[serde(default = "default_max_packet_size")]
    max_packet_size: usize,

    /// Seconds to wait for a CONNECT packet after the TCP/TLS/WS handshake.
    /// Connections that do not send CONNECT within this window are closed.
    /// Default: 10 seconds.
    #[serde(default = "default_connect_timeout")]
    connect_timeout: u64,

    listeners: Vec<ListenerConfig>,

    users: Vec<UsersConfig>,
}

fn default_resource() -> String {
    "".to_string()
}

fn default_bool_false() -> bool {
    false
}

fn default_max_packet_size() -> usize {
    1_048_576 // 1 MiB
}

fn default_connect_timeout() -> u64 {
    10
}

struct GlobalConfig {
    uuid: String,
    hostname: String,
    config: Mutex<SystemConfig>,
}

impl GlobalConfig {
    fn new() -> GlobalConfig {
        GlobalConfig {
            uuid: generate_uuid(),
            hostname: hostname::get().unwrap().into_string().unwrap(),
            config: Mutex::new(SystemConfig::default()),
        }
    }
}

#[cfg(test)]
pub fn load_test_config() -> Result<(), Box<dyn std::error::Error>> {
    GLOBALCONFIG.config.lock().unwrap().persist_method = "test".to_owned();
    Ok(())
}

#[cfg(test)]
pub fn load_sqlite_test_config() -> Result<(), Box<dyn std::error::Error>> {
    GLOBALCONFIG.config.lock().unwrap().persist_method = "sqlite".to_owned();

    GLOBALCONFIG.config.lock().unwrap().persist_data_store = "./tests/data".to_owned();
    Ok(())
}

pub fn get_uuid() -> &'static String {
    &GLOBALCONFIG.uuid
}

pub fn get_hostname() -> &'static String {
    &GLOBALCONFIG.hostname
}

async fn vault_login(vault_addr: &str, vault_roleid: &str) -> Result<String, &'static str> {
    let body = format!("{{\"role_id\":\"{}\"}}", vault_roleid);
    let request_url = format!("{host}/v1/auth/approle/login", host = vault_addr);

    let client = reqwest::Client::new();
    let response_result = client.post(&request_url).body(body).send().await;

    if let Ok(response) = response_result {
        let v_result: Result<Value, reqwest::Error> = response.json().await;
        if let Ok(v) = v_result {
            if let Some(auth) = v.get("auth") {
                if let Some(client_token) = auth.get("client_token") {
                    return Ok(client_token.to_string());
                }
            }
        }
    };

    Err("Error logging into vault")
}

async fn vault_get_kv(
    vault_addr: &str,
    vault_token: &str,
    secret: &str,
    key: &str,
) -> Result<String, String> {
    let request_url = format!(
        "{host}/v1/secret/data/{secret}",
        host = vault_addr,
        secret = secret
    );

    let client = reqwest::Client::new();

    let response_result = client
        .get(&request_url)
        .header("X-Vault-Token", vault_token)
        .send()
        .await;

    if let Ok(response) = response_result {
        let v_result: Result<Value, reqwest::Error> = response.json().await;
        if let Ok(v) = v_result {
            if let Some(response) = v.get("data") {
                if let Some(data) = response.get("data") {
                    if let Some(value) = data.get(key) {
                        let mut unqoute = value.to_string();
                        unqoute = unqoute.strip_prefix('"').unwrap().to_string();
                        unqoute = unqoute.strip_suffix('"').unwrap().to_string();
                        return Ok(unqoute);
                    }
                }
            }
        }
    };

    Err(format!("Error fetching secret {} key {}", secret, key))
}

async fn get_secret(secret: &str) -> Result<String, String> {
    if let Ok(vault_addr) = env::var("VAULT_ADDR") {
        if let Ok(vault_roleid) = env::var("VAULT_ROLEID") {
            // Get secret
            if let Ok(mut token) = vault_login(&vault_addr, &vault_roleid).await {
                token = token.trim_matches('"').to_string();

                let split = secret.split('/');
                let mut items = split.collect::<Vec<&str>>();
                let key = items.pop();
                let path = items.join("/");

                if let Some(key_value) = key {
                    vault_get_kv(&vault_addr, &token, &path, key_value).await
                } else {
                    Err(format!("Error fetching secret {}", secret))
                }
            } else {
                Err("Error logging into vault".to_string())
            }
        } else {
            Err("VAULT_ROLEID not set".to_string())
        }
    } else {
        Err("VAULT_ADDR not set".to_string())
    }
}

async fn load_secrets(settings: &mut BTreeMap<String, String>) -> Result<(), String> {
    for (_key, value) in settings.iter_mut() {
        if let Some(secret) = value.strip_prefix("secret:") {
            match get_secret(secret).await {
                Ok(secret_value) => {
                    *value = secret_value;
                }
                Err(error) => {
                    return Err(error);
                }
            };
        }
    }

    Ok(())
}

pub async fn load_config(file: &str) -> Result<(), Box<dyn std::error::Error>> {
    let f = std::fs::File::open(file)?;
    let d: SystemConfig = serde_yaml::from_reader(f)?;
    /*
    if let Err(err) = load_secrets(&mut d).await {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Error retreiving secrets: {}", err),
        )));
    }
    */
    *GLOBALCONFIG.config.lock().unwrap() = d;
    Ok(())
}

pub fn get_listeners() -> Vec<ListenerConfig> {
    GLOBALCONFIG.config.lock().unwrap().listeners.clone()
}

pub fn get_users() -> Vec<UsersConfig> {
    GLOBALCONFIG.config.lock().unwrap().users.clone()
}

pub fn get_string(key: &str) -> Option<String> {
    match key {
        "auth_method" => Some(GLOBALCONFIG.config.lock().unwrap().auth_method.clone()),
        "persist_method" => Some(GLOBALCONFIG.config.lock().unwrap().persist_method.clone()),
        "logging_endpoint" => Some(GLOBALCONFIG.config.lock().unwrap().logging_endpoint.clone()),
        "mqtt_database_host" => Some(
            GLOBALCONFIG
                .config
                .lock()
                .unwrap()
                .mqtt_database_host
                .clone(),
        ),
        "mqtt_database_db" => Some(GLOBALCONFIG.config.lock().unwrap().mqtt_database_db.clone()),
        "mqtt_database_username" => Some(
            GLOBALCONFIG
                .config
                .lock()
                .unwrap()
                .mqtt_database_username
                .clone(),
        ),
        "mqtt_database_password" => Some(
            GLOBALCONFIG
                .config
                .lock()
                .unwrap()
                .mqtt_database_password
                .clone(),
        ),
        "password_file" => Some(GLOBALCONFIG.config.lock().unwrap().password_file.clone()),
        "persist_data_store" => Some(
            GLOBALCONFIG
                .config
                .lock()
                .unwrap()
                .persist_data_store
                .clone(),
        ),
        _ => None,
    }
}

pub fn get_bool(key: &str) -> Option<bool> {
    match key {
        "logging_remote" => Some(GLOBALCONFIG.config.lock().unwrap().logging_remote),
        "enable_validation" | "enable_policy" => Some(false), // reserved for OPA/schema integration
        _ => None,
    }
}

/// Maximum accepted MQTT remaining-length (bytes).  Clients claiming a larger
/// packet are disconnected before the payload is read.
pub fn get_max_packet_size() -> usize {
    GLOBALCONFIG.config.lock().unwrap().max_packet_size
}

/// Seconds to wait for CONNECT after the transport handshake.
pub fn get_connect_timeout() -> u64 {
    GLOBALCONFIG.config.lock().unwrap().connect_timeout
}

lazy_static! {
    static ref GLOBALSTATE: GlobalState = GlobalState::new();
}

struct State {
    livez: bool,
    readyz: bool,
}

impl State {
    fn new() -> State {
        State {
            livez: false,
            readyz: false,
        }
    }
}

struct GlobalState {
    state: Mutex<State>,
}

impl GlobalState {
    fn new() -> GlobalState {
        GlobalState {
            state: Mutex::new(State::new()),
        }
    }
}

pub fn get_livez() -> bool {
    GLOBALSTATE.state.lock().unwrap().livez
}

pub fn get_readyz() -> bool {
    GLOBALSTATE.state.lock().unwrap().readyz
}

pub fn set_readyz() {
    GLOBALSTATE.state.lock().unwrap().livez = true;
    GLOBALSTATE.state.lock().unwrap().readyz = true;
}
