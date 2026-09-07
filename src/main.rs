extern crate base64;
extern crate bytes;
extern crate crypto;
extern crate rand;
extern crate tokio;
extern crate tokio_util;
#[macro_use]
extern crate log;
extern crate env_logger;
#[macro_use]
extern crate prometheus;
extern crate hyper;
#[macro_use]
extern crate lazy_static;

use std::env;

extern crate slog;
extern crate slog_async;
extern crate slog_json;
extern crate slog_term;

use env_logger::Env;
use signal_hook::{consts::SIGINT, consts::SIGTERM, iterator::Signals};
use std::thread;

mod messages;

mod client;

mod config;
mod logging;
mod metrics;

mod server;

mod persist;
mod sessions;

mod auth;

mod broker;

use broker::BrokerId;

use server::MqttServer;

#[tokio::main]
async fn main() {
    if let Ok(mut signals) = Signals::new([SIGINT, SIGTERM]) {
        thread::spawn(move || {
            if let Some(sig) = signals.forever().next() {
                println!("Received signal {:?}", sig);
                std::process::exit(0);
            }
        });
    }

    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        println!("No config supplied");
        std::process::exit(1);
    }
    let config_file_name = &args[1];

    if let Err(err) = config::load_config(config_file_name).await {
        println!("Error loading config: {}", err);
        std::process::exit(1);
    }

    let uuid = config::get_uuid();
    let logger = logging::get_logger(uuid);
    let version = option_env!("PROJECT_VERSION").unwrap_or(env!("CARGO_PKG_VERSION"));

    slog::info!(logger, "Squillion.io MQTT Broker starting");
    slog::info!(logger, "Build: {}", version);

    let mut handles = vec![];

    let metrics_logger = logger.clone();
    let metricstask = tokio::spawn(async { metrics::serve(metrics_logger).await });
    handles.push(metricstask);

    let mut server = MqttServer::new(logger);
    let servertask = tokio::spawn(async move { server.listen().await });
    handles.push(servertask);

    futures::future::join_all(handles).await;
}
