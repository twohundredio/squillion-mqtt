use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::CONTENT_TYPE;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use std::convert::Infallible;
use std::net::SocketAddr;
use tokio::net::TcpListener;

use prometheus::{Encoder, TextEncoder};

use crate::config;

type ResponseBody = Full<Bytes>;

async fn metrics(_req: Request<Incoming>) -> Result<Response<ResponseBody>, Infallible> {
    let metric_families = prometheus::gather();
    let mut buffer = vec![];

    let encoder = TextEncoder::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(Full::new(Bytes::from(buffer)))
        .unwrap();

    Ok(response)
}

async fn livez(_req: Request<Incoming>) -> Result<Response<ResponseBody>, Infallible> {
    let status = if config::get_livez() { 200 } else { 500 };

    let response = hyper::Response::builder()
        .status(status)
        .body(Full::new(Bytes::from_static(b"livez")))
        .unwrap();

    Ok(response)
}

async fn readyz(_req: Request<Incoming>) -> Result<Response<ResponseBody>, Infallible> {
    let status = if config::get_readyz() { 200 } else { 500 };

    let response = hyper::Response::builder()
        .status(status)
        .body(Full::new(Bytes::from_static(b"readyz")))
        .unwrap();

    Ok(response)
}

async fn not_found_handler(_req: Request<Incoming>) -> Result<Response<ResponseBody>, Infallible> {
    let response = hyper::Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Full::new(Bytes::from_static(b"NOT FOUND")))
        .unwrap();

    Ok(response)
}

async fn route(req: Request<Incoming>) -> Result<Response<ResponseBody>, Infallible> {
    if req.uri() == "/metrics" {
        metrics(req).await
    } else if req.uri() == "/livez" {
        livez(req).await
    } else if req.uri() == "/healthz" {
        livez(req).await
    } else if req.uri() == "/readyz" {
        readyz(req).await
    } else {
        not_found_handler(req).await
    }
}

pub async fn serve(logger: slog::Logger) {
    let addr: SocketAddr = ([0, 0, 0, 0], 9898).into();

    slog::info!(logger, "Metrics listening address: {:?}", addr);

    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            slog::error!(logger, "metrics server bind error: {}", e);
            return;
        }
    };

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(connection) => connection,
            Err(e) => {
                slog::error!(logger, "metrics server accept error: {}", e);
                continue;
            }
        };
        let logger = logger.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let service = service_fn(route);
            if let Err(e) = Builder::new(TokioExecutor::new())
                .serve_connection(io, service)
                .await
            {
                slog::error!(logger, "metrics connection error: {}", e);
            }
        });
    }
}
