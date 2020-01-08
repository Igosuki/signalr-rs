use serde_json::{Map, Value};
use std::collections::HashMap;
use std::error::Error;
use std::ops::Deref;
use std::option::NoneError;
use awc::{BoxedSocket, Client};
use awc::ws::{Codec, Frame, Message};
use url::{Url, ParseError};
use actix_codec::Framed;
use std::io;
use futures::stream::{SplitSink, StreamExt};
use awc::error::{WsProtocolError, WsClientError};
use actix::{StreamHandler, Context, Actor, Addr};
use bytes::{Buf, Bytes};
use std::time::{Duration, Instant};
use actix::io::SinkWrite;

#[derive(Debug, Fail)]
enum HubClientError {
    #[fail(display = "invalid data : {:?}", data)]
    InvalidData { data: Vec<String> },
    #[fail(display = "missing key")]
    MissingData,
    #[fail(display = "invalid json data")]
    ParseError,
}

impl From<NoneError> for HubClientError {
    fn from(_: NoneError) -> Self {
        HubClientError::MissingData
    }
}

impl From<serde_json::Error> for HubClientError {
    fn from(_: serde_json::Error) -> Self {
        HubClientError::ParseError
    }
}

pub struct HubClient {
    hb: Instant,
    name: String,
    methods: HashMap<String, String>,
    handler: Box<HubClientHandler>,
    inner: SinkWrite<Message, SplitSink<Framed<BoxedSocket, Codec>, Message>>,
}

impl Actor for HubClient
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        // start heartbeats otherwise server will disconnect after 10 seconds
        println!("Started");
    }

    fn stopped(&mut self, _: &mut Context<Self>) {
        println!("Disconnected");
    }
}

impl HubClient {
    pub async fn new(wss_url: &str, handler: Box<HubClientHandler>) -> Result<Addr<HubClient>, WsClientError> {
        let c = new_ws_client(wss_url).await;
        let (sink, stream) = c?.split();
        Ok(HubClient::create(|ctx| {
            HubClient::add_stream(stream, ctx);
            HubClient {
                inner: SinkWrite::new(sink, ctx),
                handler,
                methods: HashMap::new(),
                name: "".to_string(),
                hb: Instant::now()
            }
        }))
    }

    fn handle_bytes(&self, bytes: Bytes) -> Result<(), HubClientError> {
        let msg: Map<String, Value> = serde_json::from_slice(bytes.bytes()).unwrap();
        let m = msg.get("M");
        match m {
            Some(Value::Array(data)) => {
                data.into_iter().map(|inner_data| {
                    let hub: Option<&Value> = inner_data.get("H");
                    match hub {
                        Some(Value::String(hub_name)) if hub_name.to_lowercase() == self.name => {
                            let m: Option<&Value> = inner_data.get("M");
                            let a: Option<&Value> = inner_data.get("A");
                            match (m, a) {
                                (Some(Value::String(method)), Some(Value::Object(message))) => {
                                    Ok(self.handler.handle(method, message))
                                }
                                _ => {
                                    let m_str = serde_json::to_string(&m)?;
                                    let a_str = serde_json::to_string(&a)?;
                                    Err(HubClientError::InvalidData { data: vec![m_str, a_str] })
                                }
                            }
                        }
                        _ => {
                            let hub_str = serde_json::to_string(&hub)?;
                            Err(HubClientError::InvalidData {
                                data: vec![hub_str],
                            })
                        }
                    }
                }).collect()
            }
            _ => Ok(()),
        }
    }
}

impl StreamHandler<Result<Frame, WsProtocolError>> for HubClient {
    fn handle(&mut self, msg: Result<Frame, WsProtocolError>, ctx: &mut Context<Self>) {
        match msg {
            Ok(Frame::Ping(msg)) => {
                self.hb = Instant::now();
                self.inner.write(Message::Pong(Bytes::copy_from_slice(&msg)));
            }
            Ok(Frame::Text(txt)) => {
                self.handle_bytes(txt);
            }
            _ => {
                ();
            }
        }
    }
}

impl actix::io::WriteHandler<WsProtocolError> for HubClient
{}

pub trait HubClientHandler {
    fn handle(&self, method: &str, message: &Map<String, Value>);
}

pub async fn new_ws_client(url: &str) -> Result<Framed<BoxedSocket, Codec>, WsClientError> {
    let ssl = {
        let mut ssl = openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls()).unwrap();
        let _ = ssl.set_alpn_protos(b"\x08http/1.1");
        ssl.build()
    };
    let connector = awc::Connector::new().ssl(ssl).finish();
    let client = Client::build()
        .connector(connector)
        .finish();

    let (response, framed) = client
        .ws(url)
        .connect()
        .await?;

    Ok(framed)
}
