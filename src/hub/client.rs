use serde_json::{Map, Value};
use std::collections::HashMap;
use std::error::Error;
use std::ops::Deref;
use std::option::NoneError;
use awc::{BoxedSocket, Client};
use awc::ws::{Codec, Frame, Message};
use url::{form_urlencoded::{self, byte_serialize, parse}, Url, ParseError};
use actix_codec::Framed;
use std::io;
use futures::stream::{SplitSink, StreamExt};
use awc::error::{WsProtocolError, WsClientError, SendRequestError};
use actix::{StreamHandler, Context, Actor, Addr};
use bytes::{Buf, Bytes};
use std::time::{Duration, Instant};
use actix::io::SinkWrite;
use actix_web::HttpMessage;
use awc::cookie::Cookie;
use serde::{Serialize, Deserialize};
use actix_http::cookie::{ParseError as CookieParseError};
use actix_http::error::PayloadError;

#[derive(Debug, Fail)]
pub enum HubClientError {
    #[fail(display = "invalid data : {:?}", data)]
    InvalidData { data: Vec<String> },
    #[fail(display = "missing key")]
    MissingData,
    #[fail(display = "invalid json data {}", 0)]
    ParseError(serde_json::Error),
    #[fail(display = "send request error {:?}", 0)]
    RequestError(String),
    #[fail(display = "cookie error {:?}", 0)]
    CookieParseError(String),
    #[fail(display = "payload error {:?}", 0)]
    PayloadError(String),
    #[fail(display = "ws client error {:?}", 0)]
    WsClientError(String)
}

impl From<NoneError> for HubClientError {
    fn from(_: NoneError) -> Self {
        HubClientError::MissingData
    }
}

impl From<serde_json::Error> for HubClientError {
    fn from(e: serde_json::Error) -> Self {
        HubClientError::ParseError(e)
    }
}

impl From<SendRequestError> for HubClientError {
    fn from(e: SendRequestError) -> Self {
        HubClientError::RequestError(format!("{}", e))
    }
}

impl From<CookieParseError> for HubClientError {
    fn from(e: CookieParseError) -> Self {
        HubClientError::CookieParseError(format!("{}", e))
    }
}

impl From<PayloadError> for HubClientError {
    fn from(e: PayloadError) -> Self {
        HubClientError::PayloadError(format!("{}", e))
    }
}

impl From<WsClientError> for HubClientError {
    fn from(e: WsClientError) -> Self {
        HubClientError::WsClientError(format!("{}", e))
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

#[derive(Serialize)]
struct ConnectionData {
    name: String
}

#[derive(Deserialize)]
struct SignalrConnection {
    ConnectionToken: String,
    TryWebSockets: bool,
}

const CLIENT_PROTOCOL : &str = "1.5";

impl HubClient {
    async fn negotiate(url: &mut Url, hub: &str) -> Result<(String, SignalrConnection), HubClientError> {
        let conn_data = serde_json::to_string(&vec![ConnectionData { name: hub.to_string() }]).unwrap();
        let encoded: String = form_urlencoded::Serializer::new(String::new())
            .append_pair("connectionData", conn_data.as_str())
            .append_pair("clientProtocol", CLIENT_PROTOCOL)
            .finish();
        let mut negotiate_url = url.join("negotiate").unwrap();
        negotiate_url.set_query(Some(&encoded));


        let ssl = {
            let mut ssl = openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls()).unwrap();
            ssl.build()
        };
        let connector = awc::Connector::new().ssl(ssl).finish();
        let client = Client::build()
            .connector(connector)
            .finish();
        let mut result = client.get(negotiate_url.clone().into_string()).send().await?;
        let bytes = result.body().await?;
        let cookies = result.cookies()?;
        let cookies_str : Vec<String> = cookies.clone().into_iter().map(|c| c.to_string()).collect();
        let cookie_str = cookies_str.join(";");
        let resp : SignalrConnection = serde_json::from_slice(&bytes)?;
        Ok((cookie_str, resp))
    }

    pub async fn new(hub: &'static str, wss_url: &str, handler: Box<HubClientHandler>) -> Result<Addr<HubClient>, HubClientError> {
        let signalr_url : &mut Url = &mut Url::parse(wss_url).unwrap();
        let (cookies, resp) = HubClient::negotiate(signalr_url, hub).await?;
        if !resp.TryWebSockets {
            return Err(HubClientError::WsClientError("Websockets are not enabled for this SignalR server".to_string()));
        }
        let conn_data = serde_json::to_string(&vec![ConnectionData { name: hub.to_string() }]).unwrap();
        let encoded: String = form_urlencoded::Serializer::new(String::new())
            .append_pair("connectionToken", resp.ConnectionToken.as_str())
            .append_pair("connectionData", conn_data.as_str())
            .append_pair("clientProtocol", CLIENT_PROTOCOL)
            .append_pair("transport", "webSockets")
            .finish();
        let mut connection_url : Url = signalr_url.join("connect").unwrap();
        connection_url.set_query(Some(&encoded));
        let c = new_ws_client(connection_url.as_str()).await?;
        let (sink, stream) = c.split();
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
