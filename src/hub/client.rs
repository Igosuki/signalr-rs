pub use openssl::ssl::SslConnector;

use actix::io::SinkWrite;
use actix::{Actor, ActorContext, ActorFutureExt, Addr, AsyncContext, Context, ContextFutureSpawner, Handler,
            StreamHandler, Supervisor, WrapFuture};
use actix_codec::Framed;
use actix_http::error::PayloadError;
use actix_web::cookie::ParseError as CookieParseError;
use awc::error::{SendRequestError, WsClientError, WsProtocolError};
use awc::ws::{Codec, Frame, Message};
use awc::{BoxedSocket, Client};
use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;
use base64::DecodeError;
use bytes::{Bytes};
use futures::stream::{SplitSink, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::VecDeque;

use std::time::{Duration, Instant};
use url::{form_urlencoded::{self},
          Url};

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
    WsClientError(String),
    #[fail(display = "invalid base 64 data {:?}", 0)]
    Base64DecodeError(DecodeError),
    #[fail(display = "failed to read from stream {:?}", 0)]
    GenericIoError(String),
}

impl From<DecodeError> for HubClientError {
    fn from(e: DecodeError) -> Self { HubClientError::Base64DecodeError(e) }
}

impl From<serde_json::Error> for HubClientError {
    fn from(e: serde_json::Error) -> Self { HubClientError::ParseError(e) }
}

impl From<SendRequestError> for HubClientError {
    fn from(e: SendRequestError) -> Self { HubClientError::RequestError(format!("{}", e)) }
}

impl From<CookieParseError> for HubClientError {
    fn from(e: CookieParseError) -> Self { HubClientError::CookieParseError(format!("{}", e)) }
}

impl From<PayloadError> for HubClientError {
    fn from(e: PayloadError) -> Self { HubClientError::PayloadError(format!("{}", e)) }
}

impl From<WsClientError> for HubClientError {
    fn from(e: WsClientError) -> Self { HubClientError::WsClientError(format!("{}", e)) }
}

impl From<std::io::Error> for HubClientError {
    fn from(e: std::io::Error) -> Self { HubClientError::GenericIoError(format!("{}", e)) }
}

pub trait PendingQuery {
    fn query(&self) -> String;
}

#[derive(Eq, PartialEq, Clone)]
pub enum RestartPolicy {
    Always,
    Never,
}

// #[derive(Clone)]
pub struct HubClientBuilder {
    hub_name: String,
    /// Time between two successive pending queries
    query_backoff: u64,
    signalr_url: Url,
    connection_url: Url,
    restart_policy: RestartPolicy,
    ssl_connector: SslConnector,
    build_connection_query: Box<dyn Fn(&str) -> String>
}

impl HubClientBuilder {
    pub fn with_hub_and_url(hub: &str, url: Url) -> Self { 
        let mut ssl = openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls()).unwrap();
        ssl.set_alpn_protos(b"\x08http/1.1").unwrap();
        let connection_url = url.join("connect").unwrap();
        HubClientBuilder {
            hub_name: hub.to_string(),
            query_backoff: 20,
            signalr_url: url,
            connection_url,
            restart_policy: RestartPolicy::Always,
            ssl_connector: ssl.build(),
            build_connection_query: Box::new(|token| {
                form_urlencoded::Serializer::new(String::new())
                .append_pair("connectionToken", token)
                .append_pair("transport", "webSockets")
                .finish()
            })
        }
    }

    pub fn query_backoff(mut self, query_backoff: u64) -> Self {
        self.query_backoff = query_backoff;
        self
    }

    pub fn restart_policy(mut self, restart_policy: RestartPolicy) -> Self {
        self.restart_policy = restart_policy;
        self
    }

    pub fn ssl_connector(mut self, ssl_connector: SslConnector) -> Self {
        self.ssl_connector = ssl_connector;
        self
    }

    pub fn build_connection_query(mut self, build_connection_query: Box<dyn Fn(&str) -> String>) -> Self {
        self.build_connection_query = build_connection_query;
        self
    }

    async fn negotiate(&self) -> Result<(String, SignalrConnection), HubClientError> {
        let mut negotiate_url = self.signalr_url.join("negotiate").unwrap();
        let encoded: String = form_urlencoded::Serializer::new(String::new()).finish();
        negotiate_url.set_query(Some(&encoded));
        let connector = actix_http::client::Connector::new().ssl(self.ssl_connector.clone());
        let client = Client::builder().connector(connector).finish();
        
        let mut result = client.get(negotiate_url.clone().into_string()).send().await?;
        
        let bytes = result.body().await?;
        let cookies = result.cookies()?;
        let cookies_str: Vec<String> = cookies.clone().into_iter().map(|c| c.to_string()).collect();
        let cookie_str = cookies_str.join(";");
        let resp: SignalrConnection = serde_json::from_slice(&bytes)?;
        Ok((cookie_str, resp))
    }

    async fn create_and_connect(&self, cookie: &str) -> Result<Framed<BoxedSocket, Codec>, WsClientError> {
        let connector = awc::Connector::new().ssl(self.ssl_connector.clone());
        let client = Client::builder().header("Cookie", cookie).connector(connector).finish();
    
        let (_response, framed) = client.ws(self.connection_url.as_str()).connect().await?;
    
        Ok(framed)
    }
    
    pub async fn build_and_start(
        mut self,
        handler: Box<dyn HubClientHandler>
    ) -> Result<Addr<HubClient>, HubClientError> {
        let (cookies, resp) = self.negotiate().await?;
        if !resp.TryWebSockets {
            return Err(HubClientError::WsClientError(
                "Websockets are not enabled for this SignalR server".to_string(),
            ));
        }
        let encoded = (self.build_connection_query)(resp.ConnectionToken.as_str());
        self.connection_url.set_query(Some(&encoded));

        let socket = self.create_and_connect(&cookies).await?;
        
        let (sink, stream) = socket.split();
        Ok(Supervisor::start(move |ctx| {
            HubClient::add_stream(stream, ctx);
            let mut hc = HubClient {
                config: self,
                hb: Instant::now(),
                connected: false,
                conn_backoff: ExponentialBackoff::default(),
                handler,
                inner: SinkWrite::new(sink, ctx),
                pending_queries: VecDeque::new(),
                cookies,
            };
            hc.conn_backoff.max_elapsed_time = None;
            hc
        }))
    }
}

pub struct HubClient {
    config: HubClientBuilder,
    hb: Instant,
    connected: bool,
    /// Time between two successive pending queries
    conn_backoff: ExponentialBackoff,
    handler: Box<dyn HubClientHandler>,
    inner: SinkWrite<Message, SplitSink<Framed<BoxedSocket, Codec>, Message>>,
    pending_queries: VecDeque<Box<dyn PendingQuery>>,
    pub cookies: String,
}

impl Actor for HubClient {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        // start heartbeats otherwise server will disconnect after 10 seconds
        info!("Hub client started");
    }

    fn stopped(&mut self, _: &mut Context<Self>) {
        warn!("Hub client disconnected");
    }
}

#[derive(Serialize)]
struct ConnectionData {
    name: String,
}

#[allow(non_snake_case)]
#[derive(Deserialize)]
struct SignalrConnection {
    ConnectionToken: String,
    TryWebSockets: bool,
}

#[allow(non_snake_case)]
#[derive(Serialize, Message)]
#[rtype(result = "()")]
pub struct HubQuery<T> {
    H: String,
    M: String,
    A: T,
    I: String,
}

impl<T> HubQuery<T> {
    pub fn new(hub: String, method: String, data: T, sends: String) -> HubQuery<T> {
        HubQuery {
            H: hub,
            M: method,
            A: data,
            I: sends,
        }
    }
}

impl<T> PendingQuery for HubQuery<T>
where
    T: Serialize,
{
    fn query(&self) -> String { serde_json::to_string(self).unwrap() }
}

const CLIENT_PROTOCOL: &str = "1.5";

impl HubClient {
    #[allow(dead_code)]
    fn connected(&mut self) {}

    pub async fn new(
        hub: &'static str,
        wss_url: &str,
        query_backoff: u64,
        restart_policy: RestartPolicy,
        handler: Box<dyn HubClientHandler>,
    ) -> Result<Addr<HubClient>, HubClientError> {
        let signalr_url = Url::parse(wss_url).unwrap();
        let conn_data = serde_json::to_string(&vec![ConnectionData { name: hub.to_string() }]).unwrap();
        HubClientBuilder
            ::with_hub_and_url(hub, signalr_url)
            .query_backoff(query_backoff)
            .restart_policy(restart_policy)
            .build_connection_query(Box::new(move |token| {
                form_urlencoded::Serializer::new(String::new())
                .append_pair("connectionToken", token)
                .append_pair("connectionData", &conn_data)
                .append_pair("clientProtocol", CLIENT_PROTOCOL)
                .append_pair("transport", "webSockets")
                .finish()
            }))
            .build_and_start(handler)
            .await
    }

    fn handle_bytes(&mut self, ctx: &mut Context<Self>, bytes: Bytes) -> Result<(), HubClientError> {
        let msg: Map<String, Value> = serde_json::from_slice(bytes.as_ref()).unwrap();
        if msg.get("S").is_some() {
            self.connected = true;
            let queries: Vec<Box<dyn PendingQuery>> = self.handler.on_connect();
            for query in queries {
                self.pending_queries.push_back(query);
            }
            let mut backoff = self.config.query_backoff;
            loop {
                match self.pending_queries.pop_back() {
                    None => break,
                    Some(pq) => {
                        let query = pq.query().clone();
                        ctx.run_later(Duration::from_millis(backoff), |act, _ctx| {
                            match act.inner.write(Message::Text(query.into())) {
                                Some(_) => trace!("Wrote query"),
                                None => trace!("Tried to write in a closing/closed sink"),
                            }
                        });
                        backoff += self.config.query_backoff;
                    }
                }
            }
            return Ok(());
        }
        let id = msg.get("I").and_then(|i| i.as_str());
        if let Some(error_msg) = msg.get("E") {
            trace!("Error message {:?} for identifier {:?} ", error_msg, id);
            self.handler.error(id, error_msg);
            return Ok(());
        }
        match msg.get("R") {
            Some(Value::Bool(was_successful)) => {
                if *was_successful {
                    trace!("Query {:?} was successful", id);
                } else {
                    trace!("Query {:?} failed without error", id);
                }
                return Ok(());
            }
            Some(v) => match id {
                Some(id) => self.handler.handle(id, v),
                _ => {
                    trace!("No id to identify response query for msg : ${:?}", msg);
                    return Ok(());
                }
            },
            _ => (),
        }

        let m = msg.get("M");
        match m {
            Some(Value::Array(data)) => data
                .into_iter()
                .map(|inner_data| {
                    let hub: Option<&Value> = inner_data.get("H");
                    match hub {
                        Some(Value::String(hub_name)) if hub_name.to_lowercase() == self.config.hub_name => {
                            let m: Option<&Value> = inner_data.get("M");
                            let a: Option<&Value> = inner_data.get("A");
                            match (m, a) {
                                (Some(Value::String(method)), Some(v)) => Ok(self.handler.handle(method, v)),
                                _ => {
                                    let m_str = serde_json::to_string(&m)?;
                                    let a_str = serde_json::to_string(&a)?;
                                    Err(HubClientError::InvalidData {
                                        data: vec![m_str, a_str],
                                    })
                                }
                            }
                        }
                        _ => {
                            let hub_str = serde_json::to_string(&hub)?;
                            Err(HubClientError::InvalidData { data: vec![hub_str] })
                        }
                    }
                })
                .collect(),
            _ => Ok(()),
        }
    }
}

/// Upon disconnection of the ws client, recreate the ws client
impl actix::Supervised for HubClient {
    fn restarting(&mut self, ctx: &mut <Self as Actor>::Context) {
        if self.config.restart_policy == RestartPolicy::Never {
            warn!("Restart policy was 'never', exiting actor");
            ctx.stop();
            return;
        }

        // Ugly but works.
        // This method requires &self to be static; could not reuse create_and_connect()
        let client1 = new_ws_client(
            self.config.ssl_connector.clone(), 
            self.config.connection_url.to_string(), 
            self.cookies.clone());
        client1
            .into_actor(self)
            .map(|res, act, ctx| match res {
                Ok(client) => {
                    let (sink, stream) = client.split();
                    HubClient::add_stream(stream, ctx);
                    act.conn_backoff.reset();
                    act.connected = false;
                    act.inner = SinkWrite::new(sink, ctx);
                }
                Err(err) => {
                    error!("Can not connect to signalr websocket: {}", err);
                    // re-connect with backoff time.
                    // we stop current context, supervisor will restart it.
                    if let Some(timeout) = act.conn_backoff.next_backoff() {
                        ctx.run_later(timeout, |_, ctx| ctx.stop());
                    }
                }
            })
            .wait(ctx);
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
                let _ = self.handle_bytes(ctx, txt);
            }
            Ok(Frame::Binary(b)) => {
                let _ = self.handle_bytes(ctx, b);
            }
            _ => (),
        }
    }
}

impl<'de, T> Handler<HubQuery<T>> for HubClient
where
    T: Deserialize<'de> + Serialize + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: HubQuery<T>, _ctx: &mut Self::Context) -> Self::Result {
        if !self.connected {
            self.pending_queries.push_back(Box::new(msg));
        } else {
            let result = serde_json::to_string(&msg).unwrap();
            self.inner.write(Message::Text(result.into()));
        }
    }
}

impl actix::io::WriteHandler<WsProtocolError> for HubClient {}

pub trait HubClientHandler {
    /// Called every time the SignalR client manages to connect (receives 'S': 1)
    fn on_connect(&self) -> Vec<Box<dyn PendingQuery>>;

    /// Called for every error
    fn error(&self, _id: Option<&str>, _msg: &Value) {}

    /// Called for every message with a method 'M'
    fn handle(&mut self, method: &str, message: &Value);
}

pub async fn new_ws_client(ssl: SslConnector, url: String, cookie: String) -> Result<Framed<BoxedSocket, Codec>, WsClientError> {
    let connector = awc::Connector::new().ssl(ssl);
    let client = Client::builder().header("Cookie", cookie).connector(connector).finish();

    let (_response, framed) = client.ws(url).connect().await?;

    Ok(framed)
}
