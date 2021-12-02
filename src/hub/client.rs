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
use bytes::Bytes;
use futures::stream::{SplitSink, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::VecDeque;

use futures::Future;
use openssl::error::ErrorStack;
use openssl::ssl::SslConnector;
use std::time::{Duration, Instant};
use thiserror::Error;
use url::{form_urlencoded::{self},
          Url};

#[derive(Debug, Error)]
pub enum HubClientError {
    #[error("invalid data : {0:?}", data)]
    InvalidData { data: Vec<String> },
    #[error("missing key")]
    MissingData,
    #[error("invalid json data {0}")]
    ParseError(serde_json::Error),
    #[error("send request error {0}")]
    RequestError(String),
    #[error("cookie error {0}")]
    CookieParseError(String),
    #[error("payload error {0}")]
    PayloadError(String),
    #[error("ws client error {0}")]
    WsClientError(String),
    #[error("invalid base 64 data {0}")]
    Base64DecodeError(DecodeError),
    #[error("failed to read from stream {0}")]
    GenericIoError(String),
    #[error("tried to send in a closed sink")]
    ClosedSink,
    #[error("failed to create ssl connector")]
    NoSslConnector,
    #[error("failed to create ssl connector")]
    SslError(#[from] ErrorStack),
    #[error("error when decoding {0} : {1}")]
    DecodeError(String, serde_json::Error),
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

pub type Result<T> = std::result::Result<T, HubClientError>;

pub trait PendingQuery {
    fn query(&self) -> String;
}

#[derive(Eq, PartialEq)]
pub enum RestartPolicy {
    Always,
    Never,
}

fn default_ssl_connector() -> Result<SslConnector> {
    let mut ssl = openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls())?;
    let _ = ssl.set_alpn_protos(b"\x08http/1.1");
    Ok(ssl.build())
}

pub struct HubClientBuilder {
    hub_name: String,
    /// Time between two successive pending queries
    query_backoff: u64,
    signalr_url: Url,
    connection_url: Url,
    restart_policy: RestartPolicy,
    ssl_connector: Option<SslConnector>,
    connection_query_builder: Option<Box<dyn Fn(&str) -> String>>,
}

impl HubClientBuilder {
    pub fn with_hub_and_url(hub: &str, url: Url) -> Self {
        let connection_url = url.join("connect").unwrap();
        HubClientBuilder {
            hub_name: hub.to_string(),
            query_backoff: 20,
            signalr_url: url,
            connection_url,
            restart_policy: RestartPolicy::Always,
            ssl_connector: None,
            connection_query_builder: None,
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
        self.ssl_connector = Some(ssl_connector);
        self
    }

    pub fn connection_query_builder(mut self, connection_query_builder: Box<dyn Fn(&str) -> String>) -> Self {
        self.connection_query_builder = Some(connection_query_builder);
        self
    }

    async fn negotiate(&self, ssl: SslConnector) -> Result<(String, SignalrConnection)> {
        let conn_data = serde_json::to_string(&vec![ConnectionData {
            name: self.hub_name.clone(),
        }])
        .unwrap();
        let mut negotiate_url = self.signalr_url.join("negotiate").unwrap();
        let encoded: String = form_urlencoded::Serializer::new(String::new())
            .append_pair("connectionData", conn_data.as_str())
            .append_pair("clientProtocol", CLIENT_PROTOCOL)
            .finish();
        negotiate_url.set_query(Some(&encoded));

        let connector = awc::Connector::new().ssl(ssl);
        let client = Client::builder().connector(connector).finish();

        let mut client_response = client.get(negotiate_url.to_string()).send().await?;

        let body = client_response.body().await?;
        let cookies = client_response.cookies()?;
        let cookies_str: Vec<String> = cookies.iter().map(|c| c.to_string()).collect();
        let cookie_str = cookies_str.join(";");
        let signalr_conn: SignalrConnection = serde_json::from_slice(&body)
            .map_err(|e| HubClientError::DecodeError("SignalrConnection".to_string(), e))?;

        Ok((cookie_str, signalr_conn))
    }

    pub async fn start_supervised(mut self, handler: Box<dyn HubClientHandler>) -> Result<Addr<HubClient>> {
        let connector = self
            .ssl_connector
            .clone()
            .or_else(|| default_ssl_connector().ok())
            .ok_or(HubClientError::NoSslConnector)?;
        let (cookies, resp) = self.negotiate(connector.clone()).await?;
        if !resp.TryWebSockets {
            return Err(HubClientError::WsClientError(
                "Websockets are not enabled for this SignalR server".to_string(),
            ));
        }
        let encoded = if let Some(builder) = self.connection_query_builder {
            (builder)(resp.ConnectionToken.as_str())
        } else {
            self.default_connection_query(resp.ConnectionToken.as_str())
        };
        self.connection_url.set_query(Some(&encoded));
        HubClient::start_new(
            self.hub_name.to_string(),
            self.connection_url.clone(),
            connector,
            self.query_backoff,
            self.restart_policy,
            cookies,
            handler,
        )
        .await
    }

    fn default_connection_query(&self, token: &str) -> String {
        let conn_data = serde_json::to_string(&vec![ConnectionData {
            name: self.hub_name.clone(),
        }])
        .unwrap();
        form_urlencoded::Serializer::new(String::new())
            .append_pair("connectionToken", token)
            .append_pair("connectionData", conn_data.as_str())
            .append_pair("clientProtocol", CLIENT_PROTOCOL)
            .append_pair("transport", "webSockets")
            .finish()
    }
}

pub struct HubClient {
    hb: Instant,
    name: String,
    connected: bool,
    /// Time between two successive pending queries
    handler: Box<dyn HubClientHandler>,
    query_backoff: u64,
    inner: SinkWrite<Message, SplitSink<Framed<BoxedSocket, Codec>, Message>>,
    pending_queries: VecDeque<Box<dyn PendingQuery>>,
    connection_url: Url,
    conn_backoff: ExponentialBackoff,
    restart_policy: RestartPolicy,
    pub cookies: String,
    connector: SslConnector,
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
#[rtype(result = "Result<()>")]
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

    pub fn new(
        hub: &str,
        signalr_url: &str,
        query_backoff: u64,
        restart_policy: RestartPolicy,
        handler: Box<dyn HubClientHandler>,
    ) -> impl Future<Output = Result<Addr<HubClient>>> {
        HubClientBuilder::with_hub_and_url(hub, Url::parse(signalr_url).unwrap())
            .restart_policy(restart_policy)
            .query_backoff(query_backoff)
            .start_supervised(handler)
    }

    pub async fn start_new(
        hub: String,
        connection_url: Url,
        ssl: SslConnector,
        query_backoff: u64,
        restart_policy: RestartPolicy,
        cookies: String,
        handler: Box<dyn HubClientHandler>,
    ) -> Result<Addr<HubClient>> {
        let conn_backoff = ExponentialBackoff {
            max_elapsed_time: None,
            ..ExponentialBackoff::default()
        };
        let c = new_ws_client(ssl.clone(), connection_url.to_string(), cookies.clone()).await?;
        let (sink, stream) = c.split();
        Ok(Supervisor::start(move |ctx| {
            HubClient::add_stream(stream, ctx);
            HubClient {
                inner: SinkWrite::new(sink, ctx),
                handler,
                connected: false,
                query_backoff,
                connection_url,
                conn_backoff,
                name: hub.to_string(),
                hb: Instant::now(),
                pending_queries: VecDeque::new(),
                restart_policy,
                connector: ssl,
                cookies,
            }
        }))
    }

    fn handle_bytes(&mut self, ctx: &mut Context<Self>, bytes: Bytes) -> Result<()> {
        let msg: Map<String, Value> = serde_json::from_slice(bytes.as_ref())
            .map_err(|e| HubClientError::DecodeError("Message".to_string(), e))?;
        if msg.get("S").is_some() {
            self.connected = true;
            let queries: Vec<Box<dyn PendingQuery>> = self.handler.on_connect();
            for query in queries {
                self.pending_queries.push_back(query);
            }
            let mut backoff = self.query_backoff;
            self.pending_queries.iter().for_each(|pq| {
                let query = pq.query();
                ctx.run_later(Duration::from_millis(backoff), |act, _ctx| {
                    if act.inner.write(Message::Text(query.into())).is_err() {
                        trace!("Tried to write pending query in closed sink");
                    }
                });
                backoff += self.query_backoff;
            });
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
            Some(Value::Array(data)) => data.iter().try_for_each(|inner_data| {
                let hub: Option<&Value> = inner_data.get("H");
                match hub {
                    Some(Value::String(hub_name)) if hub_name.to_lowercase() == self.name => {
                        let m: Option<&Value> = inner_data.get("M");
                        let a: Option<&Value> = inner_data.get("A");
                        match (m, a) {
                            (Some(Value::String(method)), Some(v)) => {
                                self.handler.handle(method, v);
                                Ok(())
                            }
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
            }),
            _ => Ok(()),
        }
    }
}

/// Upon disconnection of the ws client, recreate the ws client
impl actix::Supervised for HubClient {
    fn restarting(&mut self, ctx: &mut <Self as Actor>::Context) {
        if self.restart_policy == RestartPolicy::Never {
            warn!("Restart policy was 'never', exiting actor");
            ctx.stop();
            return;
        }
        let client1 = new_ws_client(
            self.connector.clone(),
            self.connection_url.to_string(),
            self.cookies.clone(),
        );
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

impl StreamHandler<std::result::Result<Frame, WsProtocolError>> for HubClient {
    fn handle(&mut self, msg: std::result::Result<Frame, WsProtocolError>, ctx: &mut Context<Self>) {
        match msg {
            Ok(Frame::Ping(msg)) => {
                self.hb = Instant::now();
                if self.inner.write(Message::Pong(Bytes::copy_from_slice(&msg))).is_err() {
                    trace!("failed to write back pong");
                }
            }
            Ok(Frame::Text(b)) | Ok(Frame::Binary(b)) => {
                if self.handle_bytes(ctx, b).is_err() {
                    trace!("failed to handle bytes");
                }
            }
            _ => (),
        }
    }
}

impl<'de, T> Handler<HubQuery<T>> for HubClient
where
    T: Deserialize<'de> + Serialize + 'static,
{
    type Result = Result<()>;

    fn handle(&mut self, msg: HubQuery<T>, _ctx: &mut Self::Context) -> Self::Result {
        if !self.connected {
            self.pending_queries.push_back(Box::new(msg));
            Ok(())
        } else {
            let result = serde_json::to_string(&msg).unwrap();
            self.inner
                .write(Message::Text(result.into()))
                .map_err(|_| HubClientError::ClosedSink)
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

pub async fn new_ws_client(connector: SslConnector, url: String, cookie: String) -> Result<Framed<BoxedSocket, Codec>> {
    let connector = awc::Connector::new().ssl(connector);
    let client = Client::builder().header("Cookie", cookie).connector(connector).finish();

    let (_response, framed) = client.ws(url).connect().await?;

    Ok(framed)
}
