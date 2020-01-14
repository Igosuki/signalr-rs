use actix::io::SinkWrite;
use actix::{
    Actor, ActorContext, ActorFuture, Addr, AsyncContext, Context, ContextFutureSpawner, Handler,
    StreamHandler, Supervised, Supervisor, WrapFuture,
};
use actix_codec::Framed;
use actix_http::cookie::ParseError as CookieParseError;
use actix_http::error::PayloadError;
use actix_web::HttpMessage;
use awc::error::{SendRequestError, WsClientError, WsProtocolError};
use awc::ws::{Codec, Frame, Message};
use awc::{BoxedSocket, Client};
use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;
use base64::DecodeError;
use bytes::{Buf, Bytes};
use futures::stream::{SplitSink, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::VecDeque;
use std::option::NoneError;
use std::time::{Duration, Instant};
use url::{
    form_urlencoded::{self},
    Url,
};

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
}

impl From<NoneError> for HubClientError {
    fn from(_: NoneError) -> Self {
        HubClientError::MissingData
    }
}

impl From<DecodeError> for HubClientError {
    fn from(e: DecodeError) -> Self {
        HubClientError::Base64DecodeError(e)
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

trait PendingQuery {
    fn query(&self) -> String;
}

#[derive(Eq, PartialEq)]
pub enum RestartPolicy {
    Always,
    Never,
}

pub struct HubClient {
    hb: Instant,
    name: String,
    connected: bool,
    /// Time between two successive pending queries
    query_backoff: u64,
    handler: Box<dyn HubClientHandler>,
    inner: SinkWrite<Message, SplitSink<Framed<BoxedSocket, Codec>, Message>>,
    pending_queries: VecDeque<Box<dyn PendingQuery>>,
    connection_url: Url,
    conn_backoff: ExponentialBackoff,
    restart_policy: RestartPolicy,
    pub cookies: String,
}

impl Actor for HubClient {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        // start heartbeats otherwise server will disconnect after 10 seconds
        debug!("Hub clients started");
    }

    fn stopped(&mut self, _: &mut Context<Self>) {
        debug!("Hub client disconnected");
    }
}

#[derive(Serialize)]
struct ConnectionData {
    name: String,
}

#[derive(Deserialize)]
struct SignalrConnection {
    ConnectionToken: String,
    TryWebSockets: bool,
}

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
    fn query(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

const CLIENT_PROTOCOL: &str = "1.5";

impl HubClient {
    fn connected(&mut self) {}

    async fn negotiate(
        url: &mut Url,
        hub: &str,
    ) -> Result<(String, SignalrConnection), HubClientError> {
        let conn_data = serde_json::to_string(&vec![ConnectionData {
            name: hub.to_string(),
        }])
        .unwrap();
        let encoded: String = form_urlencoded::Serializer::new(String::new())
            .append_pair("connectionData", conn_data.as_str())
            .append_pair("clientProtocol", CLIENT_PROTOCOL)
            .finish();
        let mut negotiate_url = url.join("negotiate").unwrap();
        negotiate_url.set_query(Some(&encoded));

        let ssl = {
            let mut ssl =
                openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls()).unwrap();
            ssl.build()
        };
        let connector = awc::Connector::new().ssl(ssl).finish();
        let client = Client::build().connector(connector).finish();
        let mut result = client
            .get(negotiate_url.clone().into_string())
            .send()
            .await?;
        let bytes = result.body().await?;
        let cookies = result.cookies()?;
        let cookies_str: Vec<String> = cookies.clone().into_iter().map(|c| c.to_string()).collect();
        let cookie_str = cookies_str.join(";");
        let resp: SignalrConnection = serde_json::from_slice(&bytes)?;
        Ok((cookie_str, resp))
    }

    pub async fn new(
        hub: &'static str,
        wss_url: &str,
        query_backoff: u64,
        restart_policy: RestartPolicy,
        handler: Box<dyn HubClientHandler>,
    ) -> Result<Addr<HubClient>, HubClientError> {
        let signalr_url: &mut Url = &mut Url::parse(wss_url).unwrap();
        let (cookies, resp) = HubClient::negotiate(signalr_url, hub).await?;
        if !resp.TryWebSockets {
            return Err(HubClientError::WsClientError(
                "Websockets are not enabled for this SignalR server".to_string(),
            ));
        }
        let conn_data = serde_json::to_string(&vec![ConnectionData {
            name: hub.to_string(),
        }])
        .unwrap();
        let encoded: String = form_urlencoded::Serializer::new(String::new())
            .append_pair("connectionToken", resp.ConnectionToken.as_str())
            .append_pair("connectionData", conn_data.as_str())
            .append_pair("clientProtocol", CLIENT_PROTOCOL)
            .append_pair("transport", "webSockets")
            .finish();
        let mut connection_url: Url = signalr_url.join("connect").unwrap();
        connection_url.set_query(Some(&encoded));
        let mut conn_backoff = ExponentialBackoff::default();
        conn_backoff.max_elapsed_time = None;
        let c = new_ws_client(connection_url.to_string(), cookies.clone()).await?;
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
                cookies,
            }
        }))
    }

    fn handle_bytes(
        &mut self,
        ctx: &mut Context<Self>,
        bytes: Bytes,
    ) -> Result<(), HubClientError> {
        let msg: Map<String, Value> = serde_json::from_slice(bytes.bytes()).unwrap();
        if msg.get("S").is_some() {
            self.connected = true;
            let mut backoff = self.query_backoff;
            loop {
                match self.pending_queries.pop_back() {
                    None => break,
                    Some(pq) => {
                        let query = pq.query().clone();
                        ctx.run_later(Duration::from_millis(backoff), |act, ctx| {
                            match act.inner.write(Message::Text(query)) {
                                Ok(_) => debug!("Wrote query"),
                                Err(e) => debug!("Pending query write unsuccessful"),
                            }
                        });
                        backoff += backoff;
                    }
                }
            }
            return Ok(());
        }
        let id = msg.get("I").and_then(|i| i.as_str());
        if let Some(error_msg) = msg.get("E") {
            debug!("Error message {:?} for identifier {:?} ", error_msg, id);
            self.handler.error(id, error_msg);
            return Ok(());
        }
        match msg.get("R") {
            Some(Value::Bool(was_successful)) => {
                if *was_successful {
                    debug!("Query {:?} was successful", id);
                } else {
                    debug!("Query {:?} failed without error", id);
                }
                return Ok(());
            }
            Some(v) => match id {
                Some(id) => self.handler.handle(id, v),
                _ => {
                    debug!("No id to identify response query for msg : ${:?}", msg);
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
                        Some(Value::String(hub_name)) if hub_name.to_lowercase() == self.name => {
                            let m: Option<&Value> = inner_data.get("M");
                            let a: Option<&Value> = inner_data.get("A");
                            match (m, a) {
                                (Some(Value::String(method)), Some(v)) => {
                                    Ok(self.handler.handle(method, v))
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
                            Err(HubClientError::InvalidData {
                                data: vec![hub_str],
                            })
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
        if self.restart_policy == RestartPolicy::Never {
            debug!("Restart policy was 'never', exiting actor");
            ctx.stop();
            return;
        }
        let conn_str = self.connection_url.to_string().clone();
        let cookies = self.cookies.clone();
        let client1 = new_ws_client(conn_str, cookies);
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
                self.inner
                    .write(Message::Pong(Bytes::copy_from_slice(&msg)));
            }
            Ok(Frame::Text(txt)) => {
                self.handle_bytes(ctx, txt);
            }
            Ok(Frame::Binary(b)) => {
                self.handle_bytes(ctx, b);
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

    fn handle(&mut self, msg: HubQuery<T>, ctx: &mut Self::Context) -> Self::Result {
        if !self.connected {
            self.pending_queries.push_back(Box::new(msg));
        } else {
            let result = serde_json::to_string(&msg).unwrap();
            self.inner.write(Message::Text(result));
        }
    }
}

impl actix::io::WriteHandler<WsProtocolError> for HubClient {}

pub trait HubClientHandler {
    /// Called every time the SignalR client manages to connect (receives 'S': 1)
    fn connected(&self) {}

    /// Called for every error
    fn error(&self, id: Option<&str>, msg: &Value) {}

    /// Called for every message with a method 'M'
    fn handle(&mut self, method: &str, message: &Value);
}

pub async fn new_ws_client(
    url: String,
    cookie: String,
) -> Result<Framed<BoxedSocket, Codec>, WsClientError> {
    let ssl = {
        let mut ssl = openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls()).unwrap();
        let _ = ssl.set_alpn_protos(b"\x08http/1.1");
        ssl.build()
    };
    let connector = awc::Connector::new().ssl(ssl).finish();
    let client = Client::build()
        .header("Cookie", cookie)
        .connector(connector)
        .finish();

    let (response, framed) = client.ws(url).connect().await?;

    Ok(framed)
}
