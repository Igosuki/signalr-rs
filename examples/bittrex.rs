// This example shows how to use the generic API provided by Coinnect.
// This method is useful if you have to iterate throught multiple accounts of
// different exchanges and perform the same operation (such as get the current account's balance)
// You can also use the Coinnect generic API if you want a better error handling since all methods
// return Result<_, Error>.
extern crate actix;
extern crate env_logger;
extern crate signalr_rs;
#[macro_use]
extern crate serde;

use actix::System;
use base64;
use futures::io;
use libflate::deflate::Decoder;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json;
use serde_json::Value;
use signalr_rs::hub::client::{HubClient, HubClientError, HubClientHandler, HubQuery, PendingQuery, RestartPolicy};
use std::io::Read;

struct BittrexHandler {
    hub: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
struct FillEntry {
    #[serde(alias = "F")]
    FillType: String,
    #[serde(alias = "I")]
    Id: i32,
    #[serde(alias = "OT")]
    OrderType: String,
    #[serde(alias = "P")]
    Price: f32,
    #[serde(alias = "Q")]
    Quantity: f32,
    #[serde(alias = "T")]
    TimeStamp: i64,
    #[serde(alias = "U")]
    Uuid: String,
    #[serde(alias = "t")]
    Total: f32,
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
struct OrderPair {
    #[serde(alias = "Q")]
    Q: f32,
    #[serde(alias = "R")]
    R: f32,
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
struct ExchangeState {
    #[serde(alias = "M")]
    MarketName: String,
    #[serde(alias = "N")]
    Nonce: i32,
    #[serde(alias = "Z")]
    Buys: Vec<OrderPair>,
    #[serde(alias = "S")]
    Sells: Vec<OrderPair>,
    #[serde(alias = "f")]
    Fills: Vec<FillEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
struct Order {
    #[serde(alias = "U")]
    Uuid: String,
    #[serde(alias = "OU")]
    OrderUuid: String,
    #[serde(alias = "I")]
    Id: i64,
    #[serde(alias = "E")]
    Exchange: String,
    #[serde(alias = "OT")]
    OrderType: String,
    #[serde(alias = "Q")]
    Quantity: f32,
    #[serde(alias = "q")]
    QuantityRemaining: f32,
    #[serde(alias = "X")]
    Limit: f32,
    #[serde(alias = "n")]
    CommissionPaid: f32,
    #[serde(alias = "P")]
    Price: f32,
    #[serde(alias = "PU")]
    PricePerUnit: f32,
    #[serde(alias = "Y")]
    Opened: i64,
    #[serde(alias = "C")]
    Closed: i64,
    #[serde(alias = "i")]
    IsOpen: bool,
    #[serde(alias = "CI")]
    CancelInitiated: bool,
    #[serde(alias = "K")]
    ImmediateOrCancel: bool,
    #[serde(alias = "k")]
    IsConditional: bool,
    #[serde(alias = "J")]
    Condition: String,
    #[serde(alias = "j")]
    ConditionTarget: f32,
    #[serde(alias = "u")]
    Updated: i64,
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
struct OrderDelta {
    #[serde(alias = "w")]
    AccountUuid: String,
    #[serde(alias = "N")]
    Nonce: i32,
    #[serde(alias = "TY")]
    Type: i32,
    #[serde(alias = "o")]
    Order: Order,
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
enum TradeType {
    ADD,
    REMOVE,
    UPDATE,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub(crate) struct OrderLog {
    #[serde(alias = "TY")]
    pub Type: i32,
    #[serde(alias = "R")]
    pub Rate: f32,
    #[serde(alias = "Q")]
    pub Quantity: f32,
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
struct Fill {
    #[serde(alias = "FI")]
    FillId: i32,
    #[serde(alias = "OT")]
    OrderType: String,
    #[serde(alias = "R")]
    Rate: f32,
    #[serde(alias = "Q")]
    Quantity: f32,
    #[serde(alias = "T")]
    TimeStamp: i64,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub(crate) struct MarketDelta {
    #[serde(alias = "M")]
    pub MarketName: String,
    #[serde(alias = "N")]
    Nonce: i32,
    #[serde(alias = "Z")]
    pub Buys: Vec<OrderLog>,
    #[serde(alias = "S")]
    pub Sells: Vec<OrderLog>,
    #[serde(alias = "f")]
    pub Fills: Vec<Fill>,
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
struct SummaryDelta {
    #[serde(alias = "M")]
    MarketName: String,
    #[serde(alias = "H")]
    High: f32,
    #[serde(alias = "L")]
    Low: f32,
    #[serde(alias = "V")]
    Volume: f32,
    #[serde(alias = "l")]
    Last: f32,
    #[serde(alias = "m")]
    BaseVolume: f32,
    #[serde(alias = "T")]
    TimeStamp: i64,
    #[serde(alias = "B")]
    Bid: f32,
    #[serde(alias = "A")]
    Ask: f32,
    #[serde(alias = "G")]
    OpenBuyOrders: i32,
    #[serde(alias = "g")]
    OpenSellOrders: i32,
    #[serde(alias = "PD")]
    PrevDay: f32,
    #[serde(alias = "x")]
    Created: i64,
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
struct SummaryDeltaResponse {
    #[serde(alias = "N")]
    Nonce: i32,
    #[serde(alias = "d")]
    Delta: SummaryDelta,
}

impl BittrexHandler {
    fn deflate<T>(binary: &String) -> Result<T, HubClientError>
    where
        T: DeserializeOwned,
    {
        let decoded = base64::decode(binary)?;
        let mut decoder = Decoder::new(&decoded[..]);
        let mut decoded_data: Vec<u8> = Vec::new();
        decoder.read_to_end(&mut decoded_data)?;
        let v: &[u8] = &decoded_data;
        serde_json::from_slice::<T>(v).map_err(|e| HubClientError::ParseError(e))
    }

    fn deflate_array<T>(a: &Value) -> Result<T, HubClientError>
    where
        T: DeserializeOwned,
    {
        let data: Vec<String> = serde_json::from_value(a.clone())?;
        let binary = data.first().ok_or(HubClientError::MissingData)?;
        BittrexHandler::deflate::<T>(binary)
    }

    fn deflate_string<T>(a: &Value) -> Result<T, HubClientError>
    where
        T: DeserializeOwned,
    {
        let binary: String = serde_json::from_value(a.clone())?;
        BittrexHandler::deflate::<T>(&binary)
    }
}

impl HubClientHandler for BittrexHandler {
    fn on_connect(&self) -> Vec<Box<dyn PendingQuery>> {
        let pairs = vec![
            "USDT-BCC",
            "USDT-BTC",
            "USDT-DASH",
            "USDT-ETC",
            "USDT-ETH",
            "USDT-LTC",
            "USDT-NEO",
            "USDT-OMG",
            "USDT-XMR",
            "USDT-XRP",
            "USDT-ZEC",
        ];
        pairs
            .into_iter()
            .enumerate()
            .map(|(i, p)| {
                Box::new(HubQuery::new(
                    self.hub.to_string(),
                    "SubscribeToExchangeDeltas".to_string(),
                    vec![p.to_string()],
                    (i + 1).to_string(),
                )) as Box<dyn PendingQuery>
            })
            .collect()
    }

    fn error(&self, _id: Option<&str>, _msg: &Value) {}

    fn handle(&mut self, method: &str, message: &Value) {
        match method {
            "uE" => {
                let delta = BittrexHandler::deflate_array::<MarketDelta>(message);
                println!("Market Delta : {:?}", delta)
            }
            "uS" => {
                let delta = BittrexHandler::deflate_array::<SummaryDeltaResponse>(message);
                println!("Summary Delta : {:?}", delta)
            }
            s if s.starts_with("QE") => {
                let delta = BittrexHandler::deflate_string::<ExchangeState>(message).unwrap();
                let r = serde_json::to_string(&delta);
                println!("Exchange State message : {:?}", r)
            }
            _ => println!("Unknown message : method {:?} message {:?}", method, message),
        }
    }
}

#[actix_rt::main]
async fn main() -> io::Result<()> {
    env_logger::init();
    let hub = "c2";
    let handler = Box::new(BittrexHandler { hub: hub.to_string() });
    let client = HubClient::new(
        hub,
        "https://socket.bittrex.com/signalr/",
        20,
        RestartPolicy::Always,
        handler,
    )
    .await;
    match client {
        Ok(addr) => {
            addr.do_send(HubQuery::new(
                hub.to_string(),
                "QueryExchangeState".to_string(),
                vec!["BTC-NEO"],
                "QE2".to_string(),
            ));
        }
        Err(e) => {
            println!("Hub client error : {:?}", e);
            System::current().stop();
        }
    }
    actix_rt::signal::ctrl_c().await
}
