#![feature(try_trait)]
// This example shows how to use the generic API provided by Coinnect.
// This method is useful if you have to iterate throught multiple accounts of
// different exchanges and perform the same operation (such as get the current account's balance)
// You can also use the Coinnect generic API if you want a better error handling since all methods
// return Result<_, Error>.

extern crate actix;
extern crate signalr_rs;

use signalr_rs::hub::client::{HubClientHandler, HubClient, HubQuery};
use futures::io;
use actix::{System, Arbiter, Addr};
use serde_json::{Value, Map};

struct BittrexHandler {

}

impl HubClientHandler for BittrexHandler {
    fn handle(&self, method: &str, message: &Map<String, Value>) {
        match method {
            "uE" => println!("Market Delta : {:?}", message),
            "uS" => println!("Summary Delta : {:?}", message),
            _ => println!("Unknown message : method {:?} message {:?}", method, message)
        }

    }
}

fn main() -> io::Result<()> {
    let sys = System::new("websocket-client");

    Arbiter::spawn(async {
        let handler = Box::new(BittrexHandler{});
        let hub = "c2";
        let client = HubClient::new(hub, "https://socket.bittrex.com/signalr/", handler).await;
        match client {
            Ok(addr) => {
                addr.do_send(HubQuery::new(hub.to_string(), "SubscribeToExchangeDeltas".to_string(), "BTC-ETH".to_string(), 1));
                addr.do_send(HubQuery::new(hub.to_string(), "SubscribeToSummaryDeltas".to_string(), "".to_string(), 2));
                addr.do_send(HubQuery::new(hub.to_string(), "queryExchangeState".to_string(), "BTC-NEO".to_string(), 3));
            },
            Err(e) => {
                println!("Hub client error : {:?}", e);
                System::current().stop();
            }
        }
    });
    sys.run().unwrap();
    Ok(())
}
