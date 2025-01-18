use log::error;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::io::Write;
use websocket::stream::sync::NetworkStream;
use websocket::sync::Client;
use websocket::{ClientBuilder, Message, OwnedMessage};

#[derive(Serialize, Deserialize)]
struct Subscription {
    method: String,
    params: SubscriptionParams,
}

#[derive(Serialize, Deserialize)]
struct SubscriptionParams {
    channel: String,
    symbol: Vec<String>,
    depth: i32
}

#[derive(Serialize, Deserialize)]
struct OrderbookMessage {
    data: Option<Vec<OrderbookEntry>>,
    #[serde(rename = "type")]
    type_name: Option<String>,
    channel: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct OrderbookEntry {
    symbol: Option<String>,
    bids: Option<Vec<Entry>>,
    asks: Option<Vec<Entry>>,
}

#[derive(Serialize, Deserialize)]
struct Entry {
    price: f64,
    qty: f64,
}

#[derive(Serialize, Deserialize)]
struct AskEntry {
    price: f64,
    qty: f64,
}

#[derive(Serialize, Deserialize)]
struct BidEntry {
    price: f64,
    qty: f64,
}

struct Orderbook {
    bids_heap: BTreeSet<BidEntry>,
    asks_heap: BTreeSet<AskEntry>,
}

impl Eq for AskEntry {}

impl PartialEq<Self> for AskEntry {
    fn eq(&self, other: &Self) -> bool {
        self.price.eq(&other.price)
    }
}

impl PartialOrd<Self> for AskEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.price.partial_cmp(&other.price)
    }
}

impl Ord for AskEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.price.total_cmp(&other.price)
    }
}

impl Eq for BidEntry {}

impl PartialEq<Self> for BidEntry {
    fn eq(&self, other: &Self) -> bool {
        self.price.eq(&other.price)
    }
}

impl PartialOrd<Self> for BidEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.price.partial_cmp(&other.price)
    }
}

impl Ord for BidEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.price.total_cmp(&other.price)
    }
}

const SIGMA: f64 = 0.00000001;

impl Orderbook {
    fn new() -> Self {
        Orderbook {
            bids_heap: BTreeSet::new(),
            asks_heap: BTreeSet::new(),
        }
    }

    pub(crate) fn evaluate(&mut self, message: OrderbookMessage) {
        if let Some(ref type_name) = message.type_name {
            match type_name.as_str() {
                "snapshot" => self.handle_snapshot(message),
                "update" => self.handle_update(message),
                _ => {}
            }
        }
    }

    fn render(&self) {
        match (
            self.bids_heap.iter().next(),
            self.asks_heap.iter().next_back(),
        ) {
            (Some(bid), Some(ask)) => {
                print!(
                    "\r\x1b[2KBID {:?} {:.10} <-> ASK {:?} {:.10}",
                    bid.price, bid.qty, bid.price, ask.qty
                );
                std::io::stdout().flush().unwrap();
            }
            _ => {}
        }
    }

    fn handle_snapshot(&mut self, message: OrderbookMessage) {
        self.bids_heap.clear();
        self.asks_heap.clear();
        self.handle_update(message);
    }

    fn handle_update(&mut self, message: OrderbookMessage) {
        message.data.unwrap().iter().for_each(|entry| {
            match &entry.bids {
                Some(bids) => {
                    bids.iter().for_each(|bid| {
                        if bid.qty < SIGMA {
                            self.bids_heap
                                .retain(|entry| entry.price - bid.price > SIGMA);
                        } else {
                            self.bids_heap.insert(BidEntry {
                                price: bid.price,
                                qty: bid.qty,
                            });
                        }
                    });
                }
                _ => {}
            }

            match &entry.asks {
                Some(asks) => {
                    asks.iter().for_each(|ask| {
                        if ask.qty < SIGMA {
                            self.asks_heap
                                .retain(|entry| entry.price - ask.price > SIGMA);
                        } else {
                            self.asks_heap.insert(AskEntry {
                                price: ask.price,
                                qty: ask.qty,
                            });
                        }
                    });
                }
                _ => {}
            }
        })
    }
}

fn main() {
    env_logger::init();

    let client_builder = ClientBuilder::new("wss://ws.kraken.com/v2");
    let websocket_client = client_builder.unwrap().connect(None);
    match websocket_client {
        Ok(client) => {
            handle_connection(client);
        }
        Err(error) => {
            error!("Couldn't connect to the websocket. {}", error);
        }
    }
}

fn handle_connection(mut client: Client<Box<dyn NetworkStream + Send>>) {
    let subscription = get_subscription();
    let result = serde_json::to_string(&subscription);
    let message = Message::text(result.unwrap());

    client.send_message(&message).unwrap();

    let mut orderbook = Orderbook::new();

    client.incoming_messages().for_each(|result| match result {
        Ok(message) => match message {
            OwnedMessage::Text(text) => {
                let result1 = serde_json::from_str::<OrderbookMessage>(text.as_str());
                match result1 {
                    Ok(orderbook_message) => match orderbook_message.channel.as_deref() {
                        Some("book") => {
                            orderbook.evaluate(orderbook_message);
                            orderbook.render();
                        }
                        _ => {}
                    },
                    Err(err) => {
                        error!("Error while parsing message: {}", err);
                    }
                }
            }
            _ => {
                error!("Unhandled message type");
            }
        },
        Err(error) => {
            error!("Error while receiving message: {}", error);
        }
    });
}

fn get_subscription() -> Subscription {
    Subscription {
        method: String::from("subscribe"),
        params: SubscriptionParams {
            channel: String::from("book"),
            symbol: Vec::from([String::from("BTC/USD")]),
            depth: 25
        },
    }
}
