use chrono::Utc;
use disruptor::*;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::thread;

#[derive(Clone, Debug)]
struct WSLog {
    data: String,
    ts: i32,
}

#[derive(Clone, Debug)]
struct TradeLog {
    pair: String,
    price: i32,
    ts: i32,
}

impl TradeLog {
    fn from_string(input: &str, ts: i32) -> Option<Self> {
        let parts: Vec<&str> = input.split(':').collect();
        if let (Some(price_str), Some(pair)) = (parts.get(0), parts.get(1)) {
            if let Ok(price) = price_str.parse::<i32>() {
                return Some(Self {
                    pair: pair.to_string(),
                    price,
                    ts,
                });
            }
        }
        None
    }
}

#[derive(Default)]
struct State {
    data: i32,
}

struct WSProducer {
    disruptor: SingleProducer<WSLog, SingleConsumerBarrier>,
    trade_producer: Arc<Mutex<TradeProducer>>,
    log_file: Arc<Mutex<std::fs::File>>, // File handler for logging
}

impl WSProducer {
    fn new(
        size: usize,
        trade_producer: Arc<Mutex<TradeProducer>>,
        log_file: Arc<Mutex<std::fs::File>>,
    ) -> Self {
        let ws_factory = || WSLog {
            ts: 0,
            data: "69:BTC".to_string(),
        };

        let initial_state = || State::default();

        let trade_producer_clone = Arc::clone(&trade_producer);
        let log_file_clone = Arc::clone(&log_file);

        let ws_consumer_trade_parser = move |_: &mut State, e: &WSLog, _: Sequence, _: bool| {
            if let Some(trade) = TradeLog::from_string(&e.data, e.ts) {
                println!(
                    "WSConsumer received: {:?} {:?} {:?} ",
                    trade.pair, trade.price, trade.ts
                );

                // Write time and data to the file
                let now = Utc::now();
                let log_entry = format!(
                    "[{}] Received WSLog: {:?} at {}\n",
                    now.to_rfc3339(),
                    e,
                    now.timestamp()
                );
                let mut file = log_file_clone.lock().unwrap();
                file.write_all(log_entry.as_bytes())
                    .expect("Failed to write to log file");

                // Forward the trade log to the trade producer
                let mut trade_producer = trade_producer_clone.lock().unwrap();
                trade_producer.publish_trade(trade);
            } else {
                println!("Failed to parse WSLog: {:?}", e.data);
            }
        };

        let channel = disruptor::build_single_producer(size, ws_factory, BusySpin)
            .handle_events_and_state_with(ws_consumer_trade_parser, initial_state)
            .build();

        Self {
            disruptor: channel,
            trade_producer,
            log_file,
        }
    }

    fn publish_ws_log(&mut self, data: String, ts: i32) {
        self.disruptor.publish(|e| {
            e.data = data;
            e.ts = ts;
        });
    }
}

struct TradeProducer {
    disruptor: SingleProducer<TradeLog, SingleConsumerBarrier>,
}

impl TradeProducer {
    fn new(size: usize) -> Self {
        let trade_factory = || TradeLog {
            price: 0,
            ts: 0,
            pair: "BTC".to_string(),
        };

        let initial_state = || State::default();

        let trade_consumer = |_: &mut State, e: &TradeLog, _: Sequence, _: bool| {
            println!(
                "TradeConsumer received: {:?} {:?} {:?} ",
                e.pair, e.price, e.ts
            );
        };

        let channel = disruptor::build_single_producer(size, trade_factory, BusySpin)
            .handle_events_and_state_with(trade_consumer, initial_state)
            .build();

        Self { disruptor: channel }
    }

    fn publish_trade(&mut self, trade: TradeLog) {
        self.disruptor.publish(|t| {
            t.price = trade.price;
            t.pair = trade.pair;
            t.ts = trade.ts;
        });
    }
}

struct TradingSystem {
    ws_producer: WSProducer,
    trade_producer: Arc<Mutex<TradeProducer>>,
}

impl TradingSystem {
    fn new(buffer_size: usize, log_file: Arc<Mutex<std::fs::File>>) -> Self {
        let trade_producer = Arc::new(Mutex::new(TradeProducer::new(buffer_size)));
        let ws_producer = WSProducer::new(buffer_size, Arc::clone(&trade_producer), log_file);

        Self {
            ws_producer,
            trade_producer,
        }
    }

    fn start(&mut self) {
        thread::scope(|s| {
            s.spawn(move || {
                for i in 0..100 {
                    let data = format!("{}:BTC", i);
                    self.ws_producer.publish_ws_log(data, i);
                }
            });
        });
    }
}

fn main() {
    let buffer_size = 64;

    // Open the log file (Append mode)
    let log_file = Arc::new(Mutex::new(
        OpenOptions::new()
            .append(true)
            .create(true)
            .open("ws_log_timestamps.txt")
            .expect("Unable to open file"),
    ));

    let mut trading_system = TradingSystem::new(buffer_size, log_file);
    trading_system.start();
}
