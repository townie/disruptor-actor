use disruptor::*;
use std::thread;
use std::{cell::RefCell, rc::Rc};

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
    fn from_string(input: &str, ts: i32) -> Option<TradeLog> {
        let parts: Vec<&str> = input.split(':').collect();

        if let (Some(price_str), Some(pair)) = (parts.get(0), parts.get(1)) {
            // Try to parse the price as an i32
            if let Ok(price) = price_str.parse::<i32>() {
                return Some(TradeLog {
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
    data: Rc<RefCell<i32>>,
}

fn main() {
    let ws_factory = || WSLog {
        ts: 0,
        data: "69:BTC".to_string(),
    };
    let trade_factory = || TradeLog {
        price: 0,
        ts: 0,
        pair: "BTC".to_string(),
    };
    let initial_state = || State::default();
    let size = 64;

    let trade_consumer = |s: &mut State, e: &TradeLog, _: Sequence, _: bool| {
        // parse it
        println!(
            "TradeConsumer received: {:?} {:?} {:?} ",
            e.pair, e.price, e.ts
        );
    };

    let mut trade_producer = disruptor::build_single_producer(size, trade_factory, BusySpin)
        .handle_events_and_state_with(trade_consumer, initial_state)
        .build();

    let ws_consumer_trade_parser = move |s: &mut State, e: &WSLog, _: Sequence, _: bool| {
        // parse it
        let trade = TradeLog::from_string(&e.data, e.ts).unwrap();
        println!(
            "WSConsumer received: {:?} {:?} {:?} ",
            trade.pair, trade.price, trade.ts
        );
        // publish it to trade consumer
        trade_producer.publish(|t| {
            t.price = trade.price;
            t.pair = trade.pair;
            t.ts = trade.ts;
        });
    };

    // Closure for processing eventPrices *with* state.
    let mut ws_producer = disruptor::build_single_producer(size, ws_factory, BusySpin)
        .handle_events_and_state_with(ws_consumer_trade_parser, initial_state)
        .build();

    // Start the producer in thread
    thread::scope(|s| {
        s.spawn(move || {
            for i in 0..100 {
                let ws_log = WSLog {
                    data: format!("{}:BTC", i),
                    ts: i,
                };
                ws_producer.publish(|e| {
                    e.data = ws_log.data.clone();
                    e.ts = ws_log.ts;
                });
            }
        });
    });
}
