use disruptor::BusySpin;
use disruptor::*;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use std::{
    os::unix::process,
    sync::{
        atomic::{
            AtomicBool, AtomicI64,
            Ordering::{Acquire, Relaxed, Release},
        },
        Arc,
    },
};

pub struct Message {
    pub data: i64,
}

pub trait Actor {
    fn handle(&mut self, msg: Message);
}

pub struct PrintActor;

impl Actor for PrintActor {
    fn handle(&mut self, msg: Message) {
        println!("Processing message with data: {}", msg.data);
    }
}

pub struct ActorSystem {
    producers: Vec<BurstProducer>,
    consumer: Arc<AtomicI64>,
}

impl ActorSystem {
    pub fn new(producers: Vec<BurstProducer>) -> Self {
        let consumer = Arc::new(AtomicI64::new(0));
        Self {
            producers,
            consumer,
        }
    }

    pub fn start<A: Actor>(&mut self, mut actor: A) {
        let sink = Arc::clone(&self.consumer);
        let processor = move |event: &Message, _sequence: i64, _end_of_batch: bool| {
            actor.handle(Message { data: event.data });
            sink.fetch_add(1, Release);
        };

        let factory = || Message { data: 0 };
        let producer = disruptor::build_multi_producer(256, factory, BusySpin)
            .handle_events_with(processor)
            .build();

        for burst_producer in &self.producers {
            burst_producer.start();
        }

        // Processing loop or logic for actor execution
        thread::spawn(move || loop {
            // Actively poll the sink or use an event loop for handling
        });
    }

    pub fn stop(&mut self) {
        for burst_producer in &mut self.producers {
            burst_producer.stop();
        }
    }
}

pub struct BurstProducer {
    start_barrier: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    join_handle: Option<JoinHandle<()>>,
}

impl BurstProducer {
    pub fn new(mut produce_one_burst: impl FnMut() + Send + 'static) -> Self {
        let start_barrier = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));

        let join_handle = {
            let stop = Arc::clone(&stop);
            let start_barrier = Arc::clone(&start_barrier);
            thread::spawn(move || {
                while !stop.load(Acquire) {
                    while start_barrier
                        .compare_exchange(true, false, Acquire, Relaxed)
                        .is_err()
                    {
                        if stop.load(Acquire) {
                            return;
                        }
                    }
                    produce_one_burst();
                }
            })
        };

        Self {
            start_barrier,
            stop,
            join_handle: Some(join_handle),
        }
    }

    pub fn start(&self) {
        self.start_barrier.store(true, Release);
    }

    pub fn stop(&mut self) {
        self.stop.store(true, Release);
        self.join_handle
            .take()
            .unwrap()
            .join()
            .expect("Thread should join cleanly");
    }
}

fn main() {
    let burst_size = 10;
    let mut producers = Vec::new();

    for _ in 0..2 {
        // Example with 2 producers
        let producer = BurstProducer::new(move || {
            for _ in 0..burst_size {
                // Produce a burst of messages
            }
        });
        producers.push(producer);
    }

    let mut system = ActorSystem::new(producers);
    let print_actor = PrintActor;

    system.start(print_actor);

    // Simulate some time for processing
    thread::sleep(Duration::from_secs(5));

    // Stop the system gracefully
    system.stop();
}
