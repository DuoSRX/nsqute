#![allow(dead_code)]

pub mod command;
pub mod connection;
pub mod consumer;
pub mod message;
pub mod producer;
pub mod simple_logger;

use consumer::*;
use simple_logger::SimpleLogger;
// use producer::Producer;

static LOGGER: SimpleLogger = SimpleLogger;

#[tokio::main]
async fn main() {
    log::set_logger(&LOGGER).map(|_| log::set_max_level(log::LevelFilter::Info)).unwrap();

    let mut consumer = Consumer::new("plumber_backfills", "plumber");
    consumer.connect_to_nsqlookupd("http://127.0.0.1:4161/lookup?topic=plumber_backfills").await.unwrap();
    // consumer.connect_to_nsqd("127.0.0.1:4150").await.unwrap();

    // let mut producer = Producer::new("127.0.0.1:4152".to_string());
    // producer.connect().await.unwrap();
    // producer.publish("plumber_backfills".into(), b"foo bar baz"[..].into()).await;

    loop {
        for message in consumer.messages.1.recv().await {
            message.requeue();
            dbg!(message);
        }
    }

    // consumer.done.1.await.unwrap();
}
