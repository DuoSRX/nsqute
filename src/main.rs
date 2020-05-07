#![allow(dead_code)]

pub mod channel;
pub mod connection;
pub mod command;
pub mod consumer;
pub mod message;
pub mod producer;

use consumer::*;
use producer::Producer;

#[tokio::main]
async fn main() {
    let mut consumer = Consumer::new("plumber_backfills", "plumber");
    // consumer.connect_to_nsqlookupd("http://127.0.0.1:4161/lookup?topic=plumber_backfills");
    consumer.connect_to_nsqd("127.0.0.1:4150").await.unwrap();

    let mut producer = Producer::new("127.0.0.1:4150".to_string());
    producer.connect().await.unwrap();
    producer.publish("plumber_backfills".into(), b"foo bar baz"[..].into()).await;

    loop {
        for message in consumer.messages.1.recv().await {
            message.requeue();
            dbg!(message);
        }
    }

    // consumer.done.1.await.unwrap();
}

//     // let identify = "{\"client_id\":\"nsqute\"}".as_bytes();
//     // let mut msg = Vec::new();
//     // msg.put(&b"IDENTIFY\n"[..]);
//     // msg.put_u32(identify.len() as u32);
//     // msg.put(identify);
//     // stream.write(&msg)?;

//     // let mut buf = [0; 4 + 4 + 6];
//     // stream.read(&mut buf)?;
//     // dbg!(String::from_utf8_lossy(&buf));