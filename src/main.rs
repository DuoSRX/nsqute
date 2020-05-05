#![allow(dead_code)]

pub mod connection;
pub mod consumer;
pub mod message;

use consumer::*;
use message::Message;

struct Handler {}

impl MessageHandler for Handler {
    fn handle_message(&self, message: Message) {
        dbg!(&message);
        message.requeue();
    }
}

fn main() -> std::io::Result<()> {
    let mut consumer = Consumer::new("plumber_backfills", "plumber");
    consumer.add_handler(Box::new(Handler{}));
    consumer.connect_to_nsqlookupd("http://127.0.0.1:4161/lookup?topic=plumber_backfills");

    let _ = consumer.done.rx.recv();

    // let identify = "{\"client_id\":\"nsqute\"}".as_bytes();
    // let mut msg = Vec::new();
    // msg.put(&b"IDENTIFY\n"[..]);
    // msg.put_u32(identify.len() as u32);
    // msg.put(identify);
    // stream.write(&msg)?;

    // let mut buf = [0; 4 + 4 + 6];
    // stream.read(&mut buf)?;
    // dbg!(String::from_utf8_lossy(&buf));

    Ok(())
}
