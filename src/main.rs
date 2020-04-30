use std::io::prelude::*;
use std::net::TcpStream;
use byteorder::{BigEndian, ByteOrder};
use bytes::BufMut;
use crossbeam_channel::{Sender, Receiver};
use std::collections::HashMap;
use std::thread;
use std::borrow::Cow;
use std::sync::Arc;

fn main() -> std::io::Result<()> {
    let mut consumer = Consumer::new("sometopic", "chanchan");
    consumer.add_handler(Box::new(Handler{}));
    consumer.connect_to_nsqd("127.0.0.1:4150");

    for thr in consumer.threads {
        thr.join().unwrap();
    }

    // let identify = "{\"client_id\":\"nsqute\"}".as_bytes();
    // let mut msg = Vec::new();
    // msg.put(&b"IDENTIFY\n"[..]);
    // msg.put_u32(identify.len() as u32);
    // msg.put(identify);
    // stream.write(&msg)?;

    // let mut buf = [0; 4 + 4 + 6];
    // stream.read(&mut buf)?;
    // dbg!(String::from_utf8_lossy(&buf));

    // let message = "foo blah";
    // let mut msg = Vec::new();
    // msg.put(&b"PUB sometopic\n"[..]);
    // msg.put_u32(message.len() as u32);
    // msg.put(message.as_bytes());
    // stream.write(&msg)?;

    Ok(())
}

struct Consumer {
    topic: String,
    channel: String,
    connections: HashMap<String, Connection>,
    msg_tx: Sender<Message>,
    msg_rx: Receiver<Message>,
    pub threads: Vec<thread::JoinHandle<()>>,
}

impl Consumer {
    pub fn new(topic: &str, channel: &str) -> Self {
        let (tx, rx): (Sender<Message>, Receiver<Message>) = crossbeam_channel::unbounded();

        Self {
            topic: topic.to_string(),
            channel: channel.to_string(),
            connections: HashMap::new(),
            msg_tx: tx,
            msg_rx: rx,
            threads: Vec::new(),
        }
    }

    pub fn connect_to_nsqd(&mut self, address: &str) {
        let mut connection = Connection::connect(address, self.msg_tx.clone());

        let msg = format!("SUB {} {}\n", self.topic, self.channel);
        connection.write(msg.as_bytes()).unwrap();
        let res = connection.read(4 + 4 + 6).unwrap();
        dbg!(String::from_utf8_lossy(res.as_slice()));
        connection.write(&b"RDY 1\n"[..]).unwrap();

        thread::spawn(move ||
            connection.read_loop()
        );

        // TODO: Check if we're already connected
        // self.connections.insert(address.to_string(), connection);
    }

    pub fn add_handler(&mut self, handler: Box<dyn MessageHandler + Send>) {
        let rx = self.msg_rx.clone();

        let thr = thread::spawn(move || {
            loop {
                match rx.recv() {
                    Ok(msg) => handler.handle_message(msg),
                    Err(err) => {
                        dbg!(err);
                    }
                }
            }
        });

        self.threads.push(thr);
    }
}

trait MessageHandler {
    fn handle_message(&self, message: Message);
}

struct Handler {}

impl MessageHandler for Handler {
    fn handle_message(&self, message: Message) {
        dbg!(message.body_as_string());
    }
}

#[derive(Debug,PartialEq)]
enum FrameType {
    Response,
    Error,
    Message
}

struct Connection {
    stream: TcpStream,
    msg_tx: Sender<Message>,
}

impl Connection {
    fn connect(address: &str, msg_tx: Sender<Message>) -> Self {
        let mut stream = TcpStream::connect(address).unwrap();
        stream.write(b"  V2").unwrap();

        Self {
            stream,
            msg_tx,
        }
    }

    fn read_loop(&mut self) {
        loop {
            self.read_frame().unwrap();
        }
    }

    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.stream.write(data)
    }

    fn read(&mut self, n: usize) -> std::io::Result<Vec<u8>> {
        let mut buf: Vec<u8> = vec![0; n];
        self.stream.read(&mut buf)?;
        Ok(buf)
    }

    fn read_frame(&mut self) -> std::io::Result<()> {
        let mut buf = [0; 4];
        self.stream.read(&mut buf)?;
        let size = BigEndian::read_u32(&buf);
        dbg!(size);

        let buf = self.read(size as usize)?;

        let frame_type = match BigEndian::read_i32(&buf[0..4]) {
            0 => FrameType::Response,
            1 => FrameType::Error,
            2 => FrameType::Message,
            n => panic!("Unknown frame type: {}", n)
        };
        dbg!(&frame_type);

        if frame_type != FrameType::Message {
            return Ok(())
        }

        let buf = &buf[4..];
        let message = Message {
            timestamp: BigEndian::read_i64(&buf[0..8]),
            attempts:  BigEndian::read_u16(&buf[8..10]),
            id:        String::from_utf8_lossy(&buf[10..26]).into(),
            body:      buf[26..].into(),
        };

        self.msg_tx.send(message).unwrap();
        // self.write(&b"RDY 1\n"[..]).unwrap();

        Ok(())
    }
}

#[derive(Debug)]
struct Message {
    timestamp: i64,
    attempts: u16,
    id: String,
    body: Vec<u8>,
}

impl Message {
    fn body_as_string(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.body)
    }

//     fn finish(&self, stream: &mut TcpStream) {
//         let mut msg = Vec::new();
//         msg.put(&b"FIN "[..]);
//         msg.put(self.id.as_bytes());
//         msg.put(&b"\n"[..]);
//         stream.write(&msg).unwrap();
//     }
}
