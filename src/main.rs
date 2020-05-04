#![allow(dead_code)]

use std::io::prelude::*;
use std::net::TcpStream;
use byteorder::{BigEndian, ByteOrder};
use crossbeam_channel::{Sender, Receiver};
use std::collections::HashMap;
use std::thread;
use std::borrow::Cow;

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
    consumer.connect_to_nsqd("127.0.0.1:4150");

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

#[derive(Clone)]
struct Channel<T> {
    tx: Sender<T>,
    rx: Receiver<T>
}

impl<T> Channel<T> {
    fn unbounded() -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();
        Self { tx, rx }
    }
}

trait MessageHandler {
    fn handle_message(&self, message: Message);
}

struct Consumer {
    topic: String,
    channel: String,
    connections: HashMap<String, Connection>,
    msg_tx: Sender<Message>,
    msg_rx: Receiver<Message>,
    done: Channel<bool>,
}

impl Consumer {
    pub fn new(topic: &str, channel: &str) -> Self {
        let (tx, rx): (Sender<Message>, Receiver<Message>) = crossbeam_channel::unbounded();
        let done = Channel::unbounded();

        Self {
            topic: topic.to_string(),
            channel: channel.to_string(),
            connections: HashMap::new(),
            msg_tx: tx,
            msg_rx: rx,
            done: done,
        }
    }

    pub fn connect_to_nsqd(&mut self, address: &str) {
        let mut connection = Connection::connect(address, self.msg_tx.clone());

        let msg = format!("SUB {} {}\n", self.topic, self.channel);
        connection.write(msg.as_bytes()).unwrap();
        connection.write(&b"RDY 1\n"[..]).unwrap();

        // TODO: Check if we're already connected
        self.connections.insert(address.to_string(), connection);
    }

    pub fn add_handler(&mut self, handler: Box<dyn MessageHandler + Send>) {
        let rx = self.msg_rx.clone();

        thread::spawn(move || {
            loop {
                match rx.recv() {
                    Ok(msg) => handler.handle_message(msg),
                    Err(err) => {
                        dbg!(err);
                    }
                }
            }
        });
    }
}

struct Connection {
    stream: TcpStream,
    write_tx: Sender<Vec<u8>>
}

impl Connection {
    fn connect(address: &str, msg_tx: Sender<Message>) -> Self {
        let mut stream = TcpStream::connect(address).unwrap();
        stream.write(b"  V2").unwrap();

        let tx = msg_tx.clone();
        let mut stream_clone = stream.try_clone().unwrap();
        let mut stream_clone2 = stream.try_clone().unwrap();

        let (write_tx, write_rx) = crossbeam_channel::unbounded();
        let write_tx2 = write_tx.clone();

        let connection = Self {
            stream,
            write_tx
        };

        // Read Loop
        thread::spawn(move ||
            loop {
                Connection::read_frame(&mut stream_clone, &tx, &write_tx2).unwrap();
            }
        );

        // Write Loop
        thread::spawn(move ||
            loop {
                let cmd = write_rx.recv().unwrap();
                stream_clone2.write(&cmd).unwrap();
            }
        );

        connection
    }

    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.stream.write(data)
    }

    fn read(stream: &mut TcpStream, n: usize) -> std::io::Result<Vec<u8>> {
        let mut buf: Vec<u8> = vec![0; n];
        stream.read(&mut buf)?;
        Ok(buf)
    }

    fn read_frame(stream: &mut TcpStream, msg_tx: &Sender<Message>, write_tx: &Sender<Vec<u8>>) -> std::io::Result<()> {
        let mut buf = [0; 4];
        stream.read(&mut buf)?;

        let size = BigEndian::read_u32(&buf);
        let buf = Connection::read(stream, std::cmp::max(size as usize, 4))?;

        match BigEndian::read_i32(&buf[0..4]) {
            0 => { // Response
                dbg!(String::from_utf8_lossy(&buf[4..]));
                return Ok(())
            },
            1 => { // Error
                dbg!(size);
                dbg!(String::from_utf8_lossy(&buf[4..]));
                return Ok(())
            },
            2 => {}, // Message
            n => panic!("Unknown frame type: {}", n)
        };

        let buf = &buf[4..];
        let message = Message {
            timestamp: BigEndian::read_i64(&buf[0..8]),
            attempts:  BigEndian::read_u16(&buf[8..10]),
            id:        String::from_utf8_lossy(&buf[10..26]).into(),
            body:      buf[26..].into(),
            conn_chan: write_tx.clone(),
        };

        msg_tx.send(message).unwrap();

        Ok(())
    }
}

struct Message {
    timestamp: i64,
    attempts: u16,
    id: String,
    body: Vec<u8>,
    conn_chan: Sender<Vec<u8>>,
}

impl Message {
    fn body_as_string(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.body)
    }

    fn finish(&self) {
        let fin = format!("FIN {}\n", self.id);
        self.conn_chan.send(fin.into()).unwrap();
    }

    fn requeue(&self) {
        let req = format!("REQ {} 5000\n", self.id);
        self.conn_chan.send(req.into()).unwrap();
    }

    fn touch(&self) {
        let touch = format!("TOUCH {}\n", self.id);
        self.conn_chan.send(touch.into()).unwrap();
    }
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Message")
            .field("id", &self.id)
            .field("timestamp", &self.timestamp)
            .field("attempts", &self.attempts)
            .field("body", &self.body_as_string())
            .finish()
    }
}
