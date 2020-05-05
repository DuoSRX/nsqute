use crossbeam_channel::{Receiver,Sender};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::thread;

use crate::message::Message;
use crate::connection::Connection;

#[derive(Clone)]
pub struct Channel<T> {
    pub tx: Sender<T>,
    pub rx: Receiver<T>
}

impl<T> Channel<T> {
    fn unbounded() -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();
        Self { tx, rx }
    }
}

pub trait MessageHandler {
    fn handle_message(&self, message: Message);
}

pub struct Consumer {
    topic: String,
    channel: String,
    connections: HashMap<String, Connection>,
    msg_tx: Sender<Message>,
    msg_rx: Receiver<Message>,
    pub done: Channel<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
struct NsqLookupdProducer {
    remote_address: String,
    hostname: String,
    tcp_port: u16,
}

#[derive(Debug, Serialize, Deserialize)]
struct NsqLookupdResponse {
    channels: Vec<String>,
    producers: Vec<NsqLookupdProducer>,
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

    pub fn connect_to_nsqlookupd(&mut self, address: &str) {
        let res: NsqLookupdResponse = reqwest::blocking::get(address).unwrap().json().unwrap();

        for producer in res.producers {
            let hostname: &str = &producer.hostname;
            let address = (hostname, producer.tcp_port).to_socket_addrs().unwrap().next().unwrap();
            self.connect_to_nsqd(&address.to_string())
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