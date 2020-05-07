use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::{oneshot,mpsc};
use tokio::sync::oneshot::{Sender, Receiver};
use tokio::sync::mpsc::{UnboundedSender,UnboundedReceiver};

// use crate::channel::Channel;
use crate::command::Command;
use crate::message::Message;
use crate::connection::Connection;

pub trait MessageHandler {
    fn handle_message(&self, message: Message);
}

pub struct Consumer {
    topic: String,
    channel: String,
    connections: HashMap<String, Connection>,
    pub messages: (UnboundedSender<Message>, UnboundedReceiver<Message>),
    pub done: (Sender<bool>, Receiver<bool>),
}

#[derive(Debug, Serialize, Deserialize)]
struct NsqLookupdProducer {
    remote_address: String,
    hostname: String,
    broadcast_address: String,
    tcp_port: u16,
}

#[derive(Debug, Serialize, Deserialize)]
struct NsqLookupdResponse {
    channels: Vec<String>,
    producers: Vec<NsqLookupdProducer>,
}

impl Consumer {
    pub fn new(topic: &str, channel: &str) -> Self {
        let messages = mpsc::unbounded_channel();
        let done = oneshot::channel();

        Self {
            topic: topic.to_string(),
            channel: channel.to_string(),
            connections: HashMap::new(),
            done,
            messages,
        }
    }

    // pub fn connect_to_nsqlookupd(&mut self, address: &str) {
    //     let res: NsqLookupdResponse = reqwest::blocking::get(address).unwrap().json().unwrap();

    //     for producer in res.producers {
    //         let broadcast: &str = &producer.broadcast_address;
    //         let address = (broadcast, producer.tcp_port).to_socket_addrs().unwrap().next().unwrap();
    //         // TODO: Remove handle errors here
    //         self.connect_to_nsqd(&address.to_string()).unwrap()
    //     }
    // }

    pub async fn connect_to_nsqd(&mut self, address: &str) -> std::io::Result<()> {
        let mut connection = Connection::connect(address).await?;

        connection.send_command(Command::Subscribe { topic: self.topic.clone(), channel: self.channel.clone() }).await;
        connection.send_command(Command::Ready(2)).await;

        let incoming_messages = self.messages.0.clone();

        tokio::spawn(async move {
            loop {
                let message = connection.messages.recv().await.unwrap();
                incoming_messages.send(message).unwrap();
            }
        });

        // TODO: Check if we're already connected
        // self.connections.insert(address.to_string(), connection);

        Ok(())
    }

    // pub fn add_handler(&mut self, handler: Box<dyn MessageHandler + Send>) {
    //     let rx = self.messages.1.clone();

    //     tokio::spawn(async move {
    //         loop {
    //             match rx.recv().await {
    //                 Some(msg) => handler.handle_message(msg),
    //                 None => (),
    //             }
    //         }
    //     });
    // }
}
