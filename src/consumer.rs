use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc};
use tokio::sync::{oneshot,mpsc,RwLock};
use tokio::sync::oneshot::{Sender, Receiver};
use tokio::sync::mpsc::{UnboundedSender,UnboundedReceiver};

use crate::command::Command;
use crate::message::Message;
use crate::connection::Connection;

pub struct Consumer {
    topic: String,
    channel: String,
    connections: Arc<RwLock<HashMap<String, Connection>>>,
    max_in_flight: usize,
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
            connections: Arc::new(RwLock::new(HashMap::new())),
            max_in_flight: 4,
            done,
            messages,
        }
    }

    pub async fn connect_to_nsqlookupd(&mut self, address: &str) -> Result<(), Box<dyn std::error::Error>> {
        let connections = self.connections.clone();
        let topic = self.topic.clone();
        let channel = self.channel.clone();
        let address = String::from(address);
        let messages = self.messages.0.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));

            loop {
                interval.tick().await;

                let res: NsqLookupdResponse = reqwest::get(&address).await.unwrap().json().await.unwrap();

                for producer in res.producers {
                    let address = format!("{}:{}", producer.broadcast_address, producer.tcp_port);
                    let has_key = connections.read().await.contains_key(&address);
                    if has_key {
                        continue
                    }

                    let connection = Consumer::new_nsqd_connection(&address, topic.clone(), channel.clone(), messages.clone()).await.unwrap();
                    let mut conns = connections.write().await;
                    conns.insert(address, connection);
                }
            }
        });

        Ok(())
    }

    async fn new_nsqd_connection(address: &str, topic: String, channel: String, messages: UnboundedSender<Message>) -> std::io::Result<Connection> {
        let mut connection = Connection::connect(address, Some(messages)).await?;

        connection.send_command(Command::Subscribe { topic: topic, channel: channel }).await;
        connection.send_command(Command::Ready(2)).await;

        Ok(connection)
    }

    pub async fn connect_to_nsqd(&mut self, address: &str) -> std::io::Result<()> {
        let messages = self.messages.0.clone();
        let connection = Consumer::new_nsqd_connection(address, self.topic.clone(), self.channel.clone(), messages).await?;

        // TODO: Check if we're already connected
        let mut conns = self.connections.write().await;
        conns.insert(address.to_string(), connection);

        Ok(())
    }

    // fn per_connection_max_in_flight(&self) -> u64 {
    //     let conns = (*self.connections.read().await).len();
    //     let a = self.max_in_flight as f64;
    //     let s = a / conns as f64;
    //     s.max(1.0).min(a) as u64
    // }
}
