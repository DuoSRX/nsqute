use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{oneshot,mpsc,RwLock};
use tokio::sync::oneshot::{Sender, Receiver};
use tokio::sync::mpsc::{UnboundedSender,UnboundedReceiver};

use crate::connection::Connection;
use crate::lookup::Lookup;
use crate::message::Message;

pub struct Consumer {
    topic: String,
    channel: String,
    connections: Arc<RwLock<HashMap<String, Connection>>>,
    current_max_in_flight: u64,
    total_rdy: Arc<AtomicU64>,
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
            current_max_in_flight: 4,
            total_rdy: Arc::new(AtomicU64::new(0)),
            done,
            messages,
        }
    }

    pub async fn connect_to_nsqlookupd(&mut self, address: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut lookup = Lookup::new(&self.topic, &self.channel);
        let mut rx = lookup.connect(address).await;
        let connections = self.connections.clone();
        let topic = self.topic.clone();
        let channel = self.channel.clone();
        let messages = self.messages.0.clone();

        tokio::spawn(async move {
            loop {
                let nsqds = rx.recv().await.unwrap(); // TODO: Handle recv errors (is it even likely?)
                for nsq in nsqds {
                    let has_key = connections.read().await.contains_key(&nsq);
                    if has_key { continue }
                    let connection = Consumer::new_nsqd_connection(&nsq, &topic, &channel, messages.clone()).await.unwrap();
                    let mut conns = connections.write().await;
                    conns.insert(nsq, connection);

                    let futures = (*conns).iter_mut().map(|(_addr, conn)| {
                        self.try_update_rdy(conn)
                    });
                    for future in futures {
                        future.await;
                    }
                }
            }
        });

        Ok(())
    }

    async fn new_nsqd_connection(address: &str, topic: &str, channel: &str, messages: UnboundedSender<Message>) -> std::io::Result<Connection> {
        let mut connection = Connection::connect(address, Some(messages)).await?;
        connection.subscribe(topic, channel).await;
        connection.ready(2).await;

        Ok(connection)
    }

    pub async fn connect_to_nsqd(&mut self, address: &str) -> std::io::Result<()> {
        let messages = self.messages.0.clone();
        let connection = Consumer::new_nsqd_connection(address, &self.topic, &self.channel, messages).await?;

        // TODO: Check if we're already connected
        let mut conns = self.connections.write().await;
        conns.insert(address.to_string(), connection);

        let futures = (*conns).iter_mut().map(|(_addr, conn)| {
            self.try_update_rdy(conn)
        });
        for future in futures {
            future.await;
        }

        Ok(())
    }

    async fn per_connection_max_in_flight(&self) -> u64 {
        let conns = (*self.connections.read().await).len();
        let a = self.current_max_in_flight as f64;
        let s = a / conns as f64;
        s.max(1.0).min(a) as u64
    }

    async fn try_update_rdy(&self, conn: &mut Connection) {
        // TODO: Handle backoff: we shouldn't send new RDY count while backing off
        let mif = self.per_connection_max_in_flight().await;
        self.update_rdy(conn, mif).await;
    }

    async fn update_rdy(&self, conn: &mut Connection, max_in_flight: u64) {
        // TODO: Handle connection closing
        let mut count = max_in_flight;

        // Never exceeds the configured max RDY or nsqd is gonna be angry
        if max_in_flight > conn.max_rdy {
            count = conn.max_rdy
        }

        let rdy_count = conn.rdy;
        let max_possible_rdy = self.current_max_in_flight - self.total_rdy.load(Ordering::SeqCst) + rdy_count;

        if max_possible_rdy > 0 && max_possible_rdy < count {
            count = max_possible_rdy;
        }

        // TODO: Handle potential case where mpr <= 0 AND count > 0
        self.send_rdy(conn, count).await;
    }

    async fn send_rdy(&self, conn: &mut Connection, count: u64) {
        if count == 0 && conn.last_rdy == 0 {
            return // Nothing to do
        }

        self.total_rdy.fetch_add(count - conn.rdy, Ordering::SeqCst);
        conn.set_rdy(count).await;
    }
}
