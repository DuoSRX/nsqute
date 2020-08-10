use log::{warn};
use serde::{Deserialize, Serialize};
use tokio::sync::{watch, watch::Receiver};

pub struct Lookup {
    topic: String,
    channel: String,
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

impl Lookup {
    pub fn new(topic: &str, channel: &str) -> Self {
        Self {
            topic: topic.to_string(),
            channel: channel.to_string(),
        }
    }

    pub async fn connect(&mut self, address: &str) -> Receiver<Vec<String>> {
        let address = String::from(address);
        let (tx, rx) = watch::channel(vec![]);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));

            loop {
                interval.tick().await;

                let res = reqwest::get(&address).await.unwrap().json::<NsqLookupdResponse>().await;
                res.map_or_else(
                    |err| warn!("Lookupd error: {}", err),
                    |response| {
                        let producers = response.producers.iter().map(|p|
                            format!("{}:{}", p.broadcast_address, p.tcp_port)
                        ).collect();
                        tx.broadcast(producers).unwrap();
                    }
                )
            }
        });

        rx
    }
}
