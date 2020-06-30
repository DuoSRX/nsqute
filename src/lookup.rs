use actix::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use actix::fut::WrapFuture;

#[derive(Debug, Serialize, Deserialize)]
struct LookupdProducer {
    remote_address: String,
    hostname: String,
    broadcast_address: String,
    tcp_port: u16,
}

#[derive(Debug, Serialize, Deserialize)]
struct LookupdResponse {
    channels: Vec<String>,
    producers: Vec<LookupdProducer>,
}

struct Lookup {}

impl Actor for Lookup {
    type Context = Context<Self>;
}

#[derive(Message)]
#[rtype(result = "()")]
struct DoLookup(String);

impl Handler<DoLookup> for Lookup {
    type Result = ();

    fn handle(&mut self, msg: DoLookup, ctx: &mut Context<Self>) -> Self::Result {
        // let res: LookupdResponse = reqwest::get(&address).await.unwrap().json().await.unwrap();
        // actix::spawn(reqwest::get("blah"));
        let foo = reqwest::get("blah").into_actor(self);
        ()
    }
}

//                 let res: NsqLookupdResponse = reqwest::get(&address).await.unwrap().json().await.unwrap();

//                 for producer in res.producers {
    //                     let address = format!("{}:{}", producer.broadcast_address, producer.tcp_port);
    //                     let has_key = connections.read().await.contains_key(&address);
    //                     if has_key {
        //                         continue
        //                     }

        //                     let connection = Consumer::new_nsqd_connection(&address, &topic, &channel, messages.clone()).await.unwrap();
        //                     let mut conns = connections.write().await;
        //                     conns.insert(address, connection);
        //                 }
        //             }