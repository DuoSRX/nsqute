use actix::prelude::*;
use bytes::{BufMut};

#[derive(Debug)]
#[derive(Message)]
#[rtype(result = "()")]
pub enum Command {
    Nop,
    Finish(String),
    Touch(String),
    Requeue(String, u32),
    Ready(usize),
    Publish { topic: String, body: Vec<u8> },
    Subscribe { topic: String, channel: String }
}

impl Command {
    pub fn make(&self) -> Vec<u8> {
        use Command::*;

        match self {
            Nop => b"NOP\n".to_vec(),
            Publish { topic, body } => {
                let mut msg = Vec::new();
                msg.put(&b"PUB "[..]);
                msg.put(topic.as_bytes());
                msg.put(&b"\n"[..]);
                msg.put_u32(body.len() as u32);
                msg.put_slice(body);
                msg
            },
            Finish(id) => {
                format!("FIN {}\n", id).into_bytes()
            },
            Touch(id) => {
                format!("TOUCH {}\n", id).into_bytes()
            },
            Requeue(id, n) => {
                format!("REQ {} {}\n", id, n).into_bytes()
            },
            Ready(n) => {
                format!("RDY {}\n", n).into_bytes()
            },
            Subscribe { topic, channel } => {
                format!("SUB {} {}\n", topic, channel).into_bytes()
            }
        }
    }
}
