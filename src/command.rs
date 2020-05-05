use bytes::{BufMut};

#[derive(Debug)]
pub enum Command<'a> {
    Nop,
    Ready(usize),
    Publish { topic: &'a str, body: Vec<u8> },
    Subscribe { topic: &'a str, channel: &'a str }
}

impl Command<'_> {
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
            Ready(n) => {
              format!("RDY {}\n", n).into_bytes()
            },
            Subscribe { topic, channel } => {
                format!("SUB {} {}\n", topic, channel).into_bytes()
            }
        }
    }
}
