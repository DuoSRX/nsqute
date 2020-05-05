use crossbeam_channel::{Sender};
use std::borrow::Cow;

pub struct Message {
    pub timestamp: i64,
    pub attempts: u16,
    pub id: String,
    pub body: Vec<u8>,
    pub conn_chan: Sender<Vec<u8>>,
}

impl Message {
    pub fn body_as_string(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.body)
    }

    pub fn finish(&self) {
        let fin = format!("FIN {}\n", self.id);
        self.conn_chan.send(fin.into()).unwrap();
    }

    pub fn requeue(&self) {
        let req = format!("REQ {} 5000\n", self.id);
        self.conn_chan.send(req.into()).unwrap();
    }

    pub fn touch(&self) {
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
