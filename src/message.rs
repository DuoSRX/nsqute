use tokio::sync::mpsc::{UnboundedSender};
use std::borrow::Cow;

use crate::command::Command;

pub struct Message {
    pub timestamp: i64,
    pub attempts: u16,
    pub id: String,
    pub body: Vec<u8>,
    pub conn_chan: UnboundedSender<Command>,
}

impl Message {
    pub fn body_as_string(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.body)
    }

    pub fn finish(&self) {
        self.conn_chan.send(Command::Finish(self.id.clone())).unwrap();
    }

    pub fn requeue(&self) {
        self.conn_chan.send(Command::Requeue(self.id.clone(), 5000)).unwrap();
    }

    pub fn touch(&self) {
        self.conn_chan.send(Command::Touch(self.id.clone())).unwrap();
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
