use crate::command::Command;
use crate::connection::{Connection, Channel};

pub struct Producer {
    address: String,
    connection: Option<Connection>,
    commands: Channel<Command>,
}

impl Producer {
    pub fn new(address: String) -> Self {
        let commands = Channel::unbounded();

        Self {
            address,
            commands,
            connection: None,
        }
    }

    pub fn connect(&mut self) {
        let conn = Connection::connect(&self.address);
        self.connection = Some(conn);
    }

    pub fn publish(&mut self, topic: String, body: Vec<u8>) {
        if let Some(ref mut conn) = self.connection {
            let command = Command::Publish { topic, body };
            conn.send_command(command);
        }
    }
}
