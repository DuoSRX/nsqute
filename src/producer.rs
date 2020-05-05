use crate::command::Command;
use crate::connection::{Connection, Channel};

pub struct Producer<'a> {
    address: String,
    connection: Option<Connection>,
    commands: Channel<Command<'a>>,
}

impl Producer<'_> {
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

    pub fn publish(&mut self, topic: &str, body: Vec<u8>) {
        let command = Command::Publish { topic, body };
        self.send_command(command);
    }

    fn send_command(&mut self, command: Command) {
        let msg = command.make();
        self.connection.as_ref().unwrap().commands.tx.send(msg).unwrap();
    }
}
