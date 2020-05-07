use crate::command::Command;
use crate::connection::Connection;

pub struct Producer {
    address: String,
    connection: Option<Connection>,
}

impl Producer {
    pub fn new(address: String) -> Self {
        Self {
            address,
            connection: None,
        }
    }

    pub async fn connect(&mut self) -> std::io::Result<()> {
        let conn = Connection::connect(&self.address, None).await?;
        self.connection = Some(conn);
        Ok(())
    }

    pub async fn publish(&mut self, topic: String, body: Vec<u8>) {
        if let Some(ref mut conn) = self.connection {
            let command = Command::Publish { topic, body };
            conn.send_command(command).await;
        }
    }
}
