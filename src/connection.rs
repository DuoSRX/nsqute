use byteorder::{BigEndian, ByteOrder};
use tokio::prelude::*;
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf};
use tokio::sync::{mpsc};
use tokio::sync::mpsc::{UnboundedSender,UnboundedReceiver};

use crate::command::Command;
use crate::message::Message;

struct ConnectionState {
    max_rdy: u64,
    last_rdy: u64,
    rdy: u64,
}

impl ConnectionState {
    pub fn new() -> Self {
        Self {
            max_rdy: 10, // TODO: use the IDENTIFY response max_rdy
            last_rdy: 0,
            rdy: 0,
        }
    }
}

pub struct Connection {
    pub commands: UnboundedSender<Command>,
}

impl Connection {
    pub async fn connect(address: &str, messages: Option<UnboundedSender<Message>>) -> std::io::Result<Self> {
        let mut stream = TcpStream::connect(address).await?;
        stream.write_all(b"  V2").await?;

        let (mut r, mut w) = stream.into_split();

        let (cmd_tx, mut cmd_rx): (UnboundedSender<Command>, UnboundedReceiver<Command>) = mpsc::unbounded_channel();

        // let state = Arc::new(RwLock::new(ConnectionState::new()));

        tokio::spawn(async move {
            loop {
                let cmd = cmd_rx.recv().await.unwrap();
                w.write_all(&cmd.make()).await.unwrap();
                dbg!(&cmd);
            }
        });

        let cmd = cmd_tx.clone();
        tokio::spawn(async move {
            loop {
                Connection::read_frame(&mut r, &messages, &cmd).await.unwrap();
            }
        });

        Ok(Connection {
            commands: cmd_tx,
        })
    }

    pub async fn ready(&mut self, n: usize) {
        self.send_command(Command::Ready(n)).await;
    }

    pub async fn subscribe(&mut self, topic: &str, channel: &str) {
        let cmd = Command::Subscribe { topic: topic.into(), channel: channel.into() };
        self.send_command(cmd).await;
    }

    pub async fn send_command(&mut self, command: Command) {
        self.commands.send(command).unwrap();
    }

    async fn read_frame(stream: &mut OwnedReadHalf, msg_tx: &Option<UnboundedSender<Message>>, write_tx: &UnboundedSender<Command>) -> std::io::Result<()> {
        let mut buf = [0; 4];
        stream.read_exact(&mut buf).await?;

        let size = BigEndian::read_u32(&buf);
        let mut buf: Vec<u8> = vec![0; std::cmp::max(size as usize, 4)];
        stream.read_exact(&mut buf).await?;

        match BigEndian::read_i32(&buf[0..4]) {
            0 => { // Response
                // dbg!(String::from_utf8_lossy(&buf[4..]));
                return Ok(())
            },
            1 => { // Error
                dbg!(size);
                dbg!(String::from_utf8_lossy(&buf[4..]));
                return Ok(())
            },
            2 => {}, // Message
            n => panic!("Unknown frame type: {}", n)
        };

        let buf = &buf[4..];
        let message = Message {
            timestamp: BigEndian::read_i64(&buf[0..8]),
            attempts:  BigEndian::read_u16(&buf[8..10]),
            id:        String::from_utf8_lossy(&buf[10..26]).into(),
            body:      buf[26..].into(),
            conn_chan: write_tx.clone(),
        };

        // dbg!(&message);
        if let Some(tx) = msg_tx {
            tx.send(message).unwrap();
        }


        Ok(())
    }
}