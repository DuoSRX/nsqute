use byteorder::{BigEndian, ByteOrder};
use crossbeam_channel::{Receiver, Sender};
use std::io::prelude::*;
use std::net::TcpStream;
use std::thread;

use crate::channel::Channel;
use crate::command::Command;
use crate::message::Message;

pub struct Connection {
    stream: TcpStream,
    pub messages: Channel<Message>,
    pub commands: Channel<Command>,
}

impl Connection {
    pub fn connect(address: &str) -> std::io::Result<Self> {
        let mut stream = TcpStream::connect(address)?;
        stream.write(b"  V2")?;

        let commands: Channel<Command> = Channel::unbounded();
        let messages: Channel<Message> = Channel::unbounded();

        Connection::read_loop(messages.tx.clone(), commands.tx.clone(), &stream);
        Connection::write_loop(commands.rx.clone(), &stream);

        Ok(Connection {
            stream,
            messages,
            commands,
        })
    }

    pub fn send_command(&mut self, command: Command) {
        self.commands.tx.send(command).unwrap();
    }

    fn write_loop(rx: Receiver<Command>, stream: &TcpStream) {
        let mut stream = stream.try_clone().unwrap();
        thread::spawn(move ||
            loop {
                let cmd = rx.recv().unwrap();
                stream.write_all(&cmd.make()).unwrap();
            }
        );
    }

    fn read_loop(messages: Sender<Message>, commands: Sender<Command>, stream: &TcpStream) {
        let mut stream = stream.try_clone().unwrap();
        thread::spawn(move ||
            loop {
                Connection::read_frame(&mut stream, &messages, &commands).unwrap();
            }
        );
    }

    fn read(stream: &mut TcpStream, n: usize) -> std::io::Result<Vec<u8>> {
        let mut buf: Vec<u8> = vec![0; n];
        stream.read(&mut buf)?;
        Ok(buf)
    }

    fn read_frame(stream: &mut TcpStream, msg_tx: &Sender<Message>, write_tx: &Sender<Command>) -> std::io::Result<()> {
        let mut buf = [0; 4];
        stream.read(&mut buf)?;

        let size = BigEndian::read_u32(&buf);
        let buf = Connection::read(stream, std::cmp::max(size as usize, 4))?;

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

        msg_tx.send(message).unwrap();

        Ok(())
    }
}