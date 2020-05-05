use byteorder::{BigEndian, ByteOrder};
use crossbeam_channel::{Sender};
use std::io::prelude::*;
use std::net::TcpStream;
use std::thread;

use crate::message::Message;

pub struct Connection {
    stream: TcpStream,
    write_tx: Sender<Vec<u8>>
}

impl Connection {
    pub fn connect(address: &str, msg_tx: Sender<Message>) -> Self {
        let mut stream = TcpStream::connect(address).unwrap();
        stream.write(b"  V2").unwrap();

        let tx = msg_tx.clone();
        let mut stream_clone = stream.try_clone().unwrap();
        let mut stream_clone2 = stream.try_clone().unwrap();

        let (write_tx, write_rx) = crossbeam_channel::unbounded();
        let write_tx2 = write_tx.clone();

        let connection = Self {
            stream,
            write_tx
        };

        // Read Loop
        thread::spawn(move ||
            loop {
                Connection::read_frame(&mut stream_clone, &tx, &write_tx2).unwrap();
            }
        );

        // Write Loop
        thread::spawn(move ||
            loop {
                let cmd = write_rx.recv().unwrap();
                stream_clone2.write(&cmd).unwrap();
            }
        );

        connection
    }

    pub fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.stream.write(data)
    }

    pub fn read(stream: &mut TcpStream, n: usize) -> std::io::Result<Vec<u8>> {
        let mut buf: Vec<u8> = vec![0; n];
        stream.read(&mut buf)?;
        Ok(buf)
    }

    fn read_frame(stream: &mut TcpStream, msg_tx: &Sender<Message>, write_tx: &Sender<Vec<u8>>) -> std::io::Result<()> {
        let mut buf = [0; 4];
        stream.read(&mut buf)?;

        let size = BigEndian::read_u32(&buf);
        let buf = Connection::read(stream, std::cmp::max(size as usize, 4))?;

        match BigEndian::read_i32(&buf[0..4]) {
            0 => { // Response
                dbg!(String::from_utf8_lossy(&buf[4..]));
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