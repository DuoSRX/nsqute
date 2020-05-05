use crossbeam_channel::{RecvError, SendError, Receiver, Sender};

use std::clone::Clone;

pub struct Channel<T> {
    pub tx: Sender<T>,
    pub rx: Receiver<T>
}

impl<T> Channel<T> {
    pub fn unbounded() -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();
        Self { tx, rx }
    }

    pub fn send(&self, msg: T) -> Result<(), SendError<T>> {
        self.tx.send(msg)
    }

    pub fn recv(&self) -> Result<T, RecvError> {
        self.rx.recv()
    }
}

impl<T> Clone for Channel<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            rx: self.rx.clone(),
        }
    }
}