use super::{Listener, Transport};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone)]
pub struct TokioTransport;

impl Transport for TokioTransport {
    type Listener = TokioListener;
    type Stream = TcpStream;

    fn bind(&self, addr: SocketAddr) -> impl Future<Output = io::Result<Self::Listener>> + Send {
        async move {
            let listener = TcpListener::bind(addr).await?;
            Ok(TokioListener(listener))
        }
    }

    fn connect(&self, addr: SocketAddr) -> impl Future<Output = io::Result<Self::Stream>> + Send {
        TcpStream::connect(addr)
    }
}

pub struct TokioListener(TcpListener);

impl Listener for TokioListener {
    type Stream = TcpStream;

    fn accept(&self) -> impl Future<Output = io::Result<(Self::Stream, SocketAddr)>> + Send {
        self.0.accept()
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.local_addr()
    }
} 