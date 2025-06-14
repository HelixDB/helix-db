pub mod tokio_transport;

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncWrite};

/// A trait for objects that can be read from and written to asynchronously.
///
/// This is a marker trait that is automatically implemented for any type that
/// implements `AsyncRead`, `AsyncWrite`, `Send`, `Sync`, and `Unpin`.
pub trait Stream: AsyncRead + AsyncWrite + Send + Sync + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Sync + Unpin> Stream for T {}

/// A trait for listeners that can accept incoming connections.
pub trait Listener: Send + Sync {
    /// The type of stream that this listener produces.
    type Stream: Stream;

    /// Accepts a new incoming connection from this listener.
    fn accept(&self) -> impl Future<Output = io::Result<(Self::Stream, SocketAddr)>> + Send;

    /// Returns the local address that this listener is bound to.
    fn local_addr(&self) -> io::Result<SocketAddr>;
}

/// A trait for transports that can create listeners and connect to peers.
pub trait Transport: Send + Sync + 'static {
    /// The type of listener that this transport produces.
    type Listener: Listener<Stream = Self::Stream>;

    /// The type of stream that this transport produces.
    type Stream: Stream;

    /// Creates a new listener which will be bound to the specified address.
    fn bind(&self, addr: SocketAddr) -> impl Future<Output = io::Result<Self::Listener>> + Send;

    /// Opens a connection to a remote host.
    fn connect(&self, addr: SocketAddr) -> impl Future<Output = io::Result<Self::Stream>> + Send;
} 