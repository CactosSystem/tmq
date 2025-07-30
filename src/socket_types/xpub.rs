use zmq::Context as ZmqContext;

use crate::{
    comm::SenderReceiver, poll::ZmqPoller, socket::AsZmqSocket, FromZmqSocket, Result,
    SocketBuilder,
};

/// Create a builder for a XPUB socket.
///
/// ## Usage Example
///
/// ```rust,no_run
/// use tmq::{xpub, Context, Result};
/// use futures::SinkExt;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let mut socket = xpub(&Context::new()).bind("tcp://127.0.0.1:7899")?;
///     
///     // Enable verbose mode to receive all subscription messages
///     socket.set_xpub_verbose(true)?;
///     
///     // Send a message to subscribers
///     socket.send(vec!["topic", "Hello XPUB"]).await?;
///     
///     // You can also receive subscription/unsubscription messages
///     use futures::StreamExt;
///     if let Some(Ok(msg)) = socket.next().await {
///         println!("Received subscription: {:?}", msg);
///     }
///     
///     Ok(())
/// }
/// ```
pub fn xpub(context: &ZmqContext) -> SocketBuilder<Xpub> {
    SocketBuilder::new(context, zmq::SocketType::XPUB)
}

/// Asynchronous XPUB socket.
///
/// The XPUB socket is like PUB except that it can also receive
/// subscription messages from subscribers.
pub struct Xpub {
    inner: SenderReceiver,
}

impl FromZmqSocket<Xpub> for Xpub {
    fn from_zmq_socket(socket: zmq::Socket) -> crate::Result<Self> {
        Ok(Self {
            inner: SenderReceiver::new(ZmqPoller::from_zmq_socket(socket)?),
        })
    }
}

impl_wrapper!(Xpub, SenderReceiver, inner);
impl_wrapper_sink!(Xpub, inner);
impl_wrapper_stream!(Xpub, inner);

impl Xpub {
    /// Enable verbose mode to receive all subscription messages.
    ///
    /// By default, XPUB only notifies about unique subscriptions or
    /// when the last unsubscribe for a topic happens. When verbose mode
    /// is enabled, it notifies for all subscriptions, even duplicates.
    pub fn set_xpub_verbose(&mut self, verbose: bool) -> Result<()> {
        self.get_socket().set_xpub_verbose(verbose)?;
        Ok(())
    }
    
    /// Set welcome message for new subscribers.
    ///
    /// This message will be sent to any new subscriber that connects to 
    /// this socket, regardless of their subscription filter.
    pub fn set_xpub_welcome_msg(&mut self, msg: Option<&str>) -> Result<()> {
        self.get_socket().set_xpub_welcome_msg(msg)?;
        Ok(())
    }
    
    /// Get the last bound endpoint.
    ///
    /// Useful when binding to an ephemeral port (using * wildcard).
    pub fn get_last_endpoint(&self) -> Result<String> {
        match self.get_socket().get_last_endpoint() {
            Ok(Ok(endpoint)) => Ok(endpoint),
            Ok(Err(_)) => Err(crate::TmqError::Zmq(zmq::Error::EINVAL)),
            Err(e) => Err(crate::TmqError::Zmq(e)),
        }
    }

    /// Set manual subscription handling mode.
    ///
    /// By default, the XPUB socket automatically manages subscriptions.
    /// When you enable manual mode, the application is responsible for
    /// sending subscription messages to downstream subscribers.
    ///
    /// Note: This is a ZeroMQ socket option that may not be available in all versions.
    /// Check the ZeroMQ documentation for your version.
    #[cfg(feature = "manual")]
    pub fn set_xpub_manual(&mut self, manual: bool) -> Result<()> {
        self.get_socket().set_sockopt(zmq::XPUB_MANUAL, manual)?;
        Ok(())
    }

    /// Set message replay or "last value" cache behavior.
    ///
    /// This option sets whether to provide a message cache to new subscriptions.
    /// It's useful for implementing "last value caching" for late joining subscribers.
    ///
    /// Note: This is a ZeroMQ socket option that may not be available in all versions.
    /// Check the ZeroMQ documentation for your version.
    #[cfg(feature = "nodrop")]
    pub fn set_xpub_nodrop(&mut self, nodrop: bool) -> Result<()> {
        self.get_socket().set_sockopt(zmq::XPUB_NODROP, nodrop)?;
        Ok(())
    }
}