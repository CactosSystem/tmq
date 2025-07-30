use zmq::Context as ZmqContext;

use crate::{
    comm::SenderReceiver, poll::ZmqPoller, FromZmqSocket, Result, SocketBuilder,
};

/// Create a builder for a XSUB socket.
///
/// ## Usage Example
///
/// ```rust,no_run
/// use tmq::{xsub, Context, Result};
/// use futures::{SinkExt, StreamExt};
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let mut socket = xsub(&Context::new()).connect("tcp://127.0.0.1:7899")?;
///     
///     // Subscribe to a topic
///     socket.subscribe(b"topic").await?;
///     
///     // Receive messages
///     if let Some(Ok(msg)) = socket.next().await {
///         println!("Received: {:?}", msg);
///     }
///     
///     // Unsubscribe from a topic
///     socket.unsubscribe(b"topic").await?;
///     
///     Ok(())
/// }
/// ```
pub fn xsub(context: &ZmqContext) -> SocketBuilder<Xsub> {
    SocketBuilder::new(context, zmq::SocketType::XSUB)
}

/// Asynchronous XSUB socket.
///
/// The XSUB socket is like SUB except that it can also send
/// subscription messages to publishers.
pub struct Xsub {
    inner: SenderReceiver,
}

impl FromZmqSocket<Xsub> for Xsub {
    fn from_zmq_socket(socket: zmq::Socket) -> crate::Result<Self> {
        Ok(Self {
            inner: SenderReceiver::new(ZmqPoller::from_zmq_socket(socket)?),
        })
    }
}

impl_wrapper!(Xsub, SenderReceiver, inner);
impl_wrapper_sink!(Xsub, inner);
impl_wrapper_stream!(Xsub, inner);

impl Xsub {
    /// Send a subscription message for the given topic.
    ///
    /// This is similar to SUB's subscribe method but instead of using socket options,
    /// XSUB sends a message upstream with a special format: first byte is 1 (subscription flag)
    /// followed by the subscription topic.
    pub async fn subscribe(&mut self, topic: &[u8]) -> Result<()> {
        let mut msg = Vec::new();
        msg.push(1); // 1 = subscribe
        msg.extend_from_slice(topic);
        use futures::SinkExt;
        self.send(vec![msg]).await
    }
    
    /// Send an unsubscription message for the given topic.
    ///
    /// This is similar to SUB's unsubscribe method but instead of using socket options,
    /// XSUB sends a message upstream with a special format: first byte is 0 (unsubscription flag)
    /// followed by the subscription topic.
    pub async fn unsubscribe(&mut self, topic: &[u8]) -> Result<()> {
        let mut msg = Vec::new();
        msg.push(0); // 0 = unsubscribe
        msg.extend_from_slice(topic);
        use futures::SinkExt;
        self.send(vec![msg]).await
    }
}