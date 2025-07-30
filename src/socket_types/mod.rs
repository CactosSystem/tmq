/// Dealer Sockets
pub mod dealer;
/// Pair Sockets
pub mod pair;
/// Publish Sockets
pub mod publish;
/// Pull Sockets
pub mod pull;
/// Push Sockets
pub mod push;
/// Request/Reply Sockets
pub mod request_reply;
/// Router Sockets
pub mod router;
/// Subscribe Sockets
pub mod subscribe;
/// XPUB Sockets
pub mod xpub;
/// XSUB Sockets
pub mod xsub;

pub use dealer::dealer;
pub use pair::pair;
pub use publish::publish;
pub use pull::pull;
pub use push::push;
pub use request_reply::reply;
pub use request_reply::request;
pub use router::router;
pub use subscribe::subscribe;
pub use xpub::xpub;
pub use xsub::xsub;

#[doc(hidden)]
pub trait FromZmqSocket<T> {
    fn from_zmq_socket(socket: zmq::Socket) -> crate::Result<T>;
}
