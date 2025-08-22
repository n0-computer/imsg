//! imsg - A base protocol providing streams of messages.
//!
//! A wrapper around iroh connections giving you a simple API to send and receive messages.
//! If you care about round trips, bytes and high performance then use the underlying QUIC
//! API directly.  If you want to have an easy time building your own protocol on top of
//! messages, use this.
//!
//! # Bring your own ALPN
//!
//! This protocol does not have an ALPN.  You should build your own protocol on top and give
//! that an ALPN.

use std::sync::Arc;

use actor::{ConnectionActor, ConnectionActorMessage};
use anyhow::{Context, Result, anyhow, ensure};
use bytes::{Bytes, BytesMut};
use iroh::endpoint::{ConnectionError, RecvStream, SendStream, WriteError};
use n0_future::task::AbortOnDropHandle;
use n0_future::{SinkExt, StreamExt, future};
use proto::{ConnectionErrorCode, ImsgCodec, ImsgCodecError, StreamErrorCode, StreamType};
use tokio::sync::{Mutex, Notify, mpsc, oneshot};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{Instrument, error, info_span, instrument, trace};

mod actor;
mod proto;
mod varint;

/// A connection to a peer.
///
/// Takes ownership of the wrapped connection, do not try and use it directly anymore.
#[derive(Debug, Clone)]
pub struct Connection {
    conn: iroh::endpoint::Connection,
    actor_addr: mpsc::Sender<actor::ConnectionActorMessage>,
    _actor: Arc<AbortOnDropHandle<()>>,
}

impl Connection {
    /// Creates a new connection from an existing iroh connection.
    pub async fn new(conn: iroh::endpoint::Connection) -> Result<Self> {
        // First open the control stream.
        let (mut ctrl_send, mut ctrl_recv) = conn.open_bi().await?;
        let side = Side::from(ctrl_send.id());
        if matches!(side, Side::Server) {
            // The server should not open this stream, close it without sending anything.
            ctrl_send.reset(StreamErrorCode::ControlStreamNotNeeded.into())?;
            ctrl_recv.stop(StreamErrorCode::ControlStreamNotNeeded.into())?;
            (ctrl_send, ctrl_recv) = conn.accept_bi().await?;
        }

        let mut ctrl_writer = FramedWrite::new(ctrl_send, ImsgCodec);
        let mut ctrl_reader = FramedRead::new(ctrl_recv, ImsgCodec);

        // Read or send the StreamType message that opens the control stream.
        match side {
            Side::Server => {
                // Read the StreamType.
                let mut msg = ctrl_reader
                    .next()
                    .await
                    .ok_or(anyhow!("stream type message missing"))??;
                let stream_type = StreamType::decode(&mut msg)?;
                ensure!(
                    msg.is_empty(),
                    "StreamType message contained too many bytes"
                );
                ensure!(
                    matches!(stream_type, StreamType::Control),
                    "expected control stream"
                );
                trace!(StreamType = ?stream_type, "connection accepted");
            }
            Side::Client => {
                // Send the StreamType.
                let mut buf = BytesMut::with_capacity(8);
                StreamType::Control.encode(&mut buf);
                ctrl_writer.send(buf.freeze()).await?;
                trace!(StreamType = ?StreamType::Control, "connection started");
            }
        }

        // Start the actor.
        let mut actor = ConnectionActor::new(conn.clone(), side, ctrl_writer, ctrl_reader);
        let (inbox_tx, inbox_rx) = mpsc::channel(32);
        let task = tokio::spawn(async move { actor.run(inbox_rx).await }.instrument(
            info_span!("imsg-conn-actor", remote = %conn.remote_node_id().unwrap().fmt_short()),
        ));

        let conn = Self {
            conn,
            _actor: Arc::new(AbortOnDropHandle::new(task)),
            actor_addr: inbox_tx,
        };

        Ok(conn)
    }

    /// Opens a stream.
    ///
    /// Streams are cheap to open and many can be opened.  An opened stream will be
    /// immediately visible to the peer and either peer can send the first message.  It is
    /// possible to start sending messages on a stream before the peer has called
    /// [`accept_stream`].
    ///
    /// [`accept_stream`]: Connection::accept_stream
    pub async fn open_stream(&self) -> Result<Stream> {
        let (send, recv) = self.conn.open_bi().await?;
        let mut writer = FramedWrite::new(send, ImsgCodec);
        let reader = FramedRead::new(recv, ImsgCodec);

        // Send the StreamType to open the stream.
        let mut buf = BytesMut::with_capacity(8);
        StreamType::User.encode(&mut buf);
        writer.send(buf.freeze()).await?;
        trace!(StreamType=?StreamType::User, "opened stream");

        // Send the abort hook to the connection actor.
        let notify = Arc::new(Notify::new());
        self.actor_addr
            .send(ConnectionActorMessage::NewStream(notify.clone()))
            .await?;

        Ok(Stream {
            inner: Arc::new(Mutex::new(StreamInner {
                state: StreamState::Open,
                writer,
                reader,
                abort_requested: notify,
            })),
        })
    }

    /// Accepts an stream opened by the peer.
    ///
    /// All streams opened must also be accepted by the peer.
    pub async fn accept_stream(&self) -> Result<Stream> {
        let (send, recv) = self.conn.accept_bi().await?;
        let writer = FramedWrite::new(send, ImsgCodec);
        let mut reader = FramedRead::new(recv, ImsgCodec);

        // Read the StreamType frame.
        let mut msg = reader
            .next()
            .await
            .ok_or(anyhow!("missing StreamType message"))??;
        let stream_type = StreamType::decode(&mut msg)?;
        ensure!(
            msg.is_empty(),
            "StreamType message contained too many bytes"
        );
        ensure!(
            matches!(stream_type, StreamType::User),
            "expected user stream"
        );
        trace!(StreamType = ?stream_type, "accepted stream");

        // Send the abort hook to the connection actor.
        let notify = Arc::new(Notify::new());
        self.actor_addr
            .send(ConnectionActorMessage::NewStream(notify.clone()))
            .await?;

        Ok(Stream {
            inner: Arc::new(Mutex::new(StreamInner {
                state: StreamState::Open,
                writer,
                reader,
                abort_requested: notify,
            })),
        })
    }

    /// Closes a connection.
    ///
    /// To close the connection the peer must also call [`Connection::close`].  **If it does
    /// not, this will block indefinitely.** The messages of any already closed *streams*
    /// will still be able to be read by the peer before the connection is fully closed.
    ///
    /// Any streams still open when this is called will be aborted immediately.
    // TODO: I worry that indefinitely hanging is too big a footgun.
    pub async fn close(&self) -> Result<()> {
        if let Some(conn_err) = self.conn.close_reason() {
            return match conn_err {
                ConnectionError::ApplicationClosed(application_close)
                    if application_close.error_code == ConnectionErrorCode::Closed.into() =>
                {
                    Ok(())
                }
                // TODO: handle remote aborted here with an Aborted error variant
                _ => Err(anyhow!("connection error: {conn_err:?}")),
            };
        }

        let (tx, rx) = oneshot::channel();
        self.actor_addr
            .send(ConnectionActorMessage::Close(tx))
            .await
            .context("hello actor inbox")?;
        rx.await.context("TODO: ConnectionError::Aborted")?;
        Ok(())
    }

    /// Aborts the connection.
    ///
    /// Any open streams are aborted as per the semantics of [`Stream::abort`], and the
    /// connection is closed.  This may lose already sent messages.  This equivalent to
    /// dropping the [`Connection`].
    pub async fn abort(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.actor_addr
            .send(actor::ConnectionActorMessage::Abort(tx))
            .await?;
        rx.await?;
        Ok(())
    }
}

/// A stream allows sending messages to, and receiving messages from the peer.
///
/// In order not to lose any messages you should always [`close`] a stream.  If the last
/// clone of a stream is dropped it is [`aborted`] and messages might be lost.
///
/// [`close`]: Stream::close
/// [`aborted`]: Stream::abort
#[derive(Debug, Clone)]
pub struct Stream {
    inner: Arc<Mutex<StreamInner>>,
}

#[derive(Debug)]
struct StreamInner {
    /// State of our stream.
    state: StreamState,
    /// Sink allowing to send frames.
    writer: FramedWrite<SendStream, ImsgCodec>,
    /// Stream allowing to receive frames.
    reader: FramedRead<RecvStream, ImsgCodec>,
    /// Used by the [`ConnectionActor`] to abort this stream.
    abort_requested: Arc<Notify>,
}

impl StreamInner {
    /// Aborts the stream.
    ///
    /// Can be safely called whatever the state of the stream, does nothing if the stream is
    /// already closed.
    fn abort(&mut self) {
        self.writer
            .get_mut()
            .reset(StreamErrorCode::Aborted.into())
            .ok();
        self.reader
            .get_mut()
            .stop(StreamErrorCode::Aborted.into())
            .ok();
        self.state = StreamState::Aborted
    }
}

impl Drop for StreamInner {
    fn drop(&mut self) {
        match self.state {
            StreamState::Open => self.abort(),
            StreamState::Closed => (),
            StreamState::Aborted => (),
        }
    }
}

impl Stream {
    /// Sends a message to the peer.
    #[instrument(skip_all)]
    pub async fn send_msg(&self, msg: Bytes) -> Result<(), StreamError> {
        let mut inner = self.inner.lock().await;
        match inner.state {
            StreamState::Open => (),
            StreamState::Closed => return Err(StreamError::Closed),
            StreamState::Aborted => return Err(StreamError::Aborted),
        }

        // Check if the connection asked us to abort the stream.  This happens to any
        // streams still open when Connection::close is called.
        if future::poll_once(inner.abort_requested.notified())
            .await
            .is_some()
        {
            inner.state = StreamState::Aborted;
            inner.abort();
            return Err(StreamError::Aborted);
        }

        match inner.writer.send(msg).await {
            Ok(()) => Ok(()),
            Err(err) => {
                trace!(?err, "stream write error");

                // Handle a remotely closed stream.  Stream::close() stops their RecvStream
                // (our SendStream) and finishes their SendStream (our RecvStream).
                if let ImsgCodecError::IoError(io_err) = err {
                    if let Ok(WriteError::Stopped(code)) = io_err.downcast() {
                        if let Ok(code) = StreamErrorCode::try_from(code) {
                            trace!(StreamErrorCode=?code, "write stream stopped");
                            if let StreamErrorCode::Closed = code {
                                return Err(StreamError::RemoteClosed);
                            }
                        }
                    }
                }

                // Everything else results in Aborted.
                Err(StreamError::Aborted)
            }
        }
    }

    /// Receives a message from the peer.
    #[instrument(skip_all)]
    pub async fn recv_msg(&self) -> Result<Bytes, StreamError> {
        let mut inner = self.inner.lock().await;
        match inner.state {
            StreamState::Open => (),
            StreamState::Closed => return Err(StreamError::Closed),
            StreamState::Aborted => return Err(StreamError::Aborted),
        }

        // Check if the connection asked us to abort the stream.  This happens to any
        // streams still open when Connection::close is called.
        if future::poll_once(inner.abort_requested.notified())
            .await
            .is_some()
        {
            inner.state = StreamState::Aborted;
            inner.abort();
            return Err(StreamError::Aborted);
        }

        match inner.reader.next().await {
            Some(Ok(msg)) => Ok(msg),
            None => Err(StreamError::EndOfStream),
            Some(Err(err)) => {
                trace!(?err, "stream read error");
                inner.abort();
                Err(StreamError::Aborted)
            }
        }
    }

    /// Closes the stream, already sent messages can still be received.
    ///
    /// Sending or receiving messages on a closed stream will fail immediately.  Thus any
    /// messages already sent by the peer which have not yet been received will be lost.
    /// Usually this is called if no more messages are expected to be received from the
    /// peer, or if those messages are no longer required.  The peer can still receive
    /// already sent messages if it has not yet closed the stream itself.
    ///
    /// Once closed, it is safe to drop the stream.
    pub async fn close(&self) {
        let mut inner = self.inner.lock().await;
        inner.state = StreamState::Closed;
        // If the peer already closed it would have stopped our SendStream and finished our
        // RecvStrea ClosedStream error.  That's expected.
        inner
            .writer
            .get_mut()
            .finish()
            .context("finishing SendStream")
            .ok();
        inner
            .reader
            .get_mut()
            .stop(StreamErrorCode::Closed.into())
            .ok();
    }

    /// Aborts the stream, messages might be lost.
    ///
    /// Sending and receiving messages will fail immediately.  This is the equivalent of
    /// dropping the stream.
    pub async fn abort(&self) {
        self.inner.lock().await.abort()
    }
}

#[derive(Debug, Copy, Clone)]
enum StreamState {
    /// Open, maybe remote closed.
    ///
    /// When the remote closes it finishes it's [`SendStream`].  The local state will still
    /// be open as we can still any so far unread messages from the local [`RecvStream`].
    Open,
    Closed,
    Aborted,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StreamError {
    /// The stream end is reached, all data was received.
    ///
    /// Further receiving messages will keep returning this error until [`Stream::close`]
    /// is called locally, when the [`Closed`] error will be returned on receive calls.
    ///
    /// [`Closed`]: StreamError::Closed
    #[error("end of stream")]
    EndOfStream,
    /// The peer has closed the stream.
    ///
    /// Any messages already sent by the peer can still be received.  Sending messages is no
    /// longer possible.  This error is only returned when sending a message, when receiving
    /// messages continue to be returned until [`EndOfStream`] is returned.
    ///
    /// [`EndOfStream`]: StreamError::EndOfStream
    #[error("remote closed stream")]
    RemoteClosed,
    /// The stream is closed.
    ///
    /// Neither sending or receiving messages is possible.
    #[error("stream closed")]
    Closed,
    /// The stream is aborted.
    ///
    /// Neither sending or receiving messages is possible.  Data may have been lost if a
    /// stream was aborted before the [`EndOfStream`] was reached.
    ///
    /// [`EndOfStream`]: StreamError::EndOfStream
    // Implementation note: an imsg protocol error is surfaced to the user as an Aborted
    // stream.  To debug this you'll need to
    #[error("stream aborted")]
    Aborted,
}

// impl From<ImsgCodecError> for StreamError {
//     fn from(err: ImsgCodecError) -> Self {
//         Self::Aborted
//     }
// }

/// What side of a connection we are.
#[derive(Debug, Copy, Clone)]
enum Side {
    /// This is the server.
    Server,
    /// This is the client.
    Client,
}

impl Side {
    fn is_server(&self) -> bool {
        matches!(self, Side::Server)
    }

    fn is_client(&self) -> bool {
        matches!(self, Side::Client)
    }
}

impl From<iroh::endpoint::StreamId> for Side {
    fn from(source: iroh::endpoint::StreamId) -> Self {
        if source.initiator().is_server() {
            Self::Server
        } else {
            Self::Client
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use assert2::{assert, check};
    use iroh::{Endpoint, Watcher};
    use testresult::TestResult;
    use tracing::{Instrument, info_span};
    use tracing_test::traced_test;

    use super::*;

    const ALPN: &[u8] = b"n0mq";

    // #[derive(Debug)]
    // struct Pair {
    //     alice_fut: Box<dyn Fn(Connection) -> impl Future<Output = Result<()>>>,
    //     bob_fut: Box<dyn Future<Output = Result<()>>>,
    // }

    // impl Pair {
    //     fn new() -> Self {
    //         Self {
    //             alice_fut: future::ready(Ok(())).boxed_local(),
    //             bob_fut: future::ready(Ok(())).boxed_local(),
    //         }
    //     }

    //     fn with_alice<F, Fut>(&mut self, handler: F) -> Self
    //     where
    //         F: Fn(Connection) -> Fut,
    //         Fut: Future<Output = Result<()>>,
    //     {
    //         todo!()
    //     }
    // }

    // #[tokio::test]
    // async fn test_test() -> TestResult {
    //     let pair = Pair::new()
    //         .with_alice(|conn| async move {
    //             let stream = conn.accept_stream().await?;
    //             let msg = stream.recv_msg().await?;
    //             stream.send_msg(msg).await?;
    //             stream.close().await?;
    //             conn.close().await?;
    //             Ok(())
    //         })
    //         .with_bob(|conn| async move {
    //             let stream = conn.open_stream().await?;
    //             stream.send_msg(b"hello".as_ref().into()).await?;
    //             let msg = stream.recv_msg().await?;
    //             assert_eq!(&msg, b"hello".as_ref());
    //             stream.close().await?;
    //             conn.close().await?;
    //             Ok(())
    //         });
    //     pair.run().await?;
    //     Ok(())
    // }

    #[tokio::test]
    #[traced_test]
    async fn test_basic_echo() -> TestResult {
        let alice = Endpoint::builder()
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await?;
        let alice_addr = alice.node_addr().initialized().await;

        let alice_fut = async move {
            let conn = alice.accept().await.ok_or(anyhow!("no conn"))?.await?;

            let conn = Connection::new(conn).await?;
            let stream = conn.accept_stream().await?;
            let msg = stream.recv_msg().await?;
            stream.send_msg(msg).await?;
            stream.close().await;
            conn.close().await?;

            Ok::<_, testresult::TestError>(())
        }
        .instrument(info_span!("alice"));
        let bob_fut = async move {
            let bob = Endpoint::builder().bind().await?;
            let conn = bob.connect(alice_addr, ALPN).await?;

            let conn = Connection::new(conn).await?;
            let stream = conn.open_stream().await?;
            stream.send_msg(b"hello".as_ref().into()).await?;
            let msg = stream.recv_msg().await?;
            check!(&msg == b"hello".as_ref());
            stream.close().await;
            conn.close().await?;

            Ok::<_, testresult::TestError>(())
        }
        .instrument(info_span!("bob"));

        let (alice_ret, bob_ret) = tokio::join!(alice_fut, bob_fut);
        alice_ret?;
        bob_ret?;

        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_stream_not_closed() -> TestResult {
        let alice = Endpoint::builder()
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await?;
        let alice_addr = alice.node_addr().initialized().await;

        let alice_fut = async move {
            let conn = alice.accept().await.ok_or(anyhow!("no conn"))?.await?;

            let conn = Connection::new(conn).await?;
            let stream = conn.accept_stream().await?;
            let msg = stream.recv_msg().await?;
            stream.send_msg(msg).await?;
            // Not closing the stream!
            conn.close().await?;

            Ok::<_, testresult::TestError>(())
        }
        .instrument(info_span!("alice"));
        let bob_fut = async move {
            let bob = Endpoint::builder().bind().await?;
            let conn = bob.connect(alice_addr, ALPN).await?;

            let conn = Connection::new(conn).await?;
            let stream = conn.open_stream().await?;
            stream.send_msg(b"hello".as_ref().into()).await?;
            let msg = stream.recv_msg().await?;
            check!(&msg == b"hello".as_ref());
            // Not closing the stream!
            conn.close().await?;

            Ok::<_, testresult::TestError>(())
        }
        .instrument(info_span!("bob"));

        let (alice_ret, bob_ret) = tokio::join!(alice_fut, bob_fut);
        alice_ret?;
        bob_ret?;

        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_conn_dropped() -> TestResult {
        let alice = Endpoint::builder()
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await?;
        let alice_addr = alice.node_addr().initialized().await;

        let alice_fut = async move {
            let conn = alice.accept().await.ok_or(anyhow!("no conn"))?.await?;

            let conn = Connection::new(conn).await?;
            let stream = conn.accept_stream().await?;
            let msg = stream.recv_msg().await?;
            stream.send_msg(msg).await?;
            stream.close().await;
            // Dropping the connection without closing.  No messages lost because we awaited
            // the stream.  A warning is logged.

            Ok::<_, testresult::TestError>(())
        }
        .instrument(info_span!("alice"));
        let bob_fut = async move {
            let bob = Endpoint::builder().bind().await?;
            let conn = bob.connect(alice_addr, ALPN).await?;

            let conn = Connection::new(conn).await?;
            let stream = conn.open_stream().await?;
            stream.send_msg(b"hello".as_ref().into()).await?;
            let msg = stream.recv_msg().await?;
            check!(&msg == b"hello".as_ref());
            stream.close().await;
            // Dropping the connection without closing.  No message lost because we awaited
            // the stream.  A warning is logged.

            Ok::<_, testresult::TestError>(())
        }
        .instrument(info_span!("bob"));

        let (alice_ret, bob_ret) = tokio::join!(alice_fut, bob_fut);
        alice_ret?;
        bob_ret?;

        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_conn_dropped_msg_loss() -> TestResult {
        let alice = Endpoint::builder()
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await?;
        let alice_addr = alice.node_addr().initialized().await;

        let alice_fut = async move {
            let conn = alice.accept().await.ok_or(anyhow!("no conn"))?.await?;

            let conn = Connection::new(conn).await?;
            let stream = conn.accept_stream().await?;
            let msg = stream.recv_msg().await?;
            stream.send_msg(msg).await?;
            // Not closing the stream, dropping the connection results in an error message.

            Ok::<_, testresult::TestError>(())
        }
        .instrument(info_span!("alice"));
        let bob_fut = async move {
            let bob = Endpoint::builder().bind().await?;
            let conn = bob.connect(alice_addr, ALPN).await?;

            let conn = Connection::new(conn).await?;
            let stream = conn.open_stream().await?;
            stream.send_msg(b"hello".as_ref().into()).await?;
            let msg = stream.recv_msg().await?;
            check!(&msg == b"hello".as_ref());
            // Not closing the stream, dropping the connection results in an error message.

            Ok::<_, testresult::TestError>(())
        }
        .instrument(info_span!("bob"));

        let (alice_ret, bob_ret) = tokio::join!(alice_fut, bob_fut);
        alice_ret?;
        bob_ret?;

        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_accept_stream_first_message() -> TestResult {
        let alice = Endpoint::builder()
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await?;
        let alice_addr = alice.node_addr().initialized().await;

        let alice_fut = async move {
            let conn = alice.accept().await.ok_or(anyhow!("no conn"))?.await?;

            let conn = Connection::new(conn).await?;
            let stream = conn.accept_stream().await?;
            stream.send_msg(b"hello".as_ref().into()).await?;
            conn.close().await?;

            Ok::<_, testresult::TestError>(())
        }
        .instrument(info_span!("alice"));
        let bob_fut = async move {
            let bob = Endpoint::builder().bind().await?;
            let conn = bob.connect(alice_addr, ALPN).await?;

            let conn = Connection::new(conn).await?;
            let stream = conn.open_stream().await?;
            let msg = stream.recv_msg().await?;
            check!(&msg == b"hello".as_ref());
            conn.close().await?;

            Ok::<_, testresult::TestError>(())
        }
        .instrument(info_span!("bob"));

        let (alice_ret, bob_ret) = tokio::join!(alice_fut, bob_fut);
        alice_ret?;
        bob_ret?;

        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_close_stream() -> TestResult {
        let alice = Endpoint::builder()
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await?;
        let alice_addr = alice.node_addr().initialized().await;

        let alice_fut = async move {
            let conn = alice.accept().await.ok_or(anyhow!("no conn"))?.await?;

            let conn = Connection::new(conn).await?;
            let stream = conn.accept_stream().await?;
            stream.send_msg(b"hello".as_ref().into()).await?;
            stream.close().await;

            // Reading now results in a Closed error
            let res = stream.recv_msg().await;
            assert!(let Err(StreamError::Closed) = res);

            // Sending now results in a closed error
            let res = stream.send_msg(b"hello".as_ref().into()).await;
            assert!(let Err(StreamError::Closed) = res);

            // For implementation reasons, check this result is consistent.
            let res = stream.send_msg(b"hello".as_ref().into()).await;
            assert!(let Err(StreamError::Closed) = res);

            conn.close().await?;

            Ok::<_, testresult::TestError>(())
        }
        .instrument(info_span!("alice"));
        let bob_fut = async move {
            let bob = Endpoint::builder().bind().await?;
            let conn = bob.connect(alice_addr, ALPN).await?;

            let conn = Connection::new(conn).await?;
            let stream = conn.open_stream().await?;

            // First read a message
            let msg = stream.recv_msg().await?;
            assert!(&msg == b"hello".as_ref());

            // Now read end-of-stream
            let res = stream.recv_msg().await;
            assert!(let Err(StreamError::EndOfStream) = res);
            let res = stream.recv_msg().await;
            assert!(let Err(StreamError::EndOfStream) = res);

            // Sending does not work since remote has closed.
            let res = stream.send_msg(b"hello".as_ref().into()).await;
            assert!(let Err(StreamError::RemoteClosed) = res);

            stream.close().await;
            conn.close().await?;

            Ok::<_, testresult::TestError>(())
        }
        .instrument(info_span!("bob"));

        let (alice_ret, bob_ret) = tokio::join!(alice_fut, bob_fut);
        alice_ret?;
        bob_ret?;

        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_close_multiple_streams() -> TestResult {
        let alice = Endpoint::builder()
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await?;
        let alice_addr = alice.node_addr().initialized().await;

        let alice_fut = async move {
            let conn = alice.accept().await.ok_or(anyhow!("no conn"))?.await?;

            let conn = Connection::new(conn).await?;
            let stream0 = conn.accept_stream().await?;
            stream0.send_msg(b"hello 0".as_ref().into()).await?;
            let stream1 = conn.accept_stream().await?;
            stream1.send_msg(b"hello 1".as_ref().into()).await?;
            let stream2 = conn.accept_stream().await?;
            stream2.send_msg(b"hello 2".as_ref().into()).await?;

            conn.close().await?;

            Ok::<_, testresult::TestError>(())
        }
        .instrument(info_span!("alice"));
        let bob_fut = async move {
            let bob = Endpoint::builder().bind().await?;
            let conn = bob.connect(alice_addr, ALPN).await?;

            let conn = Connection::new(conn).await?;
            let stream0 = conn.open_stream().await?;
            let stream1 = conn.open_stream().await?;
            let stream2 = conn.open_stream().await?;

            // Read last stream first
            let msg = stream2.recv_msg().await?;
            assert!(&msg == b"hello 2".as_ref());

            // Now we can't send on other streams
            let res = stream0.send_msg(b"msg".as_ref().into()).await;
            assert!(let Err(StreamError::RemoteClosed) = res);
            let res = stream1.send_msg(b"msg".as_ref().into()).await;
            assert!(let Err(StreamError::RemoteClosed) = res);

            // Drain stream0
            let msg = stream0.recv_msg().await?;
            assert!(&msg == b"hello 0".as_ref());
            let res = stream0.recv_msg().await;
            check!(let Err(StreamError::EndOfStream) = res);

            // Close without draining stream1
            conn.close().await?;

            // Stream1 is now closed
            let res = stream1.recv_msg().await;
            check!(let Err(StreamError::Closed) = res);

            Ok::<_, testresult::TestError>(())
        }
        .instrument(info_span!("bob"));

        let (alice_ret, bob_ret) = tokio::join!(alice_fut, bob_fut);
        alice_ret?;
        bob_ret?;

        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_abort() -> TestResult {
        let alice = Endpoint::builder()
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await?;
        let alice_addr = alice.node_addr().initialized().await;

        let alice_fut = async move {
            let conn = alice.accept().await.ok_or(anyhow!("no conn"))?.await?;

            let conn = Connection::new(conn).await?;
            let stream = conn.accept_stream().await?;

            conn.abort().await?;

            // Sending and receiving from the stream should result in errors.
            let res = stream.send_msg(b"hello".as_ref().into()).await;
            check!(let Err(StreamError::Aborted) = res);
            let res = stream.send_msg(b"hello".as_ref().into()).await;
            check!(let Err(StreamError::Aborted) = res);

            let res = stream.recv_msg().await;
            check!(let Err(StreamError::Aborted) = res);
            let res = stream.recv_msg().await;
            check!(let Err(StreamError::Aborted) = res);

            Ok::<_, testresult::TestError>(())
        }
        .instrument(info_span!("alice"));
        let bob_fut = async move {
            let bob = Endpoint::builder().bind().await?;
            let conn = bob.connect(alice_addr, ALPN).await?;

            let conn = Connection::new(conn).await?;
            let stream = conn.open_stream().await?;

            // This may or may not succeed, depending on timing.
            match stream.send_msg(b"hello".as_ref().into()).await {
                Ok(()) => (),
                Err(StreamError::Aborted) => (),
                Err(err) => panic!("wrong error: {err:?}"),
            }

            // Remote aborted the connection.
            let res = stream.recv_msg().await;
            check!(let Err(StreamError::Aborted) = res);
            let res = stream.recv_msg().await;
            check!(let Err(StreamError::Aborted) = res);

            let res = stream.send_msg(b"hello".as_ref().into()).await;
            check!(let Err(StreamError::Aborted) = res);
            let res = stream.send_msg(b"hello".as_ref().into()).await;
            check!(matches!(res, Err(StreamError::Aborted)));

            Ok::<_, testresult::TestError>(())
        }
        .instrument(info_span!("bob"));

        let (alice_ret, bob_ret) = tokio::join!(alice_fut, bob_fut);
        alice_ret?;
        bob_ret?;

        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_abort_stream_clone() -> TestResult {
        let alice = Endpoint::builder()
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await?;
        let alice_addr = alice.node_addr().initialized().await;

        let alice_fut = async move {
            let conn = alice.accept().await.ok_or(anyhow!("no conn"))?.await?;

            let conn = Connection::new(conn).await?;
            let stream0 = conn.accept_stream().await?;
            let stream1 = stream0.clone();

            conn.abort().await?;

            let res = stream0.send_msg(b"hello".as_ref().into()).await;
            check!(let Err(StreamError::Aborted) = res);
            let res = stream1.send_msg(b"hello".as_ref().into()).await;
            check!(matches!(res, Err(StreamError::Aborted)));

            Ok::<_, testresult::TestError>(())
        }
        .instrument(info_span!("alice"));
        let bob_fut = async move {
            let bob = Endpoint::builder().bind().await?;
            let conn = bob.connect(alice_addr, ALPN).await?;

            let conn = Connection::new(conn).await?;

            let res = conn.open_stream().await;
            check!(res.is_err());

            Ok::<_, testresult::TestError>(())
        }
        .instrument(info_span!("bob"));

        let (alice_ret, bob_ret) = tokio::join!(alice_fut, bob_fut);
        alice_ret?;
        bob_ret?;

        Ok(())
    }
}
