use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use iroh::endpoint::{ConnectionError, ReadError, RecvStream, SendStream};
use n0_future::{SinkExt, StreamExt};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{error, instrument, trace};

use crate::proto::{ConnectionErrorCode, ControlFrame, ImsgCodec};
use crate::{ImsgCodecError, StreamErrorCode};

/// Actor monitoring a connection.
#[derive(Debug)]
pub(super) struct ConnectionActor {
    /// The iroh/QUIC connection.
    connection: iroh::endpoint::Connection,
    /// Whether we are the server or the client.
    side: super::Side,
    ctrl_writer: FramedWrite<SendStream, ImsgCodec>,
    ctrl_reader: FramedRead<RecvStream, ImsgCodec>,
    /// Notifier to abort all streams.
    ///
    /// Each stream adds itself to be notified here on creation, when a stream needs to be
    /// aborted the actor will use this notifier.  If the [`Arc::strong_count`] drops to 1
    /// the stream is closed.
    // TODO: Can this use a weak reference instead?
    streams: Vec<Arc<Notify>>,
    /// Whether the remote has closed this connection.
    ///
    /// This connection/actor now awaits for the user to close the connection locally:
    ///
    /// - If we are a client, we can close the connection immediately when the connection
    /// - gets closed locally.
    /// - If we are the server, we have to send the [`ControlFrame::ConnectionClose`]
    ///   message when the connection gets closed locally.
    remote_closed: bool,
    /// Whether the connection is closed locally.
    ///
    /// - The client is now only waiting for the server to close the connection.
    /// - The server is now waiting for the [`ControlFrame::ConnectionClose`] message from
    ///   the client.
    local_closed: bool,
    /// Waiters to wake up [`Connection::close`] calls.
    close_waiters: Vec<oneshot::Sender<()>>,
}

impl ConnectionActor {
    pub(super) fn new(
        connection: iroh::endpoint::Connection,
        side: super::Side,
        ctrl_writer: FramedWrite<SendStream, ImsgCodec>,
        ctrl_reader: FramedRead<RecvStream, ImsgCodec>,
    ) -> Self {
        Self {
            connection,
            side,
            ctrl_writer,
            ctrl_reader,
            streams: Vec::new(),
            remote_closed: false,
            local_closed: false,
            close_waiters: Vec::new(),
        }
    }

    pub(super) async fn run(&mut self, mut inbox: mpsc::Receiver<ConnectionActorMessage>) {
        let conn_close_code = loop {
            tokio::select! {
                biased;
                msg = inbox.recv() => {
                    let Some(msg) = msg else {
                        // Inbox closed, all connection objects dropped.
                        break ConnectionErrorCode::Closed;
                    };
                    if let Some(frame) = self.handle_inbox(msg) {
                        trace!(?frame, "sending control msg");
                        let mut buf = BytesMut::with_capacity(8);
                        frame.encode(&mut buf);
                        // TODO: no await
                        self.ctrl_writer
                            .send(buf.freeze())
                            .await
                            .expect("TODO: ctrl stream send failed");
                    }
                }
                msg = self.ctrl_reader.next() => {
                    match msg {
                        Some(Ok(msg)) => self.handle_ctrl_frame(msg),
                        Some(Err(err)) => break self.handle_ctrl_read_err(err),
                        None => break ConnectionErrorCode::Closed,
                    }
                }
                // We could also wait on self.connection.closed() here but we already do
                // this by reading from the control stream, which receives the same
                // ConnectionError on a connection close.
            }
        };

        // No need to explicitly abort the streams, closing the connection will do that.  We
        // also never finish the control stream.
        self.connection
            .close(conn_close_code.into(), b"actor finished");
        trace!(ConnectionErrorCode = ?conn_close_code, "actor finished");
    }

    /// Handles an actor message.
    ///
    /// Optionally returns a message to be sent on the control stream.
    fn handle_inbox(&mut self, msg: ConnectionActorMessage) -> Option<ControlFrame> {
        // trace!(?msg, "actor message");
        match msg {
            ConnectionActorMessage::NewStream(notify) => {
                // Clean up any closed streams first.
                self.streams.retain(|n| Arc::strong_count(n) > 1);
                self.streams.push(notify);
                None
            }
            ConnectionActorMessage::Close(tx) => {
                trace!(?self.local_closed, ?self.remote_closed, "starting connection close");
                // Store the waker for this close call.
                self.close_waiters.push(tx);

                // Abort all the streams.  If a stream was already closed but not yet
                // dropped it will simply ignore this signal.
                for notify in self.streams.drain(..) {
                    notify.notify_one();
                }

                self.local_closed = true;
                if self.remote_closed {
                    // Close the connection
                    match self.side {
                        crate::Side::Server => {
                            trace!("closing connection");
                            self.connection
                                .close(ConnectionErrorCode::Closed.into(), b"closed");
                            for tx in self.close_waiters.drain(..) {
                                tx.send(()).ok();
                            }
                            None
                        }
                        crate::Side::Client => Some(ControlFrame::ConnectionClose),
                    }
                } else {
                    match self.side {
                        crate::Side::Server => Some(ControlFrame::RequestConnectionClose),
                        crate::Side::Client => Some(ControlFrame::ConnectionClose),
                    }
                }
            }
            ConnectionActorMessage::Abort(tx) => {
                for notify in self.streams.drain(..) {
                    notify.notify_one();
                }
                tx.send(()).ok();
                trace!("aborting connection");
                self.connection
                    .close(ConnectionErrorCode::Aborted.into(), b"aborted");
                self.close_waiters.drain(..);
                None
            }
        }
    }

    fn handle_ctrl_frame(&mut self, mut msg: Bytes) {
        // TODO: close conn with protocol violation on invalid frame
        let frame = ControlFrame::decode(&mut msg).expect("TODO");
        assert!(msg.is_empty(), "TODO: invalid control message");
        trace!(?frame, ?self.local_closed, ?self.remote_closed, "handling control frame");
        match frame {
            ControlFrame::RequestConnectionClose => {
                assert!(
                    self.side.is_client(),
                    "TODO: client can not send RequestConnectionClose"
                );
                self.remote_closed = true;
            }
            ControlFrame::ConnectionClose => {
                assert!(
                    self.side.is_server(),
                    "TODO: server can not send ConnectionClose"
                );
                self.remote_closed = true;
                if self.local_closed {
                    trace!("closing connection");
                    self.connection
                        .close(ConnectionErrorCode::Closed.into(), b"closed");
                    for notify in self.close_waiters.drain(..) {
                        notify.send(()).ok();
                    }
                }
            }
        }
    }

    /// Handles a read error from the control stream.
    ///
    /// We always close the connection after a read error.  This is responsible for figuring
    /// out the error code and log the read error at an appropriate level.
    #[instrument(skip(self))]
    fn handle_ctrl_read_err(&mut self, err: ImsgCodecError) -> ConnectionErrorCode {
        let mut stream_error = None;
        let mut conn_error = None;

        let close_code = match err {
            ImsgCodecError::CodecError => ConnectionErrorCode::ProtocolError,
            ImsgCodecError::IoError(error) => match error.downcast::<ReadError>() {
                Ok(ReadError::Reset(code)) => {
                    match StreamErrorCode::try_from(code) {
                        Ok(code) => stream_error = Some(code),
                        Err(code) => error!(%code, "invalid StreamErrorCode"),
                    }
                    ConnectionErrorCode::ProtocolError
                }
                Ok(ReadError::ConnectionLost(ConnectionError::ApplicationClosed(close))) => {
                    match ConnectionErrorCode::try_from(close.error_code) {
                        Ok(code) => {
                            conn_error = Some(code);
                            match code {
                                ConnectionErrorCode::Closed => ConnectionErrorCode::Closed,
                                ConnectionErrorCode::Aborted => ConnectionErrorCode::Aborted,
                                _ => ConnectionErrorCode::ProtocolError,
                            }
                        }
                        Err(code) => {
                            error!(%code, "invalid ConnectionErrorCode");
                            ConnectionErrorCode::ProtocolError
                        }
                    }
                }
                Ok(ReadError::ConnectionLost(ConnectionError::LocallyClosed)) => {
                    ConnectionErrorCode::Closed
                }
                Ok(_) => ConnectionErrorCode::ProtocolError,
                Err(err) => {
                    error!(?err, "downcast to ReadError failed");
                    // TODO: Maybe this should be unreachable!()?
                    ConnectionErrorCode::ImplementationError
                }
            },
        };

        match close_code {
            ConnectionErrorCode::Closed => {
                trace!("connection closed");
                for tx in self.close_waiters.drain(..) {
                    tx.send(()).ok();
                }
            }
            ConnectionErrorCode::Aborted => {
                trace!("connection aborted");
                self.close_waiters.drain(..);
            }
            _ => {
                trace!(?conn_error, ?stream_error, "connection failure");
                self.close_waiters.drain(..);
            }
        }

        close_code
    }
}

#[derive(Debug)]
pub(super) enum ConnectionActorMessage {
    /// A new stream was opened.
    NewStream(Arc<Notify>),
    /// Close the connection.
    ///
    /// Responds once the connection is closed.
    Close(oneshot::Sender<()>),
    /// Abort all streams.
    ///
    /// Responds once all aborts have been sent.
    Abort(oneshot::Sender<()>),
}
