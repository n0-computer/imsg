use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use iroh::endpoint::{RecvStream, SendStream};
use n0_future::{SinkExt, StreamExt};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::debug;

use crate::proto::{ConnectionErrorCode, ControlFrame, ImsgCodec};

/// Actor monitoring a connection.
#[derive(Debug)]
pub(super) struct ConnectionActor {
    /// The iroh/QUIC connection.
    connection: iroh::endpoint::Connection,
    /// Whether we are the server or the client.
    side: super::Side,
    ctrl_writer: FramedWrite<SendStream, ImsgCodec>,
    ctrl_reader: FramedRead<RecvStream, ImsgCodec>,
    /// All streams.
    ///
    /// If the [`Arc::strong_count`] drops to 1 the stream is closed.
    streams: Vec<Arc<Notify>>,
    /// Whether the remote is closed this connection.
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
        }
    }

    pub(super) async fn run(&mut self, mut inbox: mpsc::Receiver<ConnectionActorMessage>) {
        loop {
            tokio::select! {
                biased;
                msg = inbox.recv() => {
                    let Some(msg) = msg else {
                        // Inbox closed, all connection objects dropped.
                        break; // TODO
                    };
                    if let Some(frame) = self.handle_inbox(msg) {
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
                    let Some(msg) = msg else {
                        // Remote closed the connection.
                        break;  // TODO
                    };
                    self.handle_ctrl_frame(msg.expect("TODO: ctrl stream invalid msg"));
                }
                reason = self.connection.closed() => {
                    debug!(?reason, "connection closed");
                    break;  // TODO
                }
            }
        }
    }

    fn handle_ctrl_frame(&mut self, mut msg: Bytes) {
        let frame = ControlFrame::decode(&mut msg).expect("TODO");
        assert!(msg.is_empty(), "TODO: invalid control message");
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
                    self.connection
                        .close(ConnectionErrorCode::Closed.into(), b"close".as_ref());
                }
            }
        }
    }

    /// Handles an actor message.
    ///
    /// Optionally returns a message to be sent on the control stream.
    fn handle_inbox(&mut self, msg: ConnectionActorMessage) -> Option<ControlFrame> {
        match msg {
            ConnectionActorMessage::NewStream(notify) => {
                // Clean up any closed streams first.
                self.streams.retain(|n| Arc::strong_count(n) > 1);
                self.streams.push(notify);
                None
            }
            ConnectionActorMessage::Close(tx) => {
                // First abort all the connections.  If a stream was already closed but not
                // yet dropped it will simply ignore this signal.
                for notify in self.streams.drain(..) {
                    notify.notify_one();
                }

                // We cheat, and tell our connection object that we did close.  It can't
                // tell the difference between it being on the wire or that we haven't
                // sent this yet.
                tx.send(()).ok();

                if self.remote_closed {
                    // Close the connection
                    match self.side {
                        crate::Side::Server => {
                            self.connection
                                .close(ConnectionErrorCode::Closed.into(), b"close".as_ref());
                            None
                        }
                        crate::Side::Client => Some(ControlFrame::ConnectionClose),
                    }
                } else {
                    self.local_closed = true;
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
                None
            }
        }
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
