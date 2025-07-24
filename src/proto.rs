use std::io;

use anyhow::{Result, bail};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::varint::VarInt;

/// Streams have several purposes.
///
/// The first message on the stream sent by the side initiating side indicates the type of
/// stream.  This message also explicitly opens the stream on both sides.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u32)]
pub(super) enum StreamType {
    /// A control stream.
    ///
    /// Should be the first stream opened.  Only the Client should open this stream, if the
    /// server opens this stream is should close it without sending anything on it.
    Control = 0,
    /// A stream carrying application messages.
    User = 1,
}

impl StreamType {
    pub(super) fn encode(&self, buf: impl BufMut) {
        let val = VarInt::from(*self as u32);
        val.encode(buf);
    }

    pub(super) fn decode(buf: impl Buf) -> Result<Self> {
        let varint = VarInt::decode(buf)?;
        let discriminant = u32::try_from(varint)?;
        // TODO: Some trick or crate surely solves this?
        let val = match discriminant {
            0 => Self::Control,
            1 => Self::User,
            _ => bail!("invalid stream type"),
        };
        Ok(val)
    }
}

/// Frames sent on a control stream.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u32)]
pub(super) enum ControlFrame {
    /// Requests to close the connection.
    ///
    /// The [`ConnectionClose`] message can only be sent by the client to the server.  If
    /// the server wants to initiate close it sends this messages instead, to which the
    /// client should respond with [`ConnectionClose`] after having closed its streams.
    RequestConnectionClose = 0,
    /// Instruct to close the connection.
    ///
    /// Sent by the client to the server to indicate it wants to close the connection.  The
    /// client will no longer send any new messages on any streams after this and should
    /// close or abort all other streams first.
    ///
    /// After having sent this message the client should finish this stream.
    ///
    /// When the server receives this it can close the connection.  It may optionally read
    /// other streams to the end-of-stream.
    ConnectionClose = 1,
}

impl ControlFrame {
    pub(super) fn encode(&self, buf: &mut BytesMut) {
        let val = VarInt::from(*self as u32);
        val.encode(buf);
    }

    pub(super) fn decode(buf: impl Buf) -> Result<Self> {
        let varint = VarInt::decode(buf)?;
        let discriminant = u32::try_from(varint)?;
        // TODO: Some trick or crate surely solves this?
        let val = match discriminant {
            0 => Self::RequestConnectionClose,
            1 => Self::ConnectionClose,
            _ => bail!("invalid stream type"),
        };
        Ok(val)
    }
}

/// Error codes for streams.
///
/// To be used when resetting [`SendStream`] or stopping a [`RecvStream`].
///
/// [`SendStream`]: iroh::endpoint::SendStream
/// [`RecvStream`]: iroh::endpoint::RecvStream
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u32)]
pub(super) enum StreamErrorCode {
    /// The stream was dropped.
    ///
    /// We should not do this, but this is what Quinn uses by default.
    #[allow(dead_code)]
    Dropped = 0,
    /// This control stream is not needed.
    ///
    /// Used for the control stream opened by the server, which is not needed.
    ControlStreamNotNeeded = 1,
    // /// Error in the imsg protocol.
    // ProtocolError = 2,
    /// The stream was closed.
    Closed = 3,
    /// the stream was aborted.
    Aborted = 4,
}

impl From<StreamErrorCode> for iroh::endpoint::VarInt {
    fn from(source: StreamErrorCode) -> Self {
        iroh::endpoint::VarInt::from(source as u32)
    }
}

/// Error codes for the connection.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u32)]
pub(super) enum ConnectionErrorCode {
    /// The connection was dropped.
    ///
    /// We should not use this, but this is what Quinn uses by default.
    #[allow(dead_code)]
    Dropped = 0,
    /// The connection was closed orderly.
    Closed = 1,
}

impl From<ConnectionErrorCode> for iroh::endpoint::VarInt {
    fn from(value: ConnectionErrorCode) -> Self {
        iroh::endpoint::VarInt::from(value as u32)
    }
}

/// Codec for length-delimited frames using QUIC VarInts.
#[derive(Debug)]
pub(super) struct ImsgCodec;

impl Decoder for ImsgCodec {
    type Item = Bytes;

    type Error = ImsgCodecError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Make sure not to advance the cursor in the buffer until the entire frame is
        // ready!

        if src.len() < 1 {
            return Ok(None);
        }

        let Some(payload_len_varint) = VarInt::decode_slice(&src[..]) else {
            return Ok(None);
        };
        let payload_len: usize = payload_len_varint
            .to_u64()
            .try_into()
            .map_err(|_| ImsgCodecError::CodecError)?;
        let frame_size = payload_len_varint.size() + payload_len;
        // TODO: Enforce a max frame size?

        if src.len() < frame_size {
            // Reserve enough to complete decoding the frame.
            src.reserve(frame_size);
            return Ok(None);
        }

        src.advance(payload_len_varint.size());
        let payload = src.split_to(payload_len);
        Ok(Some(payload.freeze()))
    }
}

impl Encoder<Bytes> for ImsgCodec {
    type Error = ImsgCodecError;

    fn encode(&mut self, payload: Bytes, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        // TODO: Enforce a max frame size?

        let payload_len_varint =
            VarInt::try_from(payload.len()).map_err(|_| ImsgCodecError::CodecError)?;
        payload_len_varint.encode(&mut *dst);

        // TODO: Copy all the things!  Some day we'll do this using vectored writes.  But
        // not while we use the Encoder trait.
        dst.extend_from_slice(payload.as_ref());

        Ok(())
    }
}

/// Encoding or decoding errors.
#[derive(Debug, thiserror::Error)]
#[error("encoding or decoding error")]
pub(super) enum ImsgCodecError {
    #[error("codec error")]
    CodecError,
    #[error("io error")]
    IoError(#[from] io::Error),
}
