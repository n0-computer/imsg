use std::fmt;

use anyhow::{Result, bail};
use bytes::{Buf, BufMut};

/// A QUIC VarInt.
#[derive(Default, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct VarInt(u64);

impl VarInt {
    /// Returns the value as a [`u64`].
    pub(crate) fn to_u64(&self) -> u64 {
        self.0
    }

    /// Computes the number of bytes needed to encode this value.
    pub(crate) const fn size(self) -> usize {
        if self.0 < 2u64.pow(6) {
            1
        } else if self.0 < 2u64.pow(14) {
            2
        } else if self.0 < 2u64.pow(30) {
            4
        } else if self.0 < 2u64.pow(62) {
            8
        } else {
            panic!("malformed VarInt");
        }
    }

    /// Decodes from the buffer.
    pub(super) fn decode_slice(src: &[u8]) -> Option<Self> {
        match Self::decode(src) {
            Ok(i) => Some(i),
            Err(_) => None,
        }
    }

    /// Decodes from the buffer.
    ///
    /// The cursor of the [`Buf`] will be advanced.
    pub(crate) fn decode(mut src: impl Buf) -> Result<Self> {
        if src.remaining() < 1 {
            bail!("insufficient bytes to decode VarInt");
        }
        let mut buf = [0u8; 8];
        buf[0] = src.get_u8();
        let tag = buf[0] >> 6;
        buf[0] &= 0b0011_1111;
        let val = match tag {
            0b00 => u64::from(buf[0]),
            0b01 => {
                if src.remaining() < 1 {
                    bail!("insufficient bytes to decode VarInt");
                }
                buf[1] = src.get_u8();
                u64::from(u16::from_be_bytes(buf[..2].try_into().unwrap()))
            }
            0b10 => {
                if src.remaining() < 3 {
                    bail!("insufficient bytes to decode VarInt");
                }
                src.copy_to_slice(&mut buf[1..4]);
                u64::from(u32::from_be_bytes(buf[..4].try_into().unwrap()))
            }
            0b11 => {
                if src.remaining() < 7 {
                    bail!("insufficient bytes to decode VarInt");
                }
                src.copy_to_slice(&mut buf[1..8]);
                u64::from_be_bytes(buf)
            }
            _ => unreachable!(),
        };
        Ok(Self(val))
    }

    /// Encodes into the buffer.
    pub(crate) fn encode(&self, mut buf: impl BufMut) {
        if self.0 < 2u64.pow(6) {
            buf.put_u8(self.0 as u8);
        } else if self.0 < 2u64.pow(14) {
            buf.put_u16((0b01 << 14) | self.0 as u16);
        } else if self.0 < 2u64.pow(30) {
            buf.put_u32((0b10 << 30) | self.0 as u32);
        } else if self.0 < 2u64.pow(62) {
            buf.put_u64((0b11 << 62) | self.0);
        } else {
            unreachable!()
        }
    }
}

impl From<u8> for VarInt {
    fn from(x: u8) -> Self {
        Self(x.into())
    }
}

impl From<u16> for VarInt {
    fn from(x: u16) -> Self {
        Self(x.into())
    }
}

impl From<u32> for VarInt {
    fn from(x: u32) -> Self {
        Self(x.into())
    }
}

impl TryFrom<usize> for VarInt {
    type Error = VarIntBoundExceeded;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        let value: u64 = value.try_into().map_err(|_| VarIntBoundExceeded)?;
        if value < 2u64.pow(62) {
            Ok(Self(value as u64))
        } else {
            Err(VarIntBoundExceeded)
        }
    }
}

impl TryFrom<VarInt> for u32 {
    type Error = std::num::TryFromIntError;

    fn try_from(value: VarInt) -> Result<Self, Self::Error> {
        value.0.try_into()
    }
}

impl fmt::Debug for VarInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Display for VarInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("VarInt bound exceeded")]
pub(crate) struct VarIntBoundExceeded;
