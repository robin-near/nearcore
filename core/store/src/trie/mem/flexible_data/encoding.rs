use crate::trie::mem::arena::{Arena, ArenaPtr, ArenaPtrMut, ArenaSliceMut, BorshFixedSize};
use borsh::{BorshDeserialize, BorshSerialize};
use std::io::Write;

use super::FlexibleDataHeader;

/// Facilitates allocation and encoding of flexibly-sized data.
pub struct RawEncoder<'a> {
    data: ArenaSliceMut<'a>,
    pos: usize,
}

impl<'a> Write for RawEncoder<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.data.raw_slice_mut()[self.pos..self.pos + buf.len()].copy_from_slice(buf);
        self.pos += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> RawEncoder<'a> {
    /// Creates a new memory allocation of the given size, returning an encoder
    /// that can be used to initialize the allocated memory.
    pub fn new(arena: &'a mut Arena, n: usize) -> RawEncoder<'a> {
        let data = arena.alloc(n);
        RawEncoder { data, pos: 0 }
    }

    /// Encodes the given fixed-size field to the current encoder position,
    /// and then advance the position by the size of the field.
    pub fn encode<T: BorshSerialize>(&mut self, data: T) {
        data.serialize(self).unwrap();
    }

    /// Encodes the given flexibly-sized part of the data to the current
    /// encoder position, and then advance the position by the size of the
    /// flexibly-sized part, as returned by `header.flexible_data_length()`.
    pub fn encode_flexible<T: FlexibleDataHeader>(&mut self, header: &T, data: T::InputData) {
        let length = header.flexible_data_length();
        header.encode_flexible_data(data, &mut self.data.subslice(self.pos, length));
        self.pos += length;
    }

    /// Finishes the encoding process and returns a pointer to the allocated
    /// memory. The caller is responsible for freeing the pointer later.
    pub fn finish(self) -> ArenaSliceMut<'a> {
        assert_eq!(self.pos, self.data.len());
        self.data
    }
}

/// Facilitates the decoding of flexibly-sized data.
/// The lifetime 'a should be a lifetime that guarantees the availability of
/// the memory being decoded from.
pub struct RawDecoder<'a> {
    data: ArenaPtr<'a>,
    pos: usize,
}

impl<'a> RawDecoder<'a> {
    /// Starts decoding from the given memory location. The location should be
    /// a pointer returned by RawEncoder::finish.
    pub fn new(data: ArenaPtr<'a>) -> RawDecoder<'a> {
        RawDecoder { data, pos: 0 }
    }

    /// Decodes a fixed-size field at the current decoder position, and then
    /// advances the position by the size of the field.
    pub fn decode<T: BorshDeserialize + BorshFixedSize>(&mut self) -> T {
        let slice = self.data.offset(self.pos).slice(T::SERIALIZED_SIZE);
        let result = T::try_from_slice(slice.as_slice()).unwrap();
        self.pos += T::SERIALIZED_SIZE;
        result
    }

    /// Decodes a fixed-sized field at the current position, but does not
    /// advance the position.
    pub fn peek<T: BorshDeserialize + BorshFixedSize>(&mut self) -> T {
        let slice = self.data.offset(self.pos).slice(T::SERIALIZED_SIZE);
        T::try_from_slice(slice.as_slice()).unwrap()
    }

    /// Decodes a flexibly-sized part of the data at the current position,
    /// and then advances the position by the size of the flexibly-sized part,
    /// as returned by `header.flexible_data_length()`.
    pub fn decode_flexible<T: FlexibleDataHeader>(&mut self, header: &T) -> T::View<'a> {
        let length = header.flexible_data_length();
        let view = header.decode_flexible_data(&self.data.offset(self.pos).slice(length));
        self.pos += length;
        view
    }
}

pub struct RawDecoderMut<'a> {
    data: ArenaPtrMut<'a>,
    pos: usize,
}

impl<'a> RawDecoderMut<'a> {
    pub fn new(data: ArenaPtrMut<'a>) -> RawDecoderMut<'a> {
        RawDecoderMut { data, pos: 0 }
    }

    pub fn decode<T: BorshDeserialize + BorshFixedSize>(&mut self) -> T {
        let ptr = self.data.offset(self.pos);
        let slice = ptr.slice(T::SERIALIZED_SIZE);
        let result = T::try_from_slice(slice.as_slice()).unwrap();
        self.pos += T::SERIALIZED_SIZE;
        result
    }

    pub fn peek<T: BorshDeserialize + BorshFixedSize>(&mut self) -> T {
        let ptr = self.data.offset(self.pos);
        let slice = ptr.slice(T::SERIALIZED_SIZE);
        T::try_from_slice(slice.as_slice()).unwrap()
    }

    pub fn overwrite<T: BorshSerialize + BorshFixedSize>(&mut self, data: T) {
        let mut ptr = self.data.offset(self.pos);
        let mut slice = ptr.slice_mut(T::SERIALIZED_SIZE);
        data.serialize(&mut slice.raw_slice_mut()).unwrap();
        self.pos += T::SERIALIZED_SIZE;
    }
}
