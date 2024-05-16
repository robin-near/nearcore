use super::FlexibleDataHeader;
use crate::trie::mem::arena::unsafe_ser::UnsafeSerde;
use crate::trie::mem::arena::{Arena, ArenaPtr, ArenaPtrMut, ArenaSliceMut};
use near_primitives::hash::CryptoHash;
use std::mem::size_of;

/// Facilitates allocation and encoding of flexibly-sized data.
pub struct RawEncoder<'a> {
    data: ArenaSliceMut<'a>,
    pos: usize,
}

impl<'a> RawEncoder<'a> {
    /// Creates a new arena allocation of the given size, returning an encoder
    /// that can be used to initialize the allocated memory.
    pub fn new(arena: &'a mut Arena, n: usize) -> RawEncoder<'a> {
        let data = arena.alloc(n);
        RawEncoder { data, pos: 0 }
    }

    /// Encodes the given fixed-size field to the current encoder position,
    /// and then advances the position by the encoded size of the field.
    pub fn encode<T: UnsafeSerde>(&mut self, data: T) {
        data.unsafe_serialize(self.data.subslice_mut(self.pos, size_of::<T>()).raw_slice_mut());
        self.pos += size_of::<T>();
    }

    /// Encodes the given flexibly-sized part of the data to the current
    /// encoder position, and then advances the position by the size of the
    /// flexibly-sized part, as returned by `header.flexible_data_length()`.
    /// Note that the header itself is NOT encoded; only the flexible part is.
    /// The header is expected to have been encoded earlier.
    pub fn encode_flexible<T: FlexibleDataHeader>(&mut self, header: &T, data: &T::InputData) {
        let length = header.flexible_data_length();
        header.encode_flexible_data(data, &mut self.data.subslice_mut(self.pos, length));
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
pub struct RawDecoder<'a> {
    data: ArenaPtr<'a>,
    pos: usize,
}

impl<'a> RawDecoder<'a> {
    /// Starts decoding from the given memory position. The position should be
    /// the beginning of an earlier slice returned by `RawEncoder::finish`.
    pub fn new(data: ArenaPtr<'a>) -> RawDecoder<'a> {
        RawDecoder { data, pos: 0 }
    }

    /// Decodes a fixed-size field at the current decoder position, and then
    /// advances the position by the size of the field.
    pub fn decode<T: UnsafeSerde>(&mut self) -> T {
        let slice = self.data.slice(self.pos, size_of::<T>());
        let result = T::unsafe_deserialize(slice.raw_slice());
        self.pos += size_of::<T>();
        result
    }

    /// Decodes a fixed-sized field at the current position, but does not
    /// advance the position.
    pub fn peek<T: UnsafeSerde>(&mut self) -> T {
        let slice = self.data.slice(self.pos, size_of::<T>());
        T::unsafe_deserialize(slice.raw_slice())
    }

    /// Decodes a flexibly-sized part of the data at the current position,
    /// and then advances the position by the size of the flexibly-sized part,
    /// as returned by `header.flexible_data_length()`.
    pub fn decode_flexible<T: FlexibleDataHeader>(&mut self, header: &T) -> T::View<'a> {
        let length = header.flexible_data_length();
        let view = header.decode_flexible_data(&self.data.slice(self.pos, length));
        self.pos += length;
        view
    }
}

/// Provides ability to decode, but also to overwrite some data.
pub struct RawDecoderMut<'a> {
    data: ArenaPtrMut<'a>,
    pos: usize,
}

impl<'a> RawDecoderMut<'a> {
    pub fn new(data: ArenaPtrMut<'a>) -> RawDecoderMut<'a> {
        RawDecoderMut { data, pos: 0 }
    }

    /// Same with `RawDecoder::decode`.
    pub fn decode<T: UnsafeSerde>(&mut self) -> T {
        let slice = self.data.slice(self.pos, size_of::<T>());
        let result = T::unsafe_deserialize(slice.raw_slice());
        self.pos += size_of::<T>();
        result
    }

    /// Same with `RawDecoder::peek`.
    pub fn peek<T: UnsafeSerde>(&mut self) -> T {
        let slice = self.data.slice(self.pos, size_of::<T>());
        T::unsafe_deserialize(slice.raw_slice())
    }

    /// Overwrites the data at the current position with the given data,
    /// and advances the position by the size of the data.
    pub fn overwrite<T: UnsafeSerde>(&mut self, data: T) {
        let mut slice = self.data.slice_mut(self.pos, size_of::<T>());
        data.unsafe_serialize(slice.raw_slice_mut());
        self.pos += size_of::<T>();
    }
}

#[derive(Clone, Copy)]
#[repr(C, packed)]
pub struct UnalignedCryptoHash(pub CryptoHash);

impl UnsafeSerde for UnalignedCryptoHash {}

impl From<CryptoHash> for UnalignedCryptoHash {
    fn from(hash: CryptoHash) -> Self {
        Self(hash)
    }
}

impl From<UnalignedCryptoHash> for CryptoHash {
    fn from(hash: UnalignedCryptoHash) -> Self {
        hash.0
    }
}
