use crate::trie::mem::arena::{ArenaSlice, ArenaSliceMut, BorshFixedSize};

use super::FlexibleDataHeader;
use crate::TrieStorage;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::hash::CryptoHash;
use near_primitives::state::{FlatStateValue, ValueRef};

/// Flexibly-sized data header for a trie value, representing either an inline
/// value, or a reference to a value stored in the State column.
///
/// The flexible part of the data is either the inlined value as a byte array,
/// or a CryptoHash representing the reference hash.
#[derive(Clone, Copy, BorshSerialize, BorshDeserialize)]
pub struct EncodedValueHeader {
    // The high bit is 1 if the value is inlined, 0 if it is a reference.
    // The lower bits are the length of the value.
    length_and_inlined: u32,
}

impl BorshFixedSize for EncodedValueHeader {
    const SERIALIZED_SIZE: usize = std::mem::size_of::<u32>();
}

impl EncodedValueHeader {
    const INLINED_MASK: u32 = 0x80000000;

    fn decode(&self) -> (u32, bool) {
        (
            self.length_and_inlined & !Self::INLINED_MASK,
            self.length_and_inlined & Self::INLINED_MASK != 0,
        )
    }
}

impl FlexibleDataHeader for EncodedValueHeader {
    type InputData = FlatStateValue;
    type View<'a> = ValueView<'a>;

    fn from_input(value: &FlatStateValue) -> Self {
        match value {
            FlatStateValue::Ref(value_ref) => {
                EncodedValueHeader { length_and_inlined: value_ref.length }
            }
            FlatStateValue::Inlined(v) => {
                assert!(v.len() as u32 & Self::INLINED_MASK == 0);
                EncodedValueHeader { length_and_inlined: Self::INLINED_MASK | v.len() as u32 }
            }
        }
    }

    fn flexible_data_length(&self) -> usize {
        let (length, inlined) = self.decode();
        if inlined {
            length as usize
        } else {
            std::mem::size_of::<CryptoHash>()
        }
    }

    fn encode_flexible_data(&self, value: FlatStateValue, target: &mut ArenaSliceMut<'_>) {
        let (_, inlined) = self.decode();
        match value {
            FlatStateValue::Ref(value_ref) => {
                assert!(!inlined);
                target.raw_slice_mut().copy_from_slice(&value_ref.hash.0);
            }
            FlatStateValue::Inlined(v) => {
                assert!(inlined);
                target.raw_slice_mut().copy_from_slice(&v);
            }
        }
    }

    fn decode_flexible_data<'a>(&self, source: &ArenaSlice<'a>) -> ValueView<'a> {
        let (length, inlined) = self.decode();
        if inlined {
            ValueView::Inlined(source.clone())
        } else {
            ValueView::Ref { length, hash: CryptoHash::try_from_slice(source.as_slice()).unwrap() }
        }
    }
}

// Efficient view of the encoded value.
#[derive(Debug, Clone)]
pub enum ValueView<'a> {
    Ref { length: u32, hash: CryptoHash },
    Inlined(ArenaSlice<'a>),
}

impl<'a> ValueView<'a> {
    pub fn to_value(&self, storage: &dyn TrieStorage) -> Vec<u8> {
        match self {
            Self::Ref { hash, .. } => storage.retrieve_raw_bytes(hash).unwrap().to_vec(),
            Self::Inlined(data) => data.as_slice().to_vec(),
        }
    }

    pub fn to_flat_value(&self) -> FlatStateValue {
        match self {
            Self::Ref { length, hash } => {
                FlatStateValue::Ref(ValueRef { length: *length, hash: *hash })
            }
            Self::Inlined(data) => FlatStateValue::Inlined(data.as_slice().to_vec()),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Ref { length, .. } => *length as usize,
            Self::Inlined(data) => data.len(),
        }
    }
}