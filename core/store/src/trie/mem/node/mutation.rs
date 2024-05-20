use super::MemTrieNodePtrMut;
use crate::trie::mem::arena::IArenaMemory;
use crate::trie::mem::flexible_data::encoding::RawDecoderMut;

impl<'a, Memory: IArenaMemory> MemTrieNodePtrMut<'a, Memory> {
    pub(crate) fn decoder_mut(&mut self) -> RawDecoderMut<Memory> {
        RawDecoderMut::new(self.ptr.ptr_mut())
    }
}
