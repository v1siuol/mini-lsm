use super::Block;
use bytes::Buf;
use std::{cmp::Ordering, sync::Arc};

/// Iterates on a block.
pub struct BlockIterator {
    block: Arc<Block>,
    key: Vec<u8>,
    value: Vec<u8>,
    // Index of entry.
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Returns true if the iterator is valid.
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to(self.idx);
    }

    /// Seek to the first key that >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        // Binary search
        let mut left = 0;
        let mut right = self.block.offsets.len();
        while left < right {
            let mid = (left + right) / 2;
            self.seek_to(mid);
            match self.key().cmp(key) {
                Ordering::Equal => return,
                Ordering::Less => left = mid + 1,
                Ordering::Greater => right = mid,
            }
        }
        self.seek_to(left);
    }

    fn seek_to(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value.clear();
            return;
        }
        let offset = self.block.offsets[idx] as usize;
        self.seek_to_offset(offset);
        self.idx = idx;
    }

    fn seek_to_offset(&mut self, offset: usize) {
        let mut data = &self.block.data[offset..];
        let key_len = data.get_u16() as usize;
        let key = data[..key_len].to_vec();
        self.key.clear();
        self.key.extend(key);
        data.advance(key_len);
        let value_len = data.get_u16() as usize;
        let value = data[..value_len].to_vec();
        self.value.clear();
        self.value.extend(value);
        data.advance(value_len);
    }
}
