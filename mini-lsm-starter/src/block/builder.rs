use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    // Maximum byte limit.
    block_size: usize,
    data: Vec<u8>,
    offsets: Vec<u16>,
    num_of_elements: usize,
    next_offset: u16,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            data: Vec::new(),
            offsets: Vec::new(),
            num_of_elements: 0,
            next_offset: 0,
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        // Assert key empty?
        // Assert key/value size limit?
        let key_len = key.len() as u16;
        let value_len = value.len() as u16;
        let entry_size = 2 + key_len + 2 + value_len;
        if (self.next_offset + entry_size + 2) as usize >= self.block_size {
            return false;
        }
        // Construct entry
        let key_len_bytes: [u8; 2] = key_len.to_be_bytes();
        self.data.extend(key_len_bytes);
        self.data.extend(key);
        let value_len_bytes: [u8; 2] = value_len.to_be_bytes();
        self.data.extend(value_len_bytes);
        self.data.extend(value);
        self.offsets.push(self.next_offset);
        self.next_offset += entry_size;
        self.num_of_elements += 1;
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.num_of_elements == 0
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        // sort order?
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
