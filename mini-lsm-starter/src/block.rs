#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_slice(&self.data);
        for offset in self.offsets.iter() {
            bytes.put_u16(*offset);
        }
        let num_of_elements = self.offsets.len();
        bytes.put_u16(num_of_elements as u16);
        bytes.freeze()
    }

    pub fn decode(data: &[u8]) -> Self {
        // Assume valid.
        let n = data.len();
        let num_of_elements_start = n - 2;
        let mut num_of_elements_bytes = &data[num_of_elements_start..n];
        let num_of_elements = num_of_elements_bytes.get_u16() as usize;

        let mut offsets = Vec::new();
        let mut offsets_start = n - (num_of_elements + 1) * 2;
        let data_vec = data[..offsets_start].to_vec();
        while offsets_start < num_of_elements_start {
            offsets.push((&data[offsets_start..offsets_start + 2]).get_u16());
            offsets_start += 2;
        }

        Self {
            data: data_vec,
            offsets,
        }
    }
}

#[cfg(test)]
mod tests;
