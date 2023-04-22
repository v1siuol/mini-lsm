#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

use std::cmp::Ordering;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::lsm_storage::BlockCache;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: Bytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        // [offset (4B) key_len (2B) key (keylen) ...] block_meta_offset (4B)
        // Pre-compute required space.
        let ori_len = buf.len();
        let mut est_len = 0;
        for meta in block_meta {
            est_len += 4;
            est_len += 2;
            est_len += meta.first_key.len();
        }
        est_len += 4;
        buf.reserve(est_len);
        // Encode.
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put_slice(&meta.first_key);
        }
        buf.put_u32(ori_len as u32);
        assert_eq!(est_len, buf.len() - ori_len);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut meta: Vec<BlockMeta> = Vec::new();
        while buf.has_remaining() {
            let offset = buf.get_u32() as usize;
            let key_len = buf.get_u16() as usize;
            let first_key = buf.copy_to_bytes(key_len);
            meta.push(BlockMeta { offset, first_key });
        }
        meta
    }
}

/// A file object.
pub struct FileObject(File, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0.read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        Ok(FileObject(
            File::options().read(true).write(false).open(path)?,
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        unimplemented!()
    }
}

pub struct SsTable {
    file: FileObject,
    block_metas: Vec<BlockMeta>,
    block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let n = file.size();
        let block_meta_offset = file.read(n - 4, 4)?;
        let block_meta_offset = (&block_meta_offset[..]).get_u32() as u64;
        let block_meta = file.read(block_meta_offset, n - block_meta_offset - 4)?;
        let block_metas = BlockMeta::decode_block_meta(&block_meta[..]);
        let block_meta_offset = block_meta_offset as usize;
        Ok(Self {
            file,
            block_metas,
            block_meta_offset,
            id,
            block_cache,
        })
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset = self.block_metas[block_idx].offset as u64;
        let offset_end = self
            .block_metas
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset) as u64;
        let block_data = self.file.read(offset, offset_end - offset)?;
        Ok(Arc::new(Block::decode(&block_data[..])))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref block_cache) = self.block_cache {
            let blk = block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))?;
            Ok(blk)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        let mut left = 0;
        let mut right = self.block_metas.len();
        while left < right {
            let mid = (left + right) / 2;
            let curr_key = &self.block_metas[mid].first_key[..];
            match curr_key.cmp(key) {
                Ordering::Equal => return mid,
                Ordering::Less => left = mid + 1,
                Ordering::Greater => right = mid,
            }
        }
        if left == 0 {
            0
        } else {
            left - 1
        }
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_metas.len()
    }
}

#[cfg(test)]
mod tests;
