use std::path::Path;
use std::sync::Arc;

use anyhow::{Ok, Result};

use super::{BlockMeta, FileObject, SsTable};
use crate::block::BlockBuilder;
use crate::lsm_storage::BlockCache;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    block_size: usize,
    block_builder: BlockBuilder,
    pub(super) meta: Vec<BlockMeta>,
    data: Vec<u8>,
    first_key: Vec<u8>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            block_builder: BlockBuilder::new(block_size),
            meta: Vec::new(),
            data: Vec::new(),
            first_key: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = key.to_vec();
        }
        if self.block_builder.add(key, value) {
            return;
        }
        // Block full, build and create a new block.
        self.build_block();

        // Try to add again.
        self.first_key = key.to_vec();
        assert!(self.block_builder.add(key, value));
    }

    /// Get the estimated size of the SSTable.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    fn build_block(&mut self) {
        let block_builder =
            std::mem::replace(&mut self.block_builder, BlockBuilder::new(self.block_size));
        let block_data = block_builder.build().encode();
        let meta = BlockMeta {
            offset: self.data.len(),
            first_key: std::mem::take(&mut self.first_key).into(),
        };
        self.meta.push(meta);
        self.data.extend(block_data);
    }

    /// Builds the SSTable and writes it to the given path. No need to actually write to disk until
    /// chapter 4 block cache.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.build_block();
        let mut buf = self.data;
        let meta_offset = buf.len();
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        let file = FileObject::create(path.as_ref(), buf)?;
        Ok(SsTable {
            file,
            block_metas: self.meta,
            block_meta_offset: meta_offset,
            id,
            block_cache,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
