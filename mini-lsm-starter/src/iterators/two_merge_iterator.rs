use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    use_a: bool,
}

impl<A: StorageIterator, B: StorageIterator> TwoMergeIterator<A, B> {
    fn select_ab(&mut self) {
        if !self.a.is_valid() {
            self.use_a = false;
            return;
        }
        if !self.b.is_valid() {
            self.use_a = true;
            return;
        }
        self.use_a = self.a.key() < self.b.key();
    }

    fn skip_b(&mut self) -> Result<()> {
        if self.a.is_valid() {
            while self.b.is_valid() && self.b.key() == self.a.key() {
                self.b.next()?;
            }
        }
        Ok(())
    }

    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self { a, b, use_a: false };
        iter.skip_b()?;
        iter.select_ab();
        Ok(iter)
    }
}

impl<A: StorageIterator, B: StorageIterator> StorageIterator for TwoMergeIterator<A, B> {
    fn key(&self) -> &[u8] {
        if self.use_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.use_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        if self.use_a {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.use_a {
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        self.skip_b()?;
        self.select_ab();
        Ok(())
    }
}
