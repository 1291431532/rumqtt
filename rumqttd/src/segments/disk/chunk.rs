use std::{io, path::Path};

use bytes::Bytes;
use sha2::Digest;

use super::{index::Index, segment::DiskSegment};

/// The handler for a segment file which is on the disk, and it's corresponding index file.
// TODO: the current implementation assumes that segments and index are completely in sync if are
// able to open them. As we verify checksum, this can only cause problems when we write the wrong
// information onto the disk in the first place, which can happen only in `Chunk::new`.
#[derive(Debug)]
pub struct Chunk {
    /// The handle for index file.
    index: Index,
    /// The handle for segment file.
    segment: DiskSegment,
}

impl Chunk {
    /// Opens an existing segment-index pair from the disk. Will throw error if either does not
    /// exist. Note that this does not verify the checksum. Call [`Chunk::verify`] to do so
    /// manually.
    ///
    /// This only opens them immutably.
    #[inline]
    pub(crate) fn open<P: AsRef<Path>>(dir: P, index: u64) -> io::Result<Self> {
        let index_path = dir.as_ref().join(&format!("{:020}.index", index));
        let segment_path = dir.as_ref().join(&format!("{:020}.segment", index));

        let index = Index::open(index_path)?;
        let segment = DiskSegment::open(segment_path)?;

        Ok(Self { index, segment })
    }

    /// Creates a new segment-index pair onto the disk, and throws error if they already exist. The
    /// given hasher is used to calculate the checksum of the given bytes. Given bytes are
    /// stored as 1 single segment. Hence there is no possibility of a partially filled chunk
    ///
    /// This only opens them immutably, after writing the given data.
    pub(crate) fn new<P: AsRef<Path>>(
        dir: P,
        index: u64,
        bytes: Vec<Bytes>,
        hasher: &mut impl Digest,
    ) -> io::Result<Self> {
        let index_path = dir.as_ref().join(&format!("{:020}.index", index));
        let segment_path = dir.as_ref().join(&format!("{:020}.segment", index));

        let mut lens = Vec::with_capacity(bytes.len());
        for byte in &bytes {
            lens.push(byte.len() as u64);
        }

        let bytes: Vec<u8> = bytes.into_iter().flatten().collect();
        let bytes = Bytes::from(bytes);
        hasher.update(&bytes);
        let segment_hash = hasher.finalize_reset();

        let segment = DiskSegment::new(segment_path, bytes)?;
        let index = Index::new(index_path, segment_hash.as_ref(), hasher, lens)?;

        Ok(Self { index, segment })
    }

    /// Get the size of the segment.
    #[inline]
    pub(crate) fn segment_size(&self) -> u64 {
        self.segment.size()
    }

    /// Verify the checksum by reading the checksum from the start of the index file, calculating
    /// the checksum of segment file and then comparing those two.
    pub(crate) fn verify(&self, hasher: &mut impl Digest) -> io::Result<bool> {
        let read_hash = self.index.read_hash()?;
        // unwrap fine as we reading the exactly the len starting from 0.
        let offset = self.segment.size() as u64;
        let read_segment = self.segment.read(0, offset)?.unwrap();
        hasher.update(&read_segment);
        let calculated_hash = hasher.finalize_reset();

        // TODO: maybe there is a better method than comparing 1 byte at a time
        Ok((calculated_hash.len() == read_hash.len()
            && read_hash
                .iter()
                .enumerate()
                .all(|(i, x)| *x == calculated_hash[i]))
            && self.index.verify(hasher)?)
    }

    /// Read `len` packets from disk starting at `index`. If `len` packets do not exist starting
    /// from `index`, reads as many as possible.
    ///
    /// Returns the next index at which to start reading next time, which is `None` if we read till
    /// the very end. The next segment when not `None` will be `index + len`.
    ///
    /// Returns error only in case of disk I/O error.
    #[inline]
    pub(crate) fn readv(
        &self,
        index: u64,
        len: u64,
        out: &mut Vec<Bytes>,
    ) -> io::Result<Option<u64>> {
        if index >= self.index.entries() {
            return Ok(None);
        }
        let (offsets, next_index) = self.index.readv(index, len)?;
        if self.segment.readv(offsets, out)?.is_none() {
            // TODO: this case is not documented
            log::warn!("information on index files mismatches to that on segment file");
            return Ok(None);
        }
        Ok(next_index)
    }

    /// Total number of packet appended.
    #[inline(always)]
    pub(crate) fn entries(&self) -> u64 {
        self.index.entries()
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use pretty_assertions::assert_eq;
    use sha2::Sha256;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn new_and_read_chunk() {
        pretty_env_logger::init();
        let dir = tempdir().unwrap();
        let mut hasher = Sha256::new();

        let mut v = Vec::with_capacity(20);
        for i in 0..20u8 {
            v.push(Bytes::from(vec![i; 1024]));
        }

        let chunk = Chunk::new(dir.path(), 0, v, &mut hasher).unwrap();
        assert!(chunk.verify(&mut hasher).unwrap());
        assert_eq!(chunk.segment_size(), 20 * 1024);

        for i in 0..20u8 {
            let mut o = Vec::new();
            if let Some(next) = chunk.readv(i as u64, 1, &mut o).unwrap() {
                assert_eq!(next, i as u64 + 1);
            }

            let byte = o.get(0).unwrap();
            assert_eq!(byte.len(), 1024);
            assert_eq!(byte[0], i);
            assert_eq!(byte[1023], i);
        }

        let mut o = Vec::new();
        let v = chunk.readv(20, 1, &mut o).unwrap();
        assert!(v.is_none());
        assert!(o.pop().is_none());

        let mut out = Vec::with_capacity(chunk.entries() as usize);
        let v = chunk.readv(0, chunk.entries(), &mut out).unwrap();
        assert!(chunk.readv(20, 1, &mut out).unwrap().is_none());

        assert!(v.is_none());
        for (i, byte) in out.into_iter().enumerate() {
            assert_eq!(byte.len(), 1024);
            assert_eq!(byte[0], i as u8);
            assert_eq!(byte[1023], i as u8);
        }
    }

    #[test]
    fn open_and_read_chunk() {
        let dir = tempdir().unwrap();
        let mut hasher = Sha256::new();

        let mut v = Vec::with_capacity(20);
        for i in 0..20u8 {
            v.push(Bytes::from(vec![i; 1024]));
        }

        let chunk = Chunk::new(dir.path(), 0, v, &mut hasher).unwrap();
        assert!(chunk.verify(&mut hasher).unwrap());
        assert_eq!(chunk.segment_size(), 20 * 1024);

        drop(chunk);

        let chunk = Chunk::open(dir.path(), 0).unwrap();
        assert!(chunk.verify(&mut hasher).unwrap());
        assert_eq!(chunk.segment_size(), 20 * 1024);

        for i in 0..20u8 {
            let mut o = Vec::new();
            if let Some(next) = chunk.readv(i as u64, 1, &mut o).unwrap() {
                assert_eq!(next, i as u64 + 1);
            }

            let byte = o.get(0).unwrap();
            assert_eq!(byte.len(), 1024);
            assert_eq!(byte[0], i);
            assert_eq!(byte[1023], i);
        }

        let mut o = Vec::new();
        let v = chunk.readv(20, 1, &mut o).unwrap();
        assert!(v.is_none());
        assert!(o.pop().is_none());

        let mut out = Vec::with_capacity(chunk.entries() as usize);
        chunk.readv(0, chunk.entries(), &mut out).unwrap();
        assert_eq!(chunk.readv(20, 1, &mut out).unwrap(), None);

        for (i, byte) in out.into_iter().enumerate() {
            assert_eq!(byte.len(), 1024);
            assert_eq!(byte[0], i as u8);
            assert_eq!(byte[1023], i as u8);
        }
    }
}
