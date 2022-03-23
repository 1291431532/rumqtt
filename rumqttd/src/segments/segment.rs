use std::{io, path::Path};

use bytes::Bytes;
use sha2::Digest;

use super::disk::InvalidType;
use super::disk::{chunk::Chunk, CommitFile};
use super::memory::segment::MemorySegment;

pub(crate) struct Segment {
    /// Holds the actual segment.
    pub(crate) inner: SegmentType,
    /// The absolute offset at which the `inner` starts at. All reads will return the absolute
    /// offset as the offset of the cursor.
    ///
    /// **NOTE**: this offset is re-generated on each run of the commit log.
    pub(crate) absolute_offset: u64,
}

#[derive(Debug)]
pub(crate) enum SegmentType {
    Disk(CommitFile),
    Memory(MemorySegment),
}

pub(crate) enum SegmentPosition {
    /// When the returned absolute offset exists within the current segment
    Next(u64),
    /// When the the returned absolute offset does not exist within the current segment, but
    /// instead is 1 beyond the highest absolute offset in this segment, meant for use with next
    /// segment if exists
    Done(u64),
}

impl Segment {
    #[inline]
    pub(crate) fn next_absolute_offset(&self) -> u64 {
        self.len() + self.absolute_offset
    }

    #[inline]
    /// Takes in the abosolute index to start reading from. Internally handles the conversion from
    /// relative offset to absolute offset and vice-versa.
    pub(crate) fn readv(
        &self,
        absolute_index: u64,
        len: u64,
        out: &mut Vec<Bytes>,
    ) -> io::Result<SegmentPosition> {
        // this substraction can never overflow as checking of offset happens at
        // `CommitLog::readv`.
        let idx = absolute_index - self.absolute_offset;

        match &self.inner {
            SegmentType::Disk(CommitFile::Valid(chunk)) => match chunk.readv(idx, len, out)? {
                Some(relative_offset) => {
                    Ok(SegmentPosition::Next(self.absolute_offset + relative_offset))
                }
                None => Ok(SegmentPosition::Done(self.next_absolute_offset())),
            },
            SegmentType::Memory(segment) => match segment.readv(idx, len, out) {
                Some(relative_offset) => {
                    Ok(SegmentPosition::Next(self.absolute_offset + relative_offset))
                }
                None => Ok(SegmentPosition::Done(self.next_absolute_offset())),
            },
            _ => Ok(SegmentPosition::Done(self.next_absolute_offset())),
        }
    }

    #[inline]
    pub(crate) fn len(&self) -> u64 {
        match &self.inner {
            SegmentType::Disk(CommitFile::Valid(chunk)) => chunk.entries(),
            SegmentType::Memory(segment) => segment.len(),
            _ => 0,
        }
    }

    #[inline]
    pub(crate) fn size(&self) -> u64 {
        match &self.inner {
            SegmentType::Disk(CommitFile::Valid(chunk)) => chunk.segment_size(),
            SegmentType::Memory(segment) => segment.size(),
            _ => 0,
        }
    }

    #[inline]
    pub(crate) fn move_to_disk<P: AsRef<Path>>(
        &mut self,
        dir: P,
        index: u64,
        hasher: &mut impl Digest,
    ) -> io::Result<()> {
        self.inner.move_to_disk(dir, index, hasher)
    }
}

impl SegmentType {
    /// Saves the current segment on disk. If already a disk type (valid or not), will return
    /// without error. If saving on memory failed then data is destroyed regardless (TODO: maybe we
    /// should not destoy the data).
    #[inline]
    fn move_to_disk<P: AsRef<Path>>(
        &mut self,
        dir: P,
        index: u64,
        hasher: &mut impl Digest,
    ) -> io::Result<()> {
        let dummy_swap_init = SegmentType::Disk(CommitFile::Invalid(InvalidType::NoOpen));
        match std::mem::replace(self, dummy_swap_init) {
            SegmentType::Memory(MemorySegment { data, .. }) => {
                // Package data of memory segment into a disk segment
                let chunk = Chunk::new(dir, index, data, hasher)?;
                *self = SegmentType::Disk(CommitFile::Valid(chunk));
                Ok(())
            }
            SegmentType::Disk(_) => {
                // SegmentType was already disk type. No change happens
                warn!("Moving a disk type at index {} to disk again", index);
                Ok(())
            }
        }
    }
}
