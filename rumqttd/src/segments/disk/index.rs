use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    mem::{transmute, MaybeUninit},
    path::Path,
};

use sha2::Digest;

/// Size of the offset of packet, in bytes.
const OFFSET_SIZE: u64 = 8;
/// Size of the len of packet, in bytes.
const LEN_SIZE: u64 = 8;
/// Size of the hash of segment file, stored at the start of index file.
const HASH_SIZE: u64 = 32;
/// Size of entry, in bytes.
const ENTRY_SIZE: u64 = OFFSET_SIZE + LEN_SIZE;

/// Wrapper around a index file for convenient reading of bytes sizes. The only case any operation
/// fails is when a disk I/O error occurs. In case of reading, when demands exceed what is
/// available, this only reads what we can and notify how much of the requested info has not been
/// retrieved.
///
/// ### Index file format
///
/// The index file starts with the 32-bytes hash of the segment file, followed by entries. Each
/// entry consists of 2 u64s, [  offset  |    len    ].
/// Only case this can fail is when there is an I/O error.
#[derive(Debug)]
pub(super) struct Index {
    /// The opened index file.
    file: File,
    /// Number of entries in the index file.
    entries: u64,
}

impl Index {
    /// Open a new index file. Does not create a new one, and throws error if does not exist.
    ///
    /// Note that index file is opened immutably.
    #[inline]
    pub(super) fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let entries = (file.metadata()?.len() - (HASH_SIZE * 2)) / ENTRY_SIZE;

        let index = Self { file, entries };

        Ok(index)
    }

    #[cfg(test)]
    pub(super) fn new_with_hash<P: AsRef<Path>>(
        path: P,
        segments_hash: &[u8],
        index_hash: &[u8],
        lens: Vec<u64>,
    ) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let tail = lens.len() as u64;
        let mut offset = 0;

        let entries: Vec<u8> = lens
            .into_iter()
            .map(|len| {
                let ret = [offset, len];
                offset += len;
                // SAFETY: we will read back from file in exact same manner. as representation will
                // remain same, we don't need to change the length of vec either.
                unsafe { transmute::<[u64; 2], [u8; 16]>(ret) }
            })
            .flatten()
            .collect();

        file.write_all(&index_hash[..32])?;
        file.write_all(&segments_hash[..32])?;
        file.write_all(&entries[..])?;

        Ok(Self {
            file,
            entries: tail,
        })
    }

    /// Create a new index file. Throws error if does not exist.
    /// Note that index file is opened immutably, after writing the given data.
    /// It only takes first 32 bits of the hash provided, and the self-hash happens only over the
    /// entries and not the segment's hash.
    ///
    /// ### Index File Format
    ///
    /// First 32 bytes (0-31) are the hash of the entries of the index file that are given as argument
    /// `lens`.
    ///
    /// Bytes from 32-63 are the hash of segments file which is passed along as the `segments_hash`
    /// argument.
    ///
    /// Remaining are the entries of the format `[offset, len]`.
    pub(super) fn new<P: AsRef<Path>>(
        path: P,
        segments_hash: &[u8],
        hasher: &mut impl Digest,
        lens: Vec<u64>,
    ) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let tail = lens.len() as u64;
        let mut offset = 0;

        let entries: Vec<u8> = lens
            .into_iter()
            .flat_map(|len| {
                let ret = [offset, len];
                offset += len;
                // SAFETY: we will read back from file in exact same manner. as representation will
                // remain same, we don't need to change the length of vec either.
                unsafe { transmute::<[u64; 2], [u8; 16]>(ret) }
            })
            .collect();

        hasher.update(&entries);
        let self_hash = hasher.finalize_reset();

        file.write_all(&self_hash[..32])?;
        file.write_all(&segments_hash[..32])?;
        file.write_all(&entries[..])?;

        Ok(Self {
            file,
            entries: tail,
        })
    }

    /// Return the number of entries in the index.
    #[inline]
    pub(super) fn entries(&self) -> u64 {
        self.entries
    }

    /// Read the hash stored in the index file, which is the starting 32 bytes of the file.
    #[inline]
    pub(super) fn read_hash(&self) -> io::Result<[u8; 32]> {
        let mut buf: [u8; 32] = unsafe { MaybeUninit::uninit().assume_init() };
        self.read_at(&mut buf, HASH_SIZE)?;
        Ok(buf)
    }

    // Verify the hash of the index file itself. Note that the hashing happens only over the
    // entries and not over the stored segment's hash.
    #[inline]
    pub(super) fn verify(&self, hasher: &mut impl Digest) -> io::Result<bool> {
        let mut read_index_hash = [0; 32];
        self.read_at(&mut read_index_hash, 0)?;

        let mut buf = vec![0u8; (self.entries * ENTRY_SIZE) as usize];
        self.read_at(buf.as_mut(), 2 * HASH_SIZE)?;

        hasher.update(&buf);
        let calculated_index_hash = hasher.finalize_reset();

        Ok(read_index_hash.len() == calculated_index_hash.len()
            && calculated_index_hash
                .into_iter()
                .zip(read_index_hash.iter())
                .all(|(actual, read)| actual == *read))
    }

    /// Get a vector of 2-arrays which have the offset and the size of the `len` packets, starting
    /// at the `index`. If `len` is larger than number of packets stored in segment, it will read
    /// as much as possible. Also returns the next index to start reading from, which is `None` if
    /// we reached the end of index file.
    ///
    /// Note that the next index returned, if not `None`, will be `index + len`.
    #[inline]
    pub(super) fn readv(&self, index: u64, len: u64) -> io::Result<(Vec<[u64; 2]>, Option<u64>)> {
        let limit = index + len;
        let (next_index, read_len) = if limit >= self.entries {
            // if index + len beyond total entries, notify that some will be left and read only
            // what total number of entries will allow
            (None, ((self.entries - index) * ENTRY_SIZE) as usize)
        } else {
            // else read as much as needed.
            (Some(limit), (len * ENTRY_SIZE) as usize)
        };

        // NOTE: each increment in `len`      = ENTRY_SIZE
        //       each increment in `left`     = ENTRY_SIZE
        //       each increment in `index`    = ENTRY_SIZE
        //       each increment in `read_len` = 1 byte

        // not optimizing for keeping array uninit on the basis that compiler will do that for us
        // when compiled with optimizations enabled.
        let mut buf = vec![0u8; read_len];

        self.read_at(buf.as_mut(), HASH_SIZE * 2 + index * ENTRY_SIZE)?;

        // SAFETY: needed beacuse of transmute. As new transmuted type is of different repr, we
        // need to make sure the length stored in vec also matches.
        unsafe {
            buf.set_len(read_len / ENTRY_SIZE as usize);
        }

        // SAFETY: we have written to disk in exact same manner, and vector stores contiguous on
        // heap.
        Ok((
            unsafe { transmute::<Vec<u8>, Vec<[u64; 2]>>(buf) },
            next_index,
        ))
    }

    #[allow(unused_mut)]
    #[inline]
    fn read_at(&self, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
        #[cfg(target_family = "unix")]
        {
            use std::os::unix::prelude::FileExt;
            self.file.read_exact_at(buf, offset)
        }
        #[cfg(target_family = "windows")]
        {
            use std::os::windows::fs::FileExt;
            while !buf.is_empty() {
                match self.seek_read(buf, offset) {
                    Ok(0) => return Ok(()),
                    Ok(n) => {
                        buf = &mut buf[n..];
                        offset += n as u64;
                    }
                    Err(e) => return Err(e),
                }
            }
            if !buf.is_empty() {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ))
            } else {
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod test {
    use pretty_assertions::assert_eq;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn new_and_read_index() {
        let dir = tempdir().unwrap();

        #[rustfmt::skip]
        let index = Index::new_with_hash(
            dir.path().join(format!("{:020}", 2).as_str()),
            &[2; 32],
            &[2; 32],
            vec![100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                 200, 200, 200, 200, 200, 200, 200, 200, 200, 200]
            ).unwrap();

        assert_eq!(index.entries(), 20);

        let (v, next) = index.readv(9, 1).unwrap();
        assert_eq!(next.unwrap(), 10);
        assert_eq!(v.get(0).unwrap(), &[900, 100]);

        let (v, next) = index.readv(19, 1).unwrap();
        assert!(next.is_none());
        assert_eq!(v.get(0).unwrap(), &[2800, 200]);

        assert_eq!(index.read_hash().unwrap(), [2; 32]);

        let (v, _) = index.readv(0, 20).unwrap();
        for i in 0..10 {
            assert_eq!(v[i][0] as usize, 100 * i); // offset
            assert_eq!(v[i][1], 100); // len
        }
        for i in 10..20 {
            assert_eq!(v[i][0] as usize, 1000 + 200 * (i - 10)); // offset
            assert_eq!(v[i][1], 200); // len
        }
    }

    #[test]
    fn open_and_read_index() {
        let dir = tempdir().unwrap();

        #[rustfmt::skip]
        let index = Index::new_with_hash(
            dir.path().join(format!("{:020}", 2).as_str()),
            &[2; 32],
            &[2; 32],
            vec![
                100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                200, 200, 200, 200, 200, 200, 200, 200, 200, 200],
        )
        .unwrap();

        assert_eq!(index.entries(), 20);
        assert_eq!(index.read_hash().unwrap(), [2; 32]);

        drop(index);

        let index = Index::open(dir.path().join(format!("{:020}", 2).as_str())).unwrap();
        let (v, next) = index.readv(19, 1).unwrap();
        assert!(next.is_none());
        assert_eq!(v.get(0).unwrap(), &[2800, 200]);
        assert_eq!(index.read_hash().unwrap(), [2; 32]);

        let (v, next) = index.readv(0, 20).unwrap();
        for i in 0..10 {
            assert_eq!(v[i][0] as usize, 100 * i); // offset
            assert_eq!(v[i][1], 100); // len
        }
        for i in 10..20 {
            assert_eq!(v[i][0] as usize, 1000 + 200 * (i - 10)); // offset
            assert_eq!(v[i][1], 200); // len
        }
        assert!(next.is_none());
    }
}
