use std::{
    collections::VecDeque,
    fs, io,
    path::{Path, PathBuf},
};

use fnv::FnvHashMap;
use sha2::{Digest, Sha256};

pub mod chunk;
mod index;
mod segment;

use super::segment::{Segment, SegmentType};
use chunk::Chunk;

/// A wrapper around all index and segment files on the disk.
#[allow(dead_code)]
pub(super) struct DiskHandler {
    /// Directory in which to store files in.
    pub(crate) dir: PathBuf,
    /// Files with invalid names. The file can be an index file or segment file, but was put in
    /// this because either the name can not be parsed as an offset of the
    invalid_names: Vec<PathBuf>,
    /// The hasher for segment files
    pub(crate) hasher: Sha256,
}

/// Enum for specifying whether a given file is valid or not. Used only in `DiskHandler::chunks`
/// for keeping track of state of open files.
#[derive(Debug)]
pub enum CommitFile {
    Valid(Chunk),
    Invalid(InvalidType),
}

/// Enum which specifies all sort of invalid cases that can occur when reading segment-index pair
/// from the directory provided.
#[derive(Debug, Clone)]
pub enum InvalidType {
    /// The file at this index does not exist, or even if exists it's file extension might not
    /// match.
    NoExist,
    /// Unable to open the index/segment file.
    NoOpen,
    /// There is no index for the given index, but there is a segment file.
    NoIndex,
    /// There is no segment file for the given index, but there is an index file.
    NoSegment,
    /// The hash from index file does not match that which we get after hashing the segment file.
    InvalidChecksum,
}

struct FileStatus {
    index_found: bool,
    segment_found: bool,
}

type DiskMetadata = (Vec<u64>, FnvHashMap<u64, FileStatus>, Vec<PathBuf>);

//TODO: Review all unwraps
impl DiskHandler {
    /// Create a new disk handler. Reads the given directory for previously existing index-segment
    /// pairs.
    ///
    /// Returns the tupple `(disk_head, disk_tail + 1, absolute_offset, DiskHandler)`
    pub(super) fn new<P: AsRef<Path>>(
        dir: P,
        segments: &mut VecDeque<Segment>,
    ) -> io::Result<(u64, u64, u64, Self)> {
        // creating and reading given dir
        let _ = fs::create_dir_all(&dir)?;
        let (indices, statuses, invalid_names) = DiskHandler::reconstruct_disk_metadata(&dir)?;
        let mut hasher = Sha256::new();

        let (head, tail) = if let Some(last) = indices.last() {
            (*indices.first().unwrap(), *last + 1)
        } else {
            // start from 0 if no files were found to exist in the given directory
            return Ok((
                0, // head
                0, // tail
                0, // absolute_offset
                Self {
                    dir: dir.as_ref().into(),
                    invalid_names,
                    hasher,
                },
            ));
        };

        let absolute_offset =
            DiskHandler::reconstruct_segments(&dir, segments, statuses, &mut hasher, head, tail);

        Ok((
            head,
            tail,
            absolute_offset,
            Self {
                dir: dir.as_ref().into(),
                invalid_names,
                hasher,
            },
        ))
    }

    fn reconstruct_segments<P: AsRef<Path>>(
        dir: &P,
        segments: &mut VecDeque<Segment>,
        statuses: FnvHashMap<u64, FileStatus>,
        hasher: &mut Sha256,
        head: u64,
        tail: u64,
    ) -> u64 {
        // only incremented when valid segment found
        let mut absolute_offset = 0;

        for i in head..tail {
            if let Some(status) = statuses.get(&i) {
                if !status.index_found {
                    segments.push_back(Segment {
                        inner: SegmentType::Disk(CommitFile::Invalid(InvalidType::NoIndex)),
                        absolute_offset,
                    });
                    continue;
                }

                if !status.segment_found {
                    segments.push_back(Segment {
                        inner: SegmentType::Disk(CommitFile::Invalid(InvalidType::NoSegment)),
                        absolute_offset,
                    });
                    continue;
                }

                match Chunk::open(&dir, i) {
                    Ok(chunk) => match chunk.verify(hasher) {
                        Ok(true) => {
                            let segment_len = chunk.entries();
                            segments.push_back(Segment {
                                inner: SegmentType::Disk(CommitFile::Valid(chunk)),
                                absolute_offset,
                            });
                            absolute_offset += segment_len;
                        }
                        Ok(false) => {
                            warn!("invalid checksum on segment index {}", i);
                            segments.push_back(Segment {
                                inner: SegmentType::Disk(CommitFile::Invalid(
                                    InvalidType::InvalidChecksum,
                                )),
                                absolute_offset,
                            });
                        }
                        Err(e) => {
                            warn!("unable to open file at index {} : {:?}", i, e);
                            segments.push_back(Segment {
                                inner: SegmentType::Disk(CommitFile::Invalid(InvalidType::NoOpen)),
                                absolute_offset,
                            });
                        }
                    },
                    Err(e) => {
                        warn!("unable to open file at index {} : {:?}", i, e);
                        segments.push_back(Segment {
                            inner: SegmentType::Disk(CommitFile::Invalid(InvalidType::NoOpen)),
                            absolute_offset,
                        })
                    }
                }
            } else {
                segments.push_back(Segment {
                    inner: SegmentType::Disk(CommitFile::Invalid(InvalidType::NoExist)),
                    absolute_offset,
                });
            }
        }

        absolute_offset
    }

    fn reconstruct_disk_metadata<P: AsRef<Path>>(
        dir: &P,
    ) -> io::Result<DiskMetadata> {
        let files = fs::read_dir(&dir)?;

        let mut indices = Vec::new();
        let mut statuses: FnvHashMap<u64, FileStatus> = FnvHashMap::default();
        let mut invalid_names = Vec::new();

        for file in files {
            let path = match file {
                Ok(file) => file.path(),
                Err(_) => continue,
            };

            let file_index = match path.file_stem() {
                Some(s) => s.to_str().unwrap(),
                None => {
                    invalid_names.push(path);
                    continue;
                }
            };

            let offset = match file_index.parse::<u64>() {
                Ok(n) => n,
                Err(_) => {
                    invalid_names.push(path);
                    continue;
                }
            };

            // push to `indices` if encountering the `offset` for very first time. if extension not
            // 'index' or 'segment' then push path to `invalid_names`.
            // TODO: is this unwrap fine?
            match path.extension() {
                Some(s) if s == "index" => {
                    if let Some(status) = statuses.get_mut(&offset) {
                        status.index_found = true;
                    } else {
                        indices.push(offset);
                        statuses.insert(
                            offset,
                            FileStatus {
                                index_found: true,
                                segment_found: false,
                            },
                        );
                    }
                }
                Some(s) if s == "segment" => {
                    if let Some(status) = statuses.get_mut(&offset) {
                        status.segment_found = true;
                    } else {
                        indices.push(offset);
                        statuses.insert(
                            offset,
                            FileStatus {
                                index_found: false,
                                segment_found: true,
                            },
                        );
                    }
                }
                _ => invalid_names.push(path),
            }
        }

        // getting the head and tail
        indices.sort_unstable();
        Ok((indices, statuses, invalid_names))
    }

    pub(super) fn delete(&self, index: u64) -> io::Result<()> {
        let index_path = self.dir.join(&format!("{:020}.index", index));
        let segment_path = self.dir.join(&format!("{:020}.segment", index));
        fs::remove_file(index_path)?;
        fs::remove_file(segment_path)
    }

    /// Retrieve the invalid files (see [`crate::disk::InvalidType`]).
    #[allow(dead_code)]
    #[inline]
    pub(super) fn invalid_names(&self) -> &Vec<PathBuf> {
        &self.invalid_names
    }
}
