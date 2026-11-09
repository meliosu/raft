//! Durable term/vote storage and a checksummed, append-only log mutation journal.
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::Mutex,
};

use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use serde::{Deserialize, Serialize};

use crate::replication::{Entry, ReplicatedLog, StoredEntry};

// Enough for 64 maximum-sized canonical commands plus JSON record overhead,
// including wire commands whose numeric spelling expands during normalization.
const MAX_FRAME_BYTES: usize = 2 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Mutation {
    from: u64,
    entries: Vec<StoredEntry>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    fn entries() -> Vec<Entry> {
        vec![
            Entry {
                term: 1,
                command: None,
            },
            Entry {
                term: 2,
                command: Some(Value::Null),
            },
            Entry {
                term: 2,
                command: Some(json!({"hello": "world"})),
            },
        ]
    }
    use serde_json::Value;

    fn prepare(dir: &std::path::Path) -> Storage {
        let (storage, _, _) = Storage::open(dir.to_path_buf(), 1, vec![1, 2, 3]).unwrap();
        storage
            .save(&HardState {
                current_term: 3,
                voted_for: Some(2),
            })
            .unwrap();
        storage
    }

    #[test]
    fn replays_durable_entries_and_atomic_suffix_replacements() {
        let dir = tempdir().unwrap();
        let storage = prepare(dir.path());
        storage.append(1, &entries()).unwrap();
        let replacement = Entry {
            term: 3,
            command: Some(json!("replacement")),
        };
        storage
            .append(3, std::slice::from_ref(&replacement))
            .unwrap();
        drop(storage);
        let (_, hard, log) = Storage::open(dir.path().into(), 1, vec![3, 2, 1]).unwrap();
        assert_eq!(
            hard,
            HardState {
                current_term: 3,
                voted_for: Some(2)
            }
        );
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.entry(2).unwrap().command, Some(Value::Null));
        assert_eq!(log.entry(3), Some(&replacement));
        assert_eq!(log.commit_index, 0); // Raft commitIndex is volatile.
    }

    #[test]
    fn recovers_every_incomplete_final_frame_without_partial_truncation() {
        let source = tempdir().unwrap();
        let storage = prepare(source.path());
        storage.append(1, &entries()).unwrap();
        let prefix = fs::read(source.path().join("log.journal")).unwrap();
        storage
            .append(
                2,
                &[Entry {
                    term: 3,
                    command: Some(json!("new")),
                }],
            )
            .unwrap();
        let complete = fs::read(source.path().join("log.journal")).unwrap();
        for cut in prefix.len()..complete.len() {
            let dir = tempdir().unwrap();
            let storage = prepare(dir.path());
            drop(storage);
            fs::write(dir.path().join("log.journal"), &complete[..cut]).unwrap();
            let (storage, _, log) = Storage::open(dir.path().into(), 1, vec![1, 2, 3]).unwrap();
            assert_eq!(log.last_index(), 3, "cut={cut}");
            assert_eq!(log.entry(2).unwrap().command, Some(Value::Null));
            assert_eq!(
                fs::metadata(dir.path().join("log.journal")).unwrap().len(),
                prefix.len() as u64
            );
            // The file offset must be reset after recovery, not left past EOF.
            storage
                .append(
                    4,
                    &[Entry {
                        term: 3,
                        command: None,
                    }],
                )
                .unwrap();
            drop(storage);
            assert_eq!(
                Storage::open(dir.path().into(), 1, vec![1, 2, 3])
                    .unwrap()
                    .2
                    .last_index(),
                4
            );
        }
    }

    #[test]
    fn rejects_complete_corruption_including_length_header() {
        for byte in [0, 4, 8, 12] {
            let dir = tempdir().unwrap();
            let storage = prepare(dir.path());
            storage.append(1, &entries()).unwrap();
            drop(storage);
            let path = dir.path().join("log.journal");
            let mut bytes = fs::read(&path).unwrap();
            bytes[byte] ^= 1;
            fs::write(path, bytes).unwrap();
            assert!(Storage::open(dir.path().into(), 1, vec![1, 2, 3]).is_err());
        }
    }

    #[test]
    fn preserves_legacy_hard_state_and_rejects_identity_changes_and_concurrent_use() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("hard-state.json"),
            br#"{"node_id":1,"members":[1,2,3],"hard_state":{"current_term":7,"voted_for":2}}"#,
        )
        .unwrap();
        let (storage, hard, log) = Storage::open(dir.path().into(), 1, vec![1, 2, 3]).unwrap();
        assert_eq!(hard.current_term, 7);
        assert_eq!(hard.voted_for, Some(2));
        assert_eq!(log.last_index(), 0);
        assert!(Storage::open(dir.path().into(), 1, vec![1, 2, 3]).is_err());
        drop(storage);
        assert!(Storage::open(dir.path().into(), 2, vec![1, 2, 3]).is_err());
        assert!(Storage::open(dir.path().into(), 1, vec![1, 2]).is_err());
    }

    #[test]
    fn rejects_corrupt_hard_state_and_log_term_ahead_of_durable_term() {
        let dir = tempdir().unwrap();
        let storage = prepare(dir.path());
        storage
            .append(
                1,
                &[Entry {
                    term: 4,
                    command: None,
                }],
            )
            .unwrap();
        drop(storage);
        assert!(Storage::open(dir.path().into(), 1, vec![1, 2, 3]).is_err());
        fs::write(dir.path().join("hard-state.json"), b"invalid").unwrap();
        assert!(Storage::open(dir.path().into(), 1, vec![1, 2, 3]).is_err());
    }

    #[test]
    fn propagates_journal_write_and_hard_state_errors() {
        let dir = tempdir().unwrap();
        let storage = prepare(dir.path());
        *storage.journal.lock().unwrap() = File::open(dir.path().join("log.journal")).unwrap();
        assert!(storage.append(1, &entries()).is_err());
        fs::create_dir(dir.path().join("hard-state.tmp")).unwrap();
        assert!(storage.save(&HardState::default()).is_err());
    }

    #[test]
    fn missing_hard_state_with_existing_log_is_not_silently_reinitialized() {
        let dir = tempdir().unwrap();
        let storage = prepare(dir.path());
        storage.append(1, &entries()).unwrap();
        drop(storage);
        let hard_state = dir.path().join("hard-state.json");
        fs::remove_file(&hard_state).unwrap();
        assert!(Storage::open(dir.path().into(), 1, vec![1, 2, 3]).is_err());
        assert!(!hard_state.exists());
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HardState {
    pub current_term: u64,
    pub voted_for: Option<u64>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    node_id: u64,
    members: Vec<u64>,
    hard_state: HardState,
}

pub struct Storage {
    dir: PathBuf,
    node_id: u64,
    members: Vec<u64>,
    journal: Mutex<File>,
    // Hold the OS lock for the entire node lifetime.
    _lock: File,
}

impl Storage {
    pub fn open(
        dir: PathBuf,
        node_id: u64,
        mut members: Vec<u64>,
    ) -> Result<(Self, HardState, ReplicatedLog)> {
        // Persist newly created directory entries as well as the files inside
        // them; syncing only the leaf directory does not persist its name.
        let dir = std::path::absolute(dir)?;
        let mut missing_parents = Vec::new();
        let mut ancestor = dir.as_path();
        while !ancestor.try_exists()? {
            let parent = ancestor.parent().context("data directory has no parent")?;
            missing_parents.push(parent.to_path_buf());
            ancestor = parent;
        }
        fs::create_dir_all(&dir).context("creating data directory")?;
        for parent in missing_parents.iter().rev() {
            File::open(parent)?.sync_all()?;
        }
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join("LOCK"))?;
        lock.try_lock_exclusive()
            .context("data directory is already in use")?;
        members.sort_unstable();
        let journal = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join("log.journal"))?;
        journal.sync_all()?;
        File::open(&dir)?.sync_all()?;
        let storage = Self {
            dir,
            node_id,
            members,
            journal: Mutex::new(journal),
            _lock: lock,
        };
        let state = match fs::read(storage.dir.join("hard-state.json")) {
            Ok(bytes) => {
                let record: Record = serde_json::from_slice(&bytes)
                    .context("corrupt hard state; refusing to reset term/vote")?;
                ensure!(
                    record.node_id == node_id && record.members == storage.members,
                    "data directory belongs to a different node or membership"
                );
                if let Some(vote) = record.hard_state.voted_for {
                    ensure!(
                        record.hard_state.current_term > 0 && storage.members.contains(&vote),
                        "invalid persisted vote"
                    );
                }
                record.hard_state
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                ensure!(
                    storage
                        .journal
                        .lock()
                        .map_err(|_| anyhow::anyhow!("journal lock poisoned"))?
                        .metadata()?
                        .len()
                        == 0,
                    "missing hard state for existing log; refusing to reset term/vote"
                );
                let state = HardState::default();
                storage.save(&state)?;
                state
            }
            Err(error) => return Err(error).context("reading hard state"),
        };
        let log = storage.recover()?;
        ensure!(
            log.term(log.last_index()).unwrap() <= state.current_term,
            "log term exceeds durable current term"
        );
        Ok((storage, state, log))
    }

    pub fn save(&self, state: &HardState) -> Result<()> {
        let record = Record {
            node_id: self.node_id,
            members: self.members.clone(),
            hard_state: state.clone(),
        };
        let bytes = serde_json::to_vec(&record)?;
        let temporary = self.dir.join("hard-state.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(temporary, self.dir.join("hard-state.json"))?;
        // Persist the rename too; file sync alone does not survive all power failures.
        File::open(&self.dir)?.sync_all()?;
        Ok(())
    }

    /// A single frame records both truncation and replacement. A crash cannot
    /// expose a truncated prefix without its replacement entries.
    pub fn append(&self, from: u64, entries: &[Entry]) -> Result<()> {
        let mutation = Mutation {
            from,
            entries: entries.iter().map(StoredEntry::from).collect(),
        };
        let bytes = serde_json::to_vec(&mutation)?;
        ensure!(bytes.len() <= MAX_FRAME_BYTES, "log mutation too large");
        let length = (bytes.len() as u32).to_le_bytes();
        let mut header = [0u8; 12];
        header[..4].copy_from_slice(&length);
        header[4..8].copy_from_slice(&crc32fast::hash(&length).to_le_bytes());
        header[8..12].copy_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
        let mut journal = self
            .journal
            .lock()
            .map_err(|_| anyhow::anyhow!("journal lock poisoned"))?;
        journal.write_all(&header)?;
        journal.write_all(&bytes)?;
        journal.sync_all().context("syncing log journal")?;
        Ok(())
    }

    fn recover(&self) -> Result<ReplicatedLog> {
        let mut journal = self
            .journal
            .lock()
            .map_err(|_| anyhow::anyhow!("journal lock poisoned"))?;
        journal.seek(SeekFrom::Start(0))?;
        let total = journal.metadata()?.len();
        let mut offset = 0;
        let mut log = ReplicatedLog::default();
        while total - offset >= 12 {
            let mut header = [0u8; 12];
            journal.read_exact(&mut header)?;
            ensure!(
                crc32fast::hash(&header[..4])
                    == u32::from_le_bytes(header[4..8].try_into().unwrap()),
                "corrupt log frame length at offset {offset}"
            );
            let length = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
            ensure!(
                length > 0 && length <= MAX_FRAME_BYTES,
                "invalid log frame length"
            );
            if total - offset - 12 < length as u64 {
                break;
            }
            let mut bytes = vec![0; length];
            journal.read_exact(&mut bytes)?;
            ensure!(
                crc32fast::hash(&bytes) == u32::from_le_bytes(header[8..12].try_into().unwrap()),
                "corrupt log frame at offset {offset}"
            );
            let mutation: Mutation =
                serde_json::from_slice(&bytes).context("invalid log mutation")?;
            let entries = mutation
                .entries
                .into_iter()
                .map(StoredEntry::into_entry)
                .collect::<Result<Vec<_>>>()?;
            log.replace(mutation.from, entries)
                .context("replaying log mutation")?;
            offset += 12 + length as u64;
        }
        // Only an incomplete final frame can be discarded. Complete corrupt
        // frames are fatal, never interpreted as an empty log or torn write.
        if offset != total {
            journal.set_len(offset)?;
            journal.sync_all()?;
        }
        journal.seek(SeekFrom::End(0))?;
        Ok(log)
    }
}
