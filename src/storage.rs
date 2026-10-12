//! Durable term/vote storage, separate from the future replicated log.
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::PathBuf,
};

use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use serde::{Deserialize, Serialize};

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
    // Hold the OS lock for the entire node lifetime.
    _lock: File,
}

impl Storage {
    pub fn open(dir: PathBuf, node_id: u64, mut members: Vec<u64>) -> Result<(Self, HardState)> {
        fs::create_dir_all(&dir).context("creating data directory")?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join("LOCK"))?;
        lock.try_lock_exclusive()
            .context("data directory is already in use")?;
        members.sort_unstable();
        let storage = Self {
            dir,
            node_id,
            members,
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
                let state = HardState::default();
                storage.save(&state)?;
                state
            }
            Err(error) => return Err(error).context("reading hard state"),
        };
        Ok((storage, state))
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
}
