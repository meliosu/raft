//! Log matching and bounded wire batches, independent of networking and storage.
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::proto::{AppendEntriesRequest, AppendEntriesResponse, LogEntry};

pub const MAX_COMMAND_BYTES: usize = 16 * 1024;
pub const MAX_COMMAND_DEPTH: usize = 64;
pub const MAX_BATCH_BYTES: usize = 128 * 1024;
pub const MAX_BATCH_ENTRIES: usize = 64;
pub const MAX_RPC_BYTES: usize = 256 * 1024;
pub const MAX_PAGE_ENTRIES: usize = 100;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub term: u64,
    // Outer Option distinguishes a no-op from the JSON command `null`.
    pub command: Option<Value>,
}

pub fn validate_command(command: &Value) -> Result<()> {
    fn check_depth(value: &Value, depth: usize) -> Result<()> {
        ensure!(depth <= MAX_COMMAND_DEPTH, "command nesting too deep");
        match value {
            Value::Array(values) => {
                for value in values {
                    check_depth(value, depth + 1)?;
                }
            }
            Value::Object(values) => {
                for value in values.values() {
                    check_depth(value, depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    check_depth(command, 0)?;
    ensure!(
        command.to_string().len() <= MAX_COMMAND_BYTES,
        "command too large"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn entry(term: u64) -> Entry {
        Entry {
            term,
            command: Some(json!({"term": term})),
        }
    }
    fn log(terms: &[u64]) -> ReplicatedLog {
        let mut log = ReplicatedLog::default();
        if !terms.is_empty() {
            log.replace(1, terms.iter().map(|&term| entry(term)).collect())
                .unwrap();
        }
        log
    }
    fn request(prev: u64, term: u64, terms: &[u64], commit: u64) -> AppendEntriesRequest {
        AppendEntriesRequest {
            term: 10,
            leader_id: 2,
            prev_log_index: prev,
            prev_log_term: term,
            entries: terms.iter().map(|&term| entry(term).wire()).collect(),
            leader_commit: commit,
        }
    }
    fn append(log: &mut ReplicatedLog, request: &AppendEntriesRequest) -> AppendEntriesResponse {
        let plan = log
            .plan_append(request, ReplicatedLog::decode(request).unwrap())
            .unwrap();
        if let Some((from, entries)) = plan.mutation {
            log.replace(from, entries).unwrap();
        }
        log.commit_index = plan.commit_index;
        plan.response
    }

    #[test]
    fn rejects_missing_and_conflicting_prefix_with_hints() {
        let mut log = log(&[1, 2, 2, 3]);
        let missing = append(&mut log, &request(6, 3, &[], 6));
        assert!(!missing.success);
        assert_eq!((missing.conflict_index, missing.conflict_term), (5, 0));
        let conflict = append(&mut log, &request(3, 1, &[], 3));
        assert!(!conflict.success);
        assert_eq!((conflict.conflict_index, conflict.conflict_term), (2, 2));
        assert_eq!(log.commit_index, 0);
        assert_eq!(log.last_index(), 4);
    }

    #[test]
    fn empty_and_short_requests_preserve_unmentioned_suffix_and_bound_commit() {
        let mut log = log(&[1, 2, 3, 3]);
        assert!(append(&mut log, &request(1, 1, &[], 100)).success);
        assert_eq!(log.last_index(), 4);
        assert_eq!(log.commit_index, 1);
        assert!(append(&mut log, &request(1, 1, &[2], 100)).success);
        assert_eq!(log.last_index(), 4);
        assert_eq!(log.commit_index, 2);
        // An older duplicate does not decrease commitIndex.
        append(&mut log, &request(0, 0, &[1], 0));
        assert_eq!(log.commit_index, 2);
    }

    #[test]
    fn repairs_only_conflicting_suffix_and_duplicate_is_idempotent() {
        let mut log = log(&[1, 2, 2, 4, 4]);
        log.commit_index = 2;
        let req = request(2, 2, &[3, 3], 4);
        assert!(append(&mut log, &req).success);
        assert_eq!(
            (1..=4).map(|i| log.term(i).unwrap()).collect::<Vec<_>>(),
            vec![1, 2, 3, 3]
        );
        assert_eq!((log.last_index(), log.commit_index), (4, 4));
        let plan = log
            .plan_append(&req, ReplicatedLog::decode(&req).unwrap())
            .unwrap();
        assert!(plan.response.success);
        assert!(plan.mutation.is_none());
    }

    #[test]
    fn never_overwrites_committed_entries_or_changes_same_term_payload() {
        let mut log = log(&[1, 2]);
        log.commit_index = 2;
        let req = request(1, 1, &[3], 2);
        assert!(
            log.plan_append(&req, ReplicatedLog::decode(&req).unwrap())
                .is_err()
        );
        let mut req = request(1, 1, &[2], 2);
        req.entries[0].command_json = Some("\"different\"".into());
        assert!(
            log.plan_append(&req, ReplicatedLog::decode(&req).unwrap())
                .is_err()
        );
    }

    #[test]
    fn batches_are_bounded_by_count_and_bytes() {
        let mut log = log(&vec![1; MAX_BATCH_ENTRIES + 10]);
        assert_eq!(log.batch(1).len(), MAX_BATCH_ENTRIES);
        assert!(log.batch(log.last_index() + 1).is_empty());
        let command = json!("x".repeat(MAX_COMMAND_BYTES - 2));
        log.replace(
            1,
            vec![
                Entry {
                    term: 1,
                    command: Some(command)
                };
                20
            ],
        )
        .unwrap();
        let batch = log.batch(1);
        assert!(batch.len() < 20 && !batch.is_empty());
        assert!(
            batch
                .iter()
                .map(|e| prost::Message::encoded_len(e) + 8)
                .sum::<usize>()
                <= MAX_BATCH_BYTES
        );
    }

    #[test]
    fn validates_wire_entries_and_index_arithmetic() {
        for req in [
            request(0, 1, &[], 0),
            request(1, 0, &[], 0),
            request(0, 0, &[2, 1], 0),
            request(0, 0, &[0], 0),
            request(u64::MAX, 1, &[1], 0),
            request(0, 0, &vec![1; MAX_BATCH_ENTRIES + 1], 0),
        ] {
            assert!(ReplicatedLog::decode(&req).is_err());
        }
        let mut req = request(0, 0, &[1], 0);
        req.entries[0].command_json = Some("invalid json".into());
        assert!(ReplicatedLog::decode(&req).is_err());
        req.entries[0].command_json = Some(json!("x".repeat(MAX_COMMAND_BYTES)).to_string());
        assert!(ReplicatedLog::decode(&req).is_err());
    }

    #[test]
    fn json_null_is_a_command_not_a_noop() {
        let entry = Entry {
            term: 1,
            command: Some(Value::Null),
        };
        let stored = serde_json::to_vec(&StoredEntry::from(&entry)).unwrap();
        assert_eq!(
            serde_json::from_slice::<StoredEntry>(&stored)
                .unwrap()
                .into_entry()
                .unwrap(),
            entry
        );
        assert_eq!(entry.wire().command_json.as_deref(), Some("null"));
    }

    #[test]
    fn command_depth_is_bounded_below_journal_json_recursion_limit() {
        let mut command = Value::Null;
        for _ in 0..MAX_COMMAND_DEPTH {
            command = Value::Array(vec![command]);
        }
        assert!(validate_command(&command).is_ok());
        let entry = Entry {
            term: 1,
            command: Some(command.clone()),
        };
        let bytes = serde_json::to_vec(&StoredEntry::from(&entry)).unwrap();
        assert_eq!(
            serde_json::from_slice::<StoredEntry>(&bytes)
                .unwrap()
                .into_entry()
                .unwrap(),
            entry
        );
        command = Value::Array(vec![command]);
        assert!(validate_command(&command).is_err());
    }

    #[test]
    fn floating_point_commands_roundtrip_without_changing_log_identity() {
        for number in [
            0.8455124082255701,
            f64::MIN_POSITIVE,
            f64::MAX,
            -0.0,
            1.2345678901234567,
        ] {
            let entry = Entry {
                term: 1,
                command: Some(json!(number)),
            };
            let req = AppendEntriesRequest {
                term: 1,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![entry.wire()],
                leader_commit: 0,
            };
            assert_eq!(ReplicatedLog::decode(&req).unwrap(), vec![entry.clone()]);
            let bytes = serde_json::to_vec(&StoredEntry::from(&entry)).unwrap();
            assert_eq!(
                serde_json::from_slice::<StoredEntry>(&bytes)
                    .unwrap()
                    .into_entry()
                    .unwrap(),
                entry
            );
        }
    }
}

// Serde normally decodes Some(Value::Null) as None. Store the entry kind
// explicitly in the journal so a client null command survives a restart.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredEntry {
    pub term: u64,
    pub noop: bool,
    pub command: Value,
}

impl From<&Entry> for StoredEntry {
    fn from(entry: &Entry) -> Self {
        Self {
            term: entry.term,
            noop: entry.command.is_none(),
            command: entry.command.clone().unwrap_or(Value::Null),
        }
    }
}

impl StoredEntry {
    pub fn into_entry(self) -> Result<Entry> {
        ensure!(!self.noop || self.command.is_null(), "invalid stored no-op");
        Ok(Entry {
            term: self.term,
            command: (!self.noop).then_some(self.command),
        })
    }
}

impl Entry {
    pub fn wire(&self) -> LogEntry {
        LogEntry {
            term: self.term,
            command_json: self.command.as_ref().map(Value::to_string),
        }
    }
}

#[derive(Default)]
pub struct ReplicatedLog {
    entries: Vec<Entry>,
    pub commit_index: u64,
}

pub struct AppendPlan {
    pub response: AppendEntriesResponse,
    pub mutation: Option<(u64, Vec<Entry>)>,
    pub commit_index: u64,
}

impl ReplicatedLog {
    pub fn last_index(&self) -> u64 {
        self.entries.len() as u64
    }

    pub fn term(&self, index: u64) -> Option<u64> {
        if index == 0 {
            Some(0)
        } else {
            self.entry(index).map(|entry| entry.term)
        }
    }

    pub fn entry(&self, index: u64) -> Option<&Entry> {
        usize::try_from(index.checked_sub(1)?)
            .ok()
            .and_then(|i| self.entries.get(i))
    }

    pub fn last_index_of_term(&self, term: u64) -> Option<u64> {
        // Terms are nondecreasing, so even a very long conflicting term takes
        // logarithmic work rather than scanning the entire retained log.
        let end = self.entries.partition_point(|entry| entry.term <= term);
        (end > 0 && self.entries[end - 1].term == term).then_some(end as u64)
    }

    /// Also used during journal replay. Never replace an already committed entry.
    pub fn replace(&mut self, from: u64, entries: Vec<Entry>) -> Result<()> {
        ensure!(
            from > self.commit_index && from <= self.last_index() + 1,
            "invalid log suffix replacement or overwrite of committed entry"
        );
        ensure!(!entries.is_empty(), "empty log mutation");
        let mut previous = self.term(from - 1).unwrap();
        for entry in &entries {
            ensure!(
                entry.term > 0 && entry.term >= previous,
                "invalid log terms"
            );
            if let Some(command) = &entry.command {
                validate_command(command)?;
            }
            previous = entry.term;
        }
        self.entries.truncate((from - 1) as usize);
        self.entries.extend(entries);
        Ok(())
    }

    pub fn batch(&self, from: u64) -> Vec<LogEntry> {
        let mut bytes = 0;
        let mut batch = Vec::new();
        for index in from..=self.last_index() {
            let entry = self.entry(index).unwrap().wire();
            let size = prost::Message::encoded_len(&entry) + 8;
            if batch.len() == MAX_BATCH_ENTRIES || bytes + size > MAX_BATCH_BYTES {
                break;
            }
            bytes += size;
            batch.push(entry);
        }
        batch
    }

    pub fn decode(request: &AppendEntriesRequest) -> Result<Vec<Entry>> {
        ensure!(
            request.entries.len() <= MAX_BATCH_ENTRIES,
            "too many entries"
        );
        ensure!(
            prost::Message::encoded_len(request) <= MAX_RPC_BYTES,
            "RPC too large"
        );
        ensure!(
            (request.prev_log_index == 0) == (request.prev_log_term == 0),
            "invalid previous entry"
        );
        ensure!(
            request.prev_log_term <= request.term,
            "previous term exceeds leader term"
        );
        request
            .prev_log_index
            .checked_add(request.entries.len() as u64)
            .filter(|last| *last < u64::MAX)
            .ok_or_else(|| anyhow::anyhow!("log index overflow"))?;
        let mut previous = request.prev_log_term;
        request
            .entries
            .iter()
            .map(|entry| {
                ensure!(
                    entry.term > 0 && entry.term >= previous && entry.term <= request.term,
                    "invalid entry term"
                );
                previous = entry.term;
                let command = entry
                    .command_json
                    .as_ref()
                    .map(|json| {
                        ensure!(json.len() <= MAX_COMMAND_BYTES, "command too large");
                        let command = serde_json::from_str::<Value>(json)?;
                        validate_command(&command)?;
                        Ok(command)
                    })
                    .transpose()?;
                Ok(Entry {
                    term: entry.term,
                    command,
                })
            })
            .collect()
    }

    pub fn plan_append(
        &self,
        request: &AppendEntriesRequest,
        mut entries: Vec<Entry>,
    ) -> Result<AppendPlan> {
        let mut response = AppendEntriesResponse {
            term: request.term,
            success: false,
            conflict_index: 0,
            conflict_term: 0,
        };
        if request.prev_log_index > self.last_index() {
            response.conflict_index = self.last_index() + 1;
        } else if self.term(request.prev_log_index) != Some(request.prev_log_term) {
            let term = self.term(request.prev_log_index).unwrap();
            let first = self.entries.partition_point(|entry| entry.term < term) as u64 + 1;
            response.conflict_term = term;
            response.conflict_index = first;
        } else {
            let mut conflict = None;
            for (offset, entry) in entries.iter().enumerate() {
                let index = request.prev_log_index + offset as u64 + 1;
                match self.entry(index) {
                    Some(existing) if existing.term == entry.term => {
                        ensure!(existing == entry, "same index/term has different command");
                    }
                    _ => {
                        ensure!(
                            index > self.commit_index,
                            "leader attempted to overwrite committed log"
                        );
                        conflict = Some((index, offset));
                        break;
                    }
                }
            }
            response.success = true;
            // A short or empty RPC does not prove the follower's trailing suffix matches.
            let covered = request.prev_log_index + entries.len() as u64;
            let mutation = conflict.map(|(index, offset)| (index, entries.split_off(offset)));
            return Ok(AppendPlan {
                response,
                mutation,
                commit_index: self.commit_index.max(request.leader_commit.min(covered)),
            });
        }
        Ok(AppendPlan {
            response,
            mutation: None,
            commit_index: self.commit_index,
        })
    }
}
