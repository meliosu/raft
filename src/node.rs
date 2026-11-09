//! One task owns all Raft state. Network I/O is concurrent and bounded per peer;
//! When enabled, durable mutations run on the blocking pool before acknowledgement.
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use log::{debug, info, trace, warn};
use serde::Serialize;
use serde_json::Value;
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinSet,
    time::{Instant, sleep_until, timeout},
};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Status as RpcStatus, transport::Endpoint};

use crate::{
    config::Config,
    election::{Election, Role, Status},
    proto::{
        AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse,
        election_client::ElectionClient,
    },
    replication::{Entry, MAX_PAGE_ENTRIES, MAX_RPC_BYTES, ReplicatedLog, validate_command},
    storage::{HardState, Storage},
};

const MAX_PENDING: usize = 128;

#[derive(Debug)]
pub enum ClientError {
    NotLeader { leader_id: Option<u64> },
    Busy,
    InvalidCommand,
    OutcomeUnknown,
    Unavailable,
}

#[derive(Debug, Serialize)]
pub struct Proposal {
    pub index: u64,
    pub term: u64,
}

#[derive(Debug, Serialize)]
pub struct LogView {
    pub index: u64,
    pub term: u64,
    pub kind: &'static str,
    pub command: Option<Value>,
    pub committed: bool,
}

#[derive(Debug, Serialize)]
pub struct LogPage {
    pub node_id: u64,
    pub current_term: u64,
    pub commit_index: u64,
    pub last_log_index: u64,
    pub entries: Vec<LogView>,
    pub next_index: Option<u64>,
}

pub enum Inbound {
    Vote(
        VoteRequest,
        oneshot::Sender<Result<VoteResponse, RpcStatus>>,
    ),
    Append(
        AppendEntriesRequest,
        oneshot::Sender<Result<AppendEntriesResponse, RpcStatus>>,
    ),
    Propose(Value, oneshot::Sender<Result<Proposal, ClientError>>),
    ReadLog {
        from: u64,
        limit: usize,
        reply: oneshot::Sender<LogPage>,
    },
}

enum Outbound {
    Vote(VoteRequest),
    Append(AppendEntriesRequest),
}

enum Reply {
    Vote(VoteResponse),
    Append(AppendEntriesResponse),
}

struct Completion {
    peer: u64,
    sent_term: u64,
    // Exact request metadata, not the leader's mutable current progress.
    prev_index: u64,
    last_index: u64,
    leader_commit: u64,
    reply: Result<Reply, RpcStatus>,
}

struct Progress {
    next_index: u64,
    match_index: u64,
    ack_commit: u64,
    retry_at: Instant,
}

struct Pending {
    index: u64,
    term: u64,
    reply: oneshot::Sender<Result<Proposal, ClientError>>,
}

pub struct Node {
    config: Config,
    election: Election,
    log: ReplicatedLog,
    // The live term/vote/log always belong to this task. None disables only
    // disk persistence, without introducing a second in-memory copy of state.
    storage: Option<Arc<Storage>>,
    inbound: mpsc::Receiver<Inbound>,
    status: watch::Sender<Status>,
    clients: HashMap<u64, ElectionClient<tonic::transport::Channel>>,
    in_flight: HashSet<u64>,
    unreachable: HashSet<u64>,
    calls: JoinSet<Completion>,
    next_heartbeat: Instant,
    leader_term: Option<u64>,
    progress: HashMap<u64, Progress>,
    pending: Vec<Pending>,
}

impl Node {
    pub async fn new(
        config: Config,
    ) -> Result<(Self, mpsc::Sender<Inbound>, watch::Receiver<Status>)> {
        Self::initialize(config, false).await
    }

    /// Start with empty, volatile state and never access the data directory.
    /// Losing terms/votes on restart violates Raft's crash-recovery assumptions;
    /// use this only for disposable clusters, not durable membership.
    pub async fn new_in_memory(
        config: Config,
    ) -> Result<(Self, mpsc::Sender<Inbound>, watch::Receiver<Status>)> {
        Self::initialize(config, true).await
    }

    async fn initialize(
        config: Config,
        in_memory: bool,
    ) -> Result<(Self, mpsc::Sender<Inbound>, watch::Receiver<Status>)> {
        config.validate()?;
        let id = config.id();
        let members: Vec<_> = config.nodes.iter().map(|node| node.id).collect();
        let (storage, hard, log) = if in_memory {
            warn!(
                "node={id} running in memory: state is lost on exit; restarting an existing member is unsafe"
            );
            (None, HardState::default(), ReplicatedLog::default())
        } else {
            let dir = config.data_dir.clone();
            let persisted_members = members.clone();
            let (storage, hard, log) =
                tokio::task::spawn_blocking(move || Storage::open(dir, id, persisted_members))
                    .await??;
            (Some(Arc::new(storage)), hard, log)
        };
        let mut election = Election::new(id, members, hard, Self::deadline(&config));
        election.set_log_tail(log.last_index(), log.term(log.last_index()).unwrap());
        info!(
            "node={id} loaded term={} vote={:?} last_log_index={}",
            election.hard.current_term,
            election.hard.voted_for,
            log.last_index()
        );
        let snapshot = Self::snapshot(&election, &log);
        let (sender, inbound) = mpsc::channel(128);
        let (status, receiver) = watch::channel(snapshot);
        let mut clients = HashMap::new();
        for peer in config.nodes.iter().filter(|peer| peer.id != id) {
            let channel = Endpoint::from_shared(peer.address.clone())?
                .connect_timeout(config.rpc_timeout())
                .tcp_nodelay(true)
                .connect_lazy();
            clients.insert(
                peer.id,
                ElectionClient::new(channel)
                    .max_decoding_message_size(MAX_RPC_BYTES)
                    .max_encoding_message_size(MAX_RPC_BYTES),
            );
        }
        Ok((
            Self {
                config,
                election,
                log,
                storage,
                inbound,
                status,
                clients,
                in_flight: HashSet::new(),
                unreachable: HashSet::new(),
                calls: JoinSet::new(),
                next_heartbeat: Instant::now(),
                leader_term: None,
                progress: HashMap::new(),
                pending: Vec::new(),
            },
            sender,
            receiver,
        ))
    }

    fn snapshot(election: &Election, log: &ReplicatedLog) -> Status {
        Status {
            node_id: election.id,
            role: election.role,
            current_term: election.hard.current_term,
            leader_id: election.leader_id,
            last_log_index: log.last_index(),
            commit_index: log.commit_index,
        }
    }

    fn deadline(config: &Config) -> Instant {
        Instant::now()
            + Duration::from_millis(rand::random_range(
                config.election_timeout_min_ms..=config.election_timeout_max_ms,
            ))
    }

    async fn commit_election(&mut self, mut next: Election) -> Result<()> {
        if self.election.role == Role::Leader && next.role != Role::Leader {
            next.deadline = Self::deadline(&self.config);
        }
        if next.hard != self.election.hard
            && let Some(storage) = &self.storage
        {
            let storage = Arc::clone(storage);
            let hard = next.hard.clone();
            tokio::task::spawn_blocking(move || storage.save(&hard))
                .await?
                .context("persisting term/vote; node must stop on storage failure")?;
        }
        self.election = next;
        Ok(())
    }

    /// Update the live suffix only after any configured disk write succeeds.
    async fn replace_log_suffix(&mut self, from: u64, mut entries: Vec<Entry>) -> Result<()> {
        if let Some(storage) = &self.storage {
            let storage = Arc::clone(storage);
            // Return the batch from the blocking job instead of cloning it.
            entries = tokio::task::spawn_blocking(move || {
                storage.append(from, &entries)?;
                Ok::<_, anyhow::Error>(entries)
            })
            .await?
            .context("persisting log; node must stop on storage failure")?;
        }
        self.log.replace(from, entries)?;
        self.election.set_log_tail(
            self.log.last_index(),
            self.log.term(self.log.last_index()).unwrap(),
        );
        Ok(())
    }

    fn send(&mut self, peer: u64, outbound: Outbound) {
        if !self.in_flight.insert(peer) {
            return;
        }
        let mut client = self.clients[&peer].clone();
        let limit = self.config.rpc_timeout();
        self.calls.spawn(async move {
            let (sent_term, prev_index, last_index, leader_commit) = match &outbound {
                Outbound::Vote(r) => (r.term, 0, 0, 0),
                Outbound::Append(r) => (
                    r.term,
                    r.prev_log_index,
                    r.prev_log_index + r.entries.len() as u64,
                    r.leader_commit,
                ),
            };
            let call = async {
                match outbound {
                    Outbound::Vote(body) => {
                        let mut request = Request::new(body);
                        request.set_timeout(limit);
                        client
                            .request_vote(request)
                            .await
                            .map(|r| Reply::Vote(r.into_inner()))
                    }
                    Outbound::Append(body) => {
                        let mut request = Request::new(body);
                        request.set_timeout(limit);
                        client
                            .append_entries(request)
                            .await
                            .map(|r| Reply::Append(r.into_inner()))
                    }
                }
            };
            let reply = timeout(limit, call)
                .await
                .unwrap_or_else(|_| Err(RpcStatus::deadline_exceeded("peer RPC timed out")));
            Completion {
                peer,
                sent_term,
                prev_index,
                last_index,
                leader_commit,
                reply,
            }
        });
    }

    async fn handle_inbound(&mut self, event: Inbound) -> Result<()> {
        match event {
            Inbound::Vote(request, reply) => {
                if !self.election.is_remote_member(request.candidate_id)
                    || request.term == 0
                    || (request.last_log_index == 0) != (request.last_log_term == 0)
                    || request.last_log_term > request.term
                {
                    warn!(
                        "node={} invalid vote candidate={} term={}",
                        self.election.id, request.candidate_id, request.term
                    );
                    let _ = reply.send(Err(RpcStatus::invalid_argument(
                        "invalid candidate, term, or log tail",
                    )));
                    return Ok(());
                }
                let mut next = self.election.clone();
                let response = next.request_vote(&request, Self::deadline(&self.config));
                debug!(
                    "node={} vote candidate={} term={} granted={}",
                    next.id, request.candidate_id, request.term, response.vote_granted
                );
                self.commit_election(next).await?;
                let _ = reply.send(Ok(response));
            }
            Inbound::Append(request, reply) => {
                if !self.election.is_remote_member(request.leader_id) || request.term == 0 {
                    warn!(
                        "node={} invalid AppendEntries leader={} term={}",
                        self.election.id, request.leader_id, request.term
                    );
                    let _ = reply.send(Err(RpcStatus::invalid_argument(
                        "leader must be another member and term must be positive",
                    )));
                    return Ok(());
                }
                let entries = match ReplicatedLog::decode(&request) {
                    Ok(entries) => entries,
                    Err(error) => {
                        warn!("node={} invalid AppendEntries: {error}", self.election.id);
                        let _ = reply.send(Err(RpcStatus::invalid_argument(error.to_string())));
                        return Ok(());
                    }
                };
                let mut next = self.election.clone();
                // Even a rejected prefix check is contact with the current leader.
                let accepted = next.accept_leader(
                    request.term,
                    request.leader_id,
                    Self::deadline(&self.config),
                );
                self.commit_election(next).await?;
                if !accepted {
                    let _ = reply.send(Ok(AppendEntriesResponse {
                        term: self.election.hard.current_term,
                        success: false,
                        conflict_index: 0,
                        conflict_term: 0,
                    }));
                    return Ok(());
                }
                let plan = self.log.plan_append(&request, entries)?;
                trace!(
                    "node={} AppendEntries leader={} term={} prev={} entries={} success={}",
                    self.election.id,
                    request.leader_id,
                    request.term,
                    request.prev_log_index,
                    request.entries.len(),
                    plan.response.success
                );
                if let Some((from, entries)) = plan.mutation {
                    self.replace_log_suffix(from, entries).await?;
                }
                self.log.commit_index = plan.commit_index;
                let _ = reply.send(Ok(plan.response));
            }
            Inbound::Propose(command, reply) => {
                if reply.is_closed() {
                    return Ok(());
                }
                if self.election.role != Role::Leader {
                    let _ = reply.send(Err(ClientError::NotLeader {
                        leader_id: self.election.leader_id,
                    }));
                } else if self.pending.len() >= MAX_PENDING {
                    let _ = reply.send(Err(ClientError::Busy));
                } else if validate_command(&command).is_err() {
                    let _ = reply.send(Err(ClientError::InvalidCommand));
                } else {
                    let index = self
                        .log
                        .last_index()
                        .checked_add(1)
                        .filter(|i| *i < u64::MAX)
                        .context("log index exhausted")?;
                    let term = self.election.hard.current_term;
                    self.replace_log_suffix(
                        index,
                        vec![Entry {
                            term,
                            command: Some(command),
                        }],
                    )
                    .await?;
                    self.pending.push(Pending { index, term, reply });
                }
            }
            Inbound::ReadLog { from, limit, reply } => {
                let from = from.max(1);
                let end = self.log.last_index().min(
                    from.saturating_add(limit.min(MAX_PAGE_ENTRIES) as u64)
                        .saturating_sub(1),
                );
                let entries = (from..=end)
                    .map(|index| {
                        let entry = self.log.entry(index).unwrap();
                        LogView {
                            index,
                            term: entry.term,
                            kind: if entry.command.is_some() {
                                "command"
                            } else {
                                "noop"
                            },
                            command: entry.command.clone(),
                            committed: index <= self.log.commit_index,
                        }
                    })
                    .collect();
                let _ = reply.send(LogPage {
                    node_id: self.election.id,
                    current_term: self.election.hard.current_term,
                    commit_index: self.log.commit_index,
                    last_log_index: self.log.last_index(),
                    entries,
                    next_index: (end < self.log.last_index()).then_some(end + 1),
                });
            }
        }
        Ok(())
    }

    async fn handle_completion(&mut self, completion: Completion) -> Result<()> {
        self.in_flight.remove(&completion.peer);
        let reply = match completion.reply {
            Ok(reply) => {
                if self.unreachable.remove(&completion.peer) {
                    info!(
                        "node={} peer={} reachable again",
                        self.config.id(),
                        completion.peer
                    );
                }
                reply
            }
            Err(error) => {
                if self.unreachable.insert(completion.peer) {
                    warn!(
                        "node={} peer={} RPC failed: {error}",
                        self.config.id(),
                        completion.peer
                    );
                } else {
                    debug!(
                        "node={} peer={} RPC still failing: {error}",
                        self.config.id(),
                        completion.peer
                    );
                }
                if let Some(progress) = self.progress.get_mut(&completion.peer) {
                    progress.retry_at =
                        Instant::now() + Duration::from_millis(self.config.heartbeat_interval_ms);
                }
                return Ok(());
            }
        };
        let mut next = self.election.clone();
        match &reply {
            Reply::Vote(response) => {
                next.vote_response(completion.peer, completion.sent_term, response)
            }
            Reply::Append(response) => next.observe_term(response.term),
        }
        // A higher response term is authoritative even for an obsolete RPC.
        self.commit_election(next).await?;
        if let Reply::Append(response) = reply {
            if self.election.role != Role::Leader
                || completion.sent_term != self.election.hard.current_term
                || response.term != completion.sent_term
            {
                return Ok(());
            }
            let Some(progress) = self.progress.get_mut(&completion.peer) else {
                return Ok(());
            };
            if response.success {
                progress.match_index = progress.match_index.max(completion.last_index);
                progress.next_index = progress.match_index + 1;
                progress.ack_commit = progress
                    .ack_commit
                    .max(completion.leader_commit.min(completion.last_index));
            } else {
                let hint = if response.conflict_term != 0 {
                    self.log
                        .last_index_of_term(response.conflict_term)
                        .map(|i| i + 1)
                        .unwrap_or(response.conflict_index)
                } else {
                    response.conflict_index
                };
                // Never move forward on a rejection or undo acknowledged progress.
                progress.next_index = hint
                    .max(1)
                    .min(completion.prev_index)
                    .max(progress.match_index + 1);
            }
        }
        Ok(())
    }

    /// Keep state transitions, client replies, status, and replication in order.
    async fn maintain(&mut self) -> Result<()> {
        if self.election.role == Role::Leader {
            self.initialize_leader().await?;
            self.advance_commit();
        } else {
            self.leader_term = None;
            self.progress.clear();
        }
        self.resolve_proposals();
        self.publish_status();
        if self.election.role == Role::Leader {
            self.replicate()?;
        }
        Ok(())
    }

    async fn initialize_leader(&mut self) -> Result<()> {
        let term = self.election.hard.current_term;
        if self.leader_term == Some(term) {
            return Ok(());
        }
        self.leader_term = Some(term);
        self.progress = self
            .clients
            .keys()
            .map(|&id| {
                (
                    id,
                    Progress {
                        next_index: self.log.last_index() + 1,
                        match_index: 0,
                        ack_commit: 0,
                        retry_at: Instant::now(),
                    },
                )
            })
            .collect();
        let index = self
            .log
            .last_index()
            .checked_add(1)
            .filter(|i| *i < u64::MAX)
            .context("log index exhausted")?;
        // A current-term no-op lets the new leader commit inherited entries.
        self.replace_log_suffix(
            index,
            vec![Entry {
                term,
                command: None,
            }],
        )
        .await?;
        self.next_heartbeat = Instant::now();
        Ok(())
    }

    fn advance_commit(&mut self) {
        let mut matched: Vec<_> = self.progress.values().map(|p| p.match_index).collect();
        matched.push(self.log.last_index());
        matched.sort_unstable();
        let majority = matched.len() / 2 + 1;
        let candidate = matched[matched.len() - majority];
        // Raft only directly commits entries from the leader's current term.
        if candidate > self.log.commit_index
            && self.log.term(candidate) == Some(self.election.hard.current_term)
        {
            self.log.commit_index = candidate;
        }
    }

    fn resolve_proposals(&mut self) {
        let mut waiting = Vec::new();
        for pending in self.pending.drain(..) {
            if pending.reply.is_closed() {
                continue;
            }
            if pending.index <= self.log.commit_index
                && self.log.term(pending.index) == Some(pending.term)
            {
                let _ = pending.reply.send(Ok(Proposal {
                    index: pending.index,
                    term: pending.term,
                }));
            } else if self.election.role != Role::Leader
                || pending.term != self.election.hard.current_term
            {
                let _ = pending.reply.send(Err(ClientError::OutcomeUnknown));
            } else {
                waiting.push(pending);
            }
        }
        self.pending = waiting;
    }

    fn publish_status(&self) {
        let snapshot = Self::snapshot(&self.election, &self.log);
        let before = self.status.borrow().clone();
        if before != snapshot {
            if (before.role, before.current_term, before.leader_id)
                != (snapshot.role, snapshot.current_term, snapshot.leader_id)
            {
                info!(
                    "node={} role={:?}->{:?} term={}->{} leader={:?}",
                    snapshot.node_id,
                    before.role,
                    snapshot.role,
                    before.current_term,
                    snapshot.current_term,
                    snapshot.leader_id
                );
            }
            self.status.send_replace(snapshot);
        }
    }

    /// Empty RPC successes must not cause an empty-RPC loop. Send only for new
    /// entries, new commit knowledge, or a heartbeat timer expiration.
    fn replicate(&mut self) -> Result<()> {
        let now = Instant::now();
        let heartbeat = now >= self.next_heartbeat;
        if heartbeat {
            self.next_heartbeat = now + Duration::from_millis(self.config.heartbeat_interval_ms);
        }
        let peers: Vec<_> = self.progress.keys().copied().collect();
        for peer in peers {
            let progress = &self.progress[&peer];
            if self.in_flight.contains(&peer) || now < progress.retry_at {
                continue;
            }
            if !heartbeat
                && progress.next_index > self.log.last_index()
                && progress.ack_commit >= self.log.commit_index
            {
                continue;
            }
            let prev_log_index = progress.next_index - 1;
            ensure!(
                prev_log_index <= self.log.last_index(),
                "replication progress exceeds log tail"
            );
            let request = AppendEntriesRequest {
                term: self.election.hard.current_term,
                leader_id: self.election.id,
                prev_log_index,
                prev_log_term: self.log.term(prev_log_index).unwrap(),
                entries: self.log.batch(progress.next_index),
                leader_commit: self.log.commit_index,
            };
            self.send(peer, Outbound::Append(request));
        }
        Ok(())
    }

    pub async fn run(mut self, shutdown: CancellationToken) -> Result<()> {
        loop {
            let deadline = if self.election.role == Role::Leader {
                self.next_heartbeat
            } else {
                self.election.deadline
            };
            tokio::select! {
                _ = shutdown.cancelled() => break,
                event = self.inbound.recv() => {
                    let Some(event) = event else { break; };
                    self.handle_inbound(event).await?;
                }
                result = self.calls.join_next(), if !self.calls.is_empty() => {
                    self.handle_completion(result.context("RPC task disappeared")?.context("RPC task panicked")?).await?;
                }
                _ = sleep_until(deadline) => {
                    if self.election.role != Role::Leader {
                        let mut next = self.election.clone();
                        let request = next.start_election(Self::deadline(&self.config))?;
                        info!("node={} starting election term={}", next.id, request.term);
                        self.commit_election(next).await?;
                        let peers: Vec<_> = self.clients.keys().copied().collect();
                        for peer in peers { self.send(peer, Outbound::Vote(request)); }
                    }
                }
            }
            self.maintain().await?;
        }
        self.calls.abort_all();
        for pending in self.pending.drain(..) {
            let _ = pending.reply.send(Err(ClientError::OutcomeUnknown));
        }
        info!("node={} Raft task stopped", self.config.id());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Peer;
    use serde_json::json;
    use tempfile::{TempDir, tempdir};

    fn config(count: u64, data_dir: std::path::PathBuf) -> Config {
        Config {
            node_id: Some(1),
            data_dir,
            nodes: (1..=count)
                .map(|id| Peer {
                    id,
                    address: format!("http://127.0.0.1:{}", 60000 + id),
                })
                .collect(),
            ..Config::default()
        }
    }

    async fn node(count: u64) -> (TempDir, Node, watch::Receiver<Status>) {
        let dir = tempdir().unwrap();
        let (node, _, status) = Node::new(config(count, dir.path().into())).await.unwrap();
        (dir, node, status)
    }

    async fn become_leader(node: &mut Node) {
        let mut next = node.election.clone();
        let request = next.start_election(Instant::now()).unwrap();
        for peer in 2..=node.config.nodes.len() as u64 {
            next.vote_response(
                peer,
                request.term,
                &VoteResponse {
                    term: request.term,
                    vote_granted: true,
                },
            );
        }
        node.commit_election(next).await.unwrap();
        node.maintain().await.unwrap();
        node.calls.abort_all();
        node.in_flight.clear();
    }

    fn completion(
        peer: u64,
        term: u64,
        last: u64,
        response_term: u64,
        success: bool,
    ) -> Completion {
        Completion {
            peer,
            sent_term: term,
            prev_index: last.saturating_sub(1),
            last_index: last,
            leader_commit: 0,
            reply: Ok(Reply::Append(AppendEntriesResponse {
                term: response_term,
                success,
                conflict_index: 1,
                conflict_term: 0,
            })),
        }
    }

    #[tokio::test]
    async fn commits_only_current_term_entries_with_strict_majority() {
        let (_dir, mut node, _) = node(4).await;
        let mut next = node.election.clone();
        next.observe_term(1);
        node.commit_election(next).await.unwrap();
        node.replace_log_suffix(
            1,
            vec![Entry {
                term: 1,
                command: Some(json!("old")),
            }],
        )
        .await
        .unwrap();
        become_leader(&mut node).await; // term 2, appends no-op at index 2
        assert_eq!(node.log.last_index(), 2);
        node.progress.get_mut(&2).unwrap().match_index = 1;
        node.progress.get_mut(&3).unwrap().match_index = 1;
        node.maintain().await.unwrap();
        assert_eq!(node.log.commit_index, 0); // old-term majority is insufficient
        node.progress.get_mut(&2).unwrap().match_index = 2;
        node.maintain().await.unwrap();
        assert_eq!(node.log.commit_index, 0); // two of four is not a majority
        node.progress.get_mut(&3).unwrap().match_index = 2;
        node.maintain().await.unwrap();
        assert_eq!(node.log.commit_index, 2);
    }

    #[tokio::test]
    async fn ignores_stale_progress_but_always_observes_higher_response_terms() {
        let (_dir, mut node, _) = node(3).await;
        become_leader(&mut node).await;
        let term = node.election.hard.current_term;
        node.handle_completion(completion(2, term - 1, 1, term - 1, true))
            .await
            .unwrap();
        assert_eq!(node.progress[&2].match_index, 0);
        node.handle_completion(completion(2, term, 1, term, true))
            .await
            .unwrap();
        assert_eq!(node.progress[&2].match_index, 1);
        node.handle_completion(completion(3, term - 1, 1, term + 1, false))
            .await
            .unwrap();
        assert_eq!(node.election.role, Role::Follower);
        assert_eq!(node.election.hard.current_term, term + 1);
        node.maintain().await.unwrap();
        assert!(node.progress.is_empty());
    }

    #[tokio::test]
    async fn successful_response_acknowledges_only_entries_in_its_request() {
        let (_dir, mut node, _) = node(3).await;
        become_leader(&mut node).await;
        let term = node.election.hard.current_term;
        node.replace_log_suffix(
            2,
            vec![Entry {
                term,
                command: Some(json!("later")),
            }],
        )
        .await
        .unwrap();
        node.handle_completion(completion(2, term, 1, term, true))
            .await
            .unwrap();
        node.maintain().await.unwrap();
        assert_eq!(node.progress[&2].match_index, 1);
        assert_eq!(node.progress[&2].next_index, 2);
        assert_eq!(node.log.commit_index, 1);
    }

    #[tokio::test]
    async fn rejected_prefix_resets_follower_timer_but_stale_request_does_not() {
        let (_dir, mut node, _) = node(3).await;
        let mut next = node.election.clone();
        next.start_election(Instant::now()).unwrap();
        node.commit_election(next).await.unwrap();
        let before = node.election.deadline;
        let request = AppendEntriesRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 0,
        };
        let (tx, rx) = oneshot::channel();
        node.handle_inbound(Inbound::Append(request.clone(), tx))
            .await
            .unwrap();
        assert!(!rx.await.unwrap().unwrap().success);
        assert_eq!(node.election.role, Role::Follower);
        assert!(node.election.deadline > before);
        let deadline = node.election.deadline;
        let mut next = node.election.clone();
        next.observe_term(2);
        node.commit_election(next).await.unwrap();
        let (tx, rx) = oneshot::channel();
        node.handle_inbound(Inbound::Append(request, tx))
            .await
            .unwrap();
        let response = rx.await.unwrap().unwrap();
        assert!(!response.success);
        assert_eq!(response.term, 2);
        assert_eq!(node.election.deadline, deadline);
    }

    #[tokio::test]
    async fn single_node_commits_noop_and_proposals_and_handles_null() {
        let (_dir, mut node, status) = node(1).await;
        become_leader(&mut node).await;
        assert_eq!(node.log.commit_index, 1);
        let (tx, rx) = oneshot::channel();
        node.handle_inbound(Inbound::Propose(Value::Null, tx))
            .await
            .unwrap();
        node.maintain().await.unwrap();
        let result = rx.await.unwrap().unwrap();
        assert_eq!(result.index, 2);
        assert_eq!(node.log.entry(2).unwrap().command, Some(Value::Null));
        assert_eq!(status.borrow().commit_index, 2);
    }

    #[tokio::test]
    async fn stepdown_resolves_uncommitted_proposal_as_unknown_not_success() {
        let (_dir, mut node, _) = node(3).await;
        become_leader(&mut node).await;
        let (tx, mut rx) = oneshot::channel();
        node.handle_inbound(Inbound::Propose(json!("isolated"), tx))
            .await
            .unwrap();
        node.maintain().await.unwrap();
        assert!(matches!(
            rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        let mut next = node.election.clone();
        next.observe_term(2);
        node.commit_election(next).await.unwrap();
        node.maintain().await.unwrap();
        assert!(matches!(
            rx.await.unwrap(),
            Err(ClientError::OutcomeUnknown)
        ));
    }

    #[tokio::test]
    async fn persistence_failure_prevents_vote_reply_and_publication() {
        let (dir, mut node, status) = node(3).await;
        std::fs::create_dir(dir.path().join("hard-state.tmp")).unwrap();
        let (tx, rx) = oneshot::channel();
        let result = node
            .handle_inbound(Inbound::Vote(
                VoteRequest {
                    term: 1,
                    candidate_id: 2,
                    last_log_index: 0,
                    last_log_term: 0,
                },
                tx,
            ))
            .await;
        assert!(result.is_err());
        assert!(rx.await.is_err());
        assert_eq!(node.election.hard.current_term, 0);
        assert_eq!(status.borrow().current_term, 0);
    }

    #[tokio::test]
    async fn follower_success_is_durable_and_restored_log_is_used_for_voting() {
        let (dir, mut node, _) = node(3).await;
        let (tx, rx) = oneshot::channel();
        node.handle_inbound(Inbound::Append(
            AppendEntriesRequest {
                term: 3,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![
                    Entry {
                        term: 2,
                        command: Some(json!("durable")),
                    }
                    .wire(),
                ],
                leader_commit: 1,
            },
            tx,
        ))
        .await
        .unwrap();
        assert!(rx.await.unwrap().unwrap().success);
        let config = node.config.clone();
        drop(node);
        let (mut node, _sender, _) = Node::new(config).await.unwrap();
        assert_eq!(node.log.entry(1).unwrap().command, Some(json!("durable")));
        assert_eq!(node.election.hard.current_term, 3);
        let (tx, rx) = oneshot::channel();
        node.handle_inbound(Inbound::Vote(
            VoteRequest {
                term: 4,
                candidate_id: 3,
                last_log_index: 0,
                last_log_term: 0,
            },
            tx,
        ))
        .await
        .unwrap();
        assert!(!rx.await.unwrap().unwrap().vote_granted);
        drop(node);
        let (_, hard, log) = Storage::open(dir.path().into(), 1, vec![1, 2, 3]).unwrap();
        assert_eq!(hard.current_term, 4);
        assert_eq!(log.last_index(), 1);
    }

    #[tokio::test]
    async fn conflict_response_uses_term_hint_without_changing_match_index() {
        let (_dir, mut node, _) = node(3).await;
        let mut next = node.election.clone();
        next.observe_term(3);
        node.commit_election(next).await.unwrap();
        node.replace_log_suffix(
            1,
            [1, 2, 2, 3]
                .into_iter()
                .map(|term| Entry {
                    term,
                    command: None,
                })
                .collect(),
        )
        .await
        .unwrap();
        become_leader(&mut node).await;
        assert_eq!(node.progress[&2].next_index, 5);
        node.handle_completion(Completion {
            peer: 2,
            sent_term: 4,
            prev_index: 4,
            last_index: 5,
            leader_commit: 0,
            reply: Ok(Reply::Append(AppendEntriesResponse {
                term: 4,
                success: false,
                conflict_term: 2,
                conflict_index: 1,
            })),
        })
        .await
        .unwrap();
        assert_eq!(node.progress[&2].next_index, 4);
        assert_eq!(node.progress[&2].match_index, 0);
    }

    #[tokio::test]
    async fn in_memory_votes_replication_and_proposals_never_create_state_files() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("unused/nested/state");
        let config = config(3, path.clone());
        let (mut node, _sender, status) = Node::new_in_memory(config.clone()).await.unwrap();
        assert!(node.storage.is_none());
        for (candidate_id, granted) in [(2, true), (3, false)] {
            let (tx, rx) = oneshot::channel();
            node.handle_inbound(Inbound::Vote(
                VoteRequest {
                    term: 2,
                    candidate_id,
                    last_log_index: 0,
                    last_log_term: 0,
                },
                tx,
            ))
            .await
            .unwrap();
            assert_eq!(rx.await.unwrap().unwrap().vote_granted, granted);
        }
        let (tx, rx) = oneshot::channel();
        node.handle_inbound(Inbound::Append(
            AppendEntriesRequest {
                term: 3,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![
                    Entry {
                        term: 3,
                        command: Some(Value::Null),
                    }
                    .wire(),
                ],
                leader_commit: 1,
            },
            tx,
        ))
        .await
        .unwrap();
        assert!(rx.await.unwrap().unwrap().success);
        assert_eq!(node.log.entry(1).unwrap().command, Some(Value::Null));
        become_leader(&mut node).await;
        let (tx, mut rx) = oneshot::channel();
        node.handle_inbound(Inbound::Propose(json!("memory proposal"), tx))
            .await
            .unwrap();
        node.maintain().await.unwrap();
        assert!(matches!(
            rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        node.handle_completion(completion(2, 4, 3, 4, true))
            .await
            .unwrap();
        node.maintain().await.unwrap();
        assert_eq!(rx.await.unwrap().unwrap().index, 3);
        assert_eq!(status.borrow().commit_index, 3);
        assert!(!dir.path().join("unused").exists());

        drop(node);
        let (fresh, _, status) = Node::new_in_memory(config).await.unwrap();
        assert_eq!(fresh.election.hard, HardState::default());
        assert_eq!(status.borrow().last_log_index, 0);
        assert_eq!(status.borrow().commit_index, 0);
        assert!(!dir.path().join("unused").exists());
    }

    #[tokio::test]
    async fn in_memory_ignores_locked_existing_state_without_modifying_it() {
        let (dir, mut disk, _) = node(3).await;
        let mut next = disk.election.clone();
        next.observe_term(7);
        disk.commit_election(next).await.unwrap();
        disk.replace_log_suffix(
            1,
            vec![Entry {
                term: 7,
                command: Some(json!("on disk")),
            }],
        )
        .await
        .unwrap();
        let hard = std::fs::read(dir.path().join("hard-state.json")).unwrap();
        let log = std::fs::read(dir.path().join("log.journal")).unwrap();

        // The durable node still holds its directory lock. A volatile node must
        // neither try to acquire that lock nor load its term or log.
        let (mut memory, _, status) = Node::new_in_memory(disk.config.clone()).await.unwrap();
        assert_eq!(status.borrow().current_term, 0);
        assert_eq!(status.borrow().last_log_index, 0);
        become_leader(&mut memory).await;
        assert_eq!(memory.election.hard.current_term, 1);
        assert_eq!(
            std::fs::read(dir.path().join("hard-state.json")).unwrap(),
            hard
        );
        assert_eq!(std::fs::read(dir.path().join("log.journal")).unwrap(), log);
    }

    #[tokio::test]
    async fn in_memory_accepts_a_data_path_that_is_not_a_directory() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("not-a-directory");
        std::fs::write(&path, b"untouched").unwrap();
        let (mut node, _, _) = Node::new_in_memory(config(1, path.clone())).await.unwrap();
        become_leader(&mut node).await;
        let (tx, rx) = oneshot::channel();
        node.handle_inbound(Inbound::Propose(json!("test"), tx))
            .await
            .unwrap();
        node.maintain().await.unwrap();
        assert_eq!(rx.await.unwrap().unwrap().index, 2);
        assert_eq!(std::fs::read(path).unwrap(), b"untouched");
    }
}
