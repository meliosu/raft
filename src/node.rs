//! A single task owns election state. RPCs never hold a lock across network I/O.
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use log::{debug, info, trace, warn};
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
        HeartbeatRequest, HeartbeatResponse, VoteRequest, VoteResponse,
        election_client::ElectionClient,
    },
    storage::Storage,
};

pub enum Inbound {
    Vote(
        VoteRequest,
        oneshot::Sender<Result<VoteResponse, RpcStatus>>,
    ),
    Heartbeat(
        HeartbeatRequest,
        oneshot::Sender<Result<HeartbeatResponse, RpcStatus>>,
    ),
}

#[derive(Clone)]
enum Outbound {
    Vote(VoteRequest),
    Heartbeat(HeartbeatRequest),
}
enum Reply {
    Vote(VoteResponse),
    Heartbeat(HeartbeatResponse),
}
struct Completion {
    peer: u64,
    sent_term: u64,
    reply: Result<Reply, RpcStatus>,
}

pub struct Node {
    config: Config,
    election: Election,
    storage: Arc<Storage>,
    inbound: mpsc::Receiver<Inbound>,
    status: watch::Sender<Status>,
    clients: HashMap<u64, ElectionClient<tonic::transport::Channel>>,
    in_flight: HashSet<u64>,
    unreachable: HashSet<u64>,
    calls: JoinSet<Completion>,
    next_heartbeat: Instant,
}

impl Node {
    pub async fn new(
        config: Config,
    ) -> Result<(Self, mpsc::Sender<Inbound>, watch::Receiver<Status>)> {
        config.validate()?;
        let id = config.id();
        let members: Vec<_> = config.nodes.iter().map(|node| node.id).collect();
        let dir = config.data_dir.clone();
        let persisted_members = members.clone();
        let (storage, hard) =
            tokio::task::spawn_blocking(move || Storage::open(dir, id, persisted_members))
                .await??;
        let election = Election::new(id, members, hard, Self::deadline(&config));
        info!(
            "node={id} loaded term={} vote={:?}",
            election.hard.current_term, election.hard.voted_for
        );
        let (sender, inbound) = mpsc::channel(128);
        let (status, receiver) = watch::channel(election.status());
        let mut clients = HashMap::new();
        for peer in config.nodes.iter().filter(|peer| peer.id != id) {
            let channel = Endpoint::from_shared(peer.address.clone())?
                .connect_timeout(config.rpc_timeout())
                .tcp_nodelay(true)
                .connect_lazy();
            clients.insert(peer.id, ElectionClient::new(channel));
        }
        Ok((
            Self {
                config,
                election,
                storage: Arc::new(storage),
                inbound,
                status,
                clients,
                in_flight: HashSet::new(),
                unreachable: HashSet::new(),
                calls: JoinSet::new(),
                next_heartbeat: Instant::now(),
            },
            sender,
            receiver,
        ))
    }

    fn deadline(config: &Config) -> Instant {
        let milliseconds =
            rand::random_range(config.election_timeout_min_ms..=config.election_timeout_max_ms);
        Instant::now() + Duration::from_millis(milliseconds)
    }

    /// Commit hard state before publishing status, sending votes, or replying to RPCs.
    async fn commit(&mut self, mut next: Election) -> Result<()> {
        let before = self.election.status();
        if before.role == Role::Leader && next.role != Role::Leader {
            next.deadline = Self::deadline(&self.config);
        }
        if next.hard != self.election.hard {
            let storage = Arc::clone(&self.storage);
            let hard = next.hard.clone();
            tokio::task::spawn_blocking(move || storage.save(&hard))
                .await?
                .context("persisting term/vote; node must stop on storage failure")?;
        }
        let after = next.status();
        if before != after {
            info!(
                "node={} role={:?}->{:?} term={}->{} leader={:?}",
                after.node_id,
                before.role,
                after.role,
                before.current_term,
                after.current_term,
                after.leader_id
            );
            if before.role != Role::Leader && after.role == Role::Leader {
                self.next_heartbeat = Instant::now();
            }
            self.status.send_replace(after);
        }
        self.election = next;
        Ok(())
    }

    fn broadcast(&mut self, outbound: Outbound) {
        for (&peer, client) in &self.clients {
            // Bound work even when a peer is down or a response is delayed.
            if !self.in_flight.insert(peer) {
                continue;
            }
            let mut client = client.clone();
            let outbound = outbound.clone();
            let limit = self.config.rpc_timeout();
            self.calls.spawn(async move {
                let sent_term = match &outbound {
                    Outbound::Vote(r) => r.term,
                    Outbound::Heartbeat(r) => r.term,
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
                        Outbound::Heartbeat(body) => {
                            let mut request = Request::new(body);
                            request.set_timeout(limit);
                            client
                                .heartbeat(request)
                                .await
                                .map(|r| Reply::Heartbeat(r.into_inner()))
                        }
                    }
                };
                let reply = timeout(limit, call)
                    .await
                    .unwrap_or_else(|_| Err(RpcStatus::deadline_exceeded("peer RPC timed out")));
                Completion {
                    peer,
                    sent_term,
                    reply,
                }
            });
        }
    }

    async fn handle_inbound(&mut self, event: Inbound) -> Result<()> {
        let mut next = self.election.clone();
        let deadline = Self::deadline(&self.config);
        match event {
            Inbound::Vote(request, reply) => {
                if !next.is_remote_member(request.candidate_id) || request.term == 0 {
                    warn!(
                        "node={} rejected invalid vote request candidate={} term={}",
                        next.id, request.candidate_id, request.term
                    );
                    let _ = reply.send(Err(RpcStatus::invalid_argument(
                        "candidate must be another configured member and term must be positive",
                    )));
                    return Ok(());
                }
                let response = next.request_vote(&request, deadline);
                debug!(
                    "node={} vote candidate={} requested_term={} granted={}",
                    next.id, request.candidate_id, request.term, response.vote_granted
                );
                self.commit(next).await?;
                let _ = reply.send(Ok(response));
            }
            Inbound::Heartbeat(request, reply) => {
                if !next.is_remote_member(request.leader_id) || request.term == 0 {
                    warn!(
                        "node={} rejected invalid heartbeat leader={} term={}",
                        next.id, request.leader_id, request.term
                    );
                    let _ = reply.send(Err(RpcStatus::invalid_argument(
                        "leader must be another configured member and term must be positive",
                    )));
                    return Ok(());
                }
                let response = next.heartbeat(&request, deadline);
                trace!(
                    "node={} heartbeat leader={} term={} accepted={}",
                    next.id, request.leader_id, request.term, response.accepted
                );
                self.commit(next).await?;
                let _ = reply.send(Ok(response));
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
                return Ok(());
            }
        };
        let mut next = self.election.clone();
        match reply {
            Reply::Vote(response) => {
                debug!(
                    "node={} peer={} vote response term={} granted={}",
                    next.id, completion.peer, response.term, response.vote_granted
                );
                next.vote_response(completion.peer, completion.sent_term, &response);
            }
            Reply::Heartbeat(response) => next.observe_term(response.term),
        }
        self.commit(next).await
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
                    if self.election.role == Role::Leader {
                        self.broadcast(Outbound::Heartbeat(HeartbeatRequest { term: self.election.hard.current_term, leader_id: self.config.id() }));
                        self.next_heartbeat = Instant::now() + Duration::from_millis(self.config.heartbeat_interval_ms);
                    } else {
                        let mut next = self.election.clone();
                        let request = next.start_election(Self::deadline(&self.config))?;
                        info!("node={} starting election term={}", next.id, request.term);
                        self.commit(next).await?;
                        self.broadcast(Outbound::Vote(request));
                    }
                }
            }
        }
        self.calls.abort_all();
        info!("node={} election task stopped", self.config.id());
        Ok(())
    }
}
