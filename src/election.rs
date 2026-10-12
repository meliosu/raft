//! Election rules without networking or filesystem I/O.
use std::{collections::HashSet, sync::Arc};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::{
    proto::{HeartbeatRequest, HeartbeatResponse, VoteRequest, VoteResponse},
    storage::HardState,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Status {
    pub node_id: u64,
    pub role: Role,
    pub current_term: u64,
    pub leader_id: Option<u64>,
}

#[derive(Clone)]
pub struct Election {
    pub id: u64,
    pub hard: HardState,
    pub role: Role,
    pub leader_id: Option<u64>,
    pub deadline: Instant,
    members: Arc<HashSet<u64>>,
    votes: HashSet<u64>,
    // Extension point: load these from durable log storage when adding replication.
    last_log_index: u64,
    last_log_term: u64,
}

impl Election {
    pub fn new(
        id: u64,
        members: impl IntoIterator<Item = u64>,
        hard: HardState,
        deadline: Instant,
    ) -> Self {
        Self {
            id,
            hard,
            role: Role::Follower,
            leader_id: None,
            deadline,
            members: Arc::new(members.into_iter().collect()),
            votes: HashSet::new(),
            last_log_index: 0,
            last_log_term: 0,
        }
    }

    pub fn is_remote_member(&self, id: u64) -> bool {
        id != self.id && self.members.contains(&id)
    }

    pub fn status(&self) -> Status {
        Status {
            node_id: self.id,
            role: self.role,
            current_term: self.hard.current_term,
            leader_id: self.leader_id,
        }
    }

    pub fn observe_term(&mut self, term: u64) {
        if term > self.hard.current_term {
            self.hard = HardState {
                current_term: term,
                voted_for: None,
            };
            self.follow(None);
        }
    }

    fn follow(&mut self, leader: Option<u64>) {
        self.role = Role::Follower;
        self.leader_id = leader;
        self.votes.clear();
    }

    pub fn start_election(&mut self, deadline: Instant) -> Result<VoteRequest> {
        self.hard.current_term = self
            .hard
            .current_term
            .checked_add(1)
            .context("term counter exhausted")?;
        self.hard.voted_for = Some(self.id);
        self.role = Role::Candidate;
        self.leader_id = None;
        self.votes = HashSet::from([self.id]);
        self.deadline = deadline;
        self.try_become_leader();
        Ok(VoteRequest {
            term: self.hard.current_term,
            candidate_id: self.id,
            last_log_index: self.last_log_index,
            last_log_term: self.last_log_term,
        })
    }

    pub fn request_vote(&mut self, request: &VoteRequest, deadline: Instant) -> VoteResponse {
        self.observe_term(request.term);
        let up_to_date = (request.last_log_term, request.last_log_index)
            >= (self.last_log_term, self.last_log_index);
        let granted = request.term == self.hard.current_term
            && up_to_date
            && self
                .hard
                .voted_for
                .is_none_or(|id| id == request.candidate_id);
        if granted {
            self.hard.voted_for = Some(request.candidate_id);
            self.deadline = deadline;
        }
        VoteResponse {
            term: self.hard.current_term,
            vote_granted: granted,
        }
    }

    pub fn heartbeat(
        &mut self,
        request: &HeartbeatRequest,
        deadline: Instant,
    ) -> HeartbeatResponse {
        self.observe_term(request.term);
        let accepted = request.term == self.hard.current_term;
        if accepted {
            self.follow(Some(request.leader_id));
            self.deadline = deadline;
        }
        HeartbeatResponse {
            term: self.hard.current_term,
            accepted,
        }
    }

    pub fn vote_response(&mut self, peer: u64, election_term: u64, response: &VoteResponse) {
        // A higher term always wins, even from an RPC sent in an earlier election.
        self.observe_term(response.term);
        if self.role == Role::Candidate
            && election_term == self.hard.current_term
            && response.term == election_term
            && response.vote_granted
            && self.is_remote_member(peer)
        {
            self.votes.insert(peer);
            self.try_become_leader();
        }
    }

    fn try_become_leader(&mut self) {
        if self.votes.len() > self.members.len() / 2 {
            self.role = Role::Leader;
            self.leader_id = Some(self.id);
            self.votes.clear();
        }
    }
}
