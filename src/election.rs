//! Election rules without networking or filesystem I/O.
use std::{collections::HashSet, sync::Arc};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::{
    proto::{VoteRequest, VoteResponse},
    storage::HardState,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn election() -> Election {
        Election::new(
            1,
            [1, 2, 3],
            HardState {
                current_term: 4,
                voted_for: None,
            },
            Instant::now(),
        )
    }

    #[test]
    fn votes_compare_last_term_before_length_and_vote_once_per_term() {
        let mut election = election();
        election.set_log_tail(5, 3);
        let mut req = VoteRequest {
            term: 4,
            candidate_id: 2,
            last_log_index: 100,
            last_log_term: 2,
        };
        assert!(!election.request_vote(&req, Instant::now()).vote_granted);
        req.last_log_term = 3;
        req.last_log_index = 4;
        assert!(!election.request_vote(&req, Instant::now()).vote_granted);
        req.last_log_term = 4;
        req.last_log_index = 1;
        assert!(election.request_vote(&req, Instant::now()).vote_granted);
        assert!(election.request_vote(&req, Instant::now()).vote_granted);
        req.candidate_id = 3;
        assert!(!election.request_vote(&req, Instant::now()).vote_granted);
    }

    #[test]
    fn election_advertises_real_tail_and_ignores_obsolete_votes() {
        let mut election = election();
        election.set_log_tail(12, 4);
        let req = election.start_election(Instant::now()).unwrap();
        assert_eq!((req.last_log_index, req.last_log_term), (12, 4));
        election.vote_response(
            2,
            4,
            &VoteResponse {
                term: 4,
                vote_granted: true,
            },
        );
        assert_eq!(election.role, Role::Candidate);
        election.vote_response(
            2,
            5,
            &VoteResponse {
                term: 5,
                vote_granted: true,
            },
        );
        assert_eq!(election.role, Role::Leader);
        election.vote_response(
            3,
            4,
            &VoteResponse {
                term: 6,
                vote_granted: false,
            },
        );
        assert_eq!(election.role, Role::Follower);
        assert_eq!(election.hard.current_term, 6);
        assert_eq!(election.hard.voted_for, None);
    }

    #[test]
    fn current_leader_contact_steps_down_and_resets_timer_but_stale_contact_does_not() {
        let mut election = election();
        election.start_election(Instant::now()).unwrap();
        let old_deadline = election.deadline;
        let deadline = Instant::now() + Duration::from_secs(1);
        assert!(!election.accept_leader(4, 2, deadline));
        assert_eq!(election.deadline, old_deadline);
        assert_eq!(election.role, Role::Candidate);
        assert!(election.accept_leader(5, 2, deadline));
        assert_eq!(election.deadline, deadline);
        assert_eq!(election.role, Role::Follower);
        assert_eq!(election.leader_id, Some(2));
        assert_eq!(election.hard.voted_for, Some(1));
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Status {
    pub node_id: u64,
    pub role: Role,
    pub current_term: u64,
    pub leader_id: Option<u64>,
    pub last_log_index: u64,
    pub commit_index: u64,
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

    pub fn set_log_tail(&mut self, index: u64, term: u64) {
        self.last_log_index = index;
        self.last_log_term = term;
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

    pub fn accept_leader(&mut self, term: u64, leader_id: u64, deadline: Instant) -> bool {
        self.observe_term(term);
        if term == self.hard.current_term {
            self.follow(Some(leader_id));
            self.deadline = deadline;
            true
        } else {
            false
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
