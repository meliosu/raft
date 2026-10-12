pub mod api;
pub mod config;
pub mod election;
pub mod node;
pub mod storage;
pub mod transport;

pub mod proto {
    tonic::include_proto!("raft.election.v1");
}
