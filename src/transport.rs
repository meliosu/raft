use std::time::Duration;

use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use tonic::{Request, Response, Status};

use crate::{
    node::Inbound,
    proto::{
        HeartbeatRequest, HeartbeatResponse, VoteRequest, VoteResponse, election_server::Election,
    },
};

#[derive(Clone)]
pub struct ElectionService {
    sender: mpsc::Sender<Inbound>,
    timeout: Duration,
}

impl ElectionService {
    pub fn new(sender: mpsc::Sender<Inbound>, timeout: Duration) -> Self {
        Self { sender, timeout }
    }

    async fn dispatch<T>(
        &self,
        event: Inbound,
        receiver: oneshot::Receiver<Result<T, Status>>,
    ) -> Result<Response<T>, Status> {
        timeout(self.timeout, async {
            self.sender
                .send(event)
                .await
                .map_err(|_| Status::unavailable("election task stopped"))?;
            receiver
                .await
                .map_err(|_| Status::unavailable("election task stopped"))?
                .map(Response::new)
        })
        .await
        .map_err(|_| Status::deadline_exceeded("election task busy"))?
    }
}

#[tonic::async_trait]
impl Election for ElectionService {
    async fn request_vote(
        &self,
        request: Request<VoteRequest>,
    ) -> Result<Response<VoteResponse>, Status> {
        let (reply, receiver) = oneshot::channel();
        self.dispatch(Inbound::Vote(request.into_inner(), reply), receiver)
            .await
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let (reply, receiver) = oneshot::channel();
        self.dispatch(Inbound::Heartbeat(request.into_inner(), reply), receiver)
            .await
    }
}
