//! Real tonic transport, real durable directories, and independently restarted nodes.
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use axum::{
    body::{Body, to_bytes},
    http::{Request as HttpRequest, StatusCode},
};
use raft::{
    config::{Config, Peer},
    election::{Role, Status},
    node::{ClientError, Inbound, LogPage, Node, Proposal},
    proto::{
        AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse,
        election_server::{Election, ElectionServer},
    },
    replication::MAX_RPC_BYTES,
    transport::ElectionService,
};
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot, watch},
    task::JoinSet,
    time::{sleep, timeout},
};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status as RpcStatus};
use tower::ServiceExt;

// Cut links between two groups while all nodes and their timers keep running.
struct PartitionableService {
    id: u64,
    partition: Arc<AtomicU64>,
    inner: ElectionService,
}

impl PartitionableService {
    fn check_link(&self, remote: u64) -> Result<(), RpcStatus> {
        let group = self.partition.load(Ordering::SeqCst);
        if ((group >> self.id) & 1) != ((group >> remote) & 1) {
            Err(RpcStatus::unavailable("test network partition"))
        } else {
            Ok(())
        }
    }
}

#[tonic::async_trait]
impl Election for PartitionableService {
    async fn request_vote(
        &self,
        request: Request<VoteRequest>,
    ) -> Result<Response<VoteResponse>, RpcStatus> {
        self.check_link(request.get_ref().candidate_id)?;
        self.inner.request_vote(request).await
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, RpcStatus> {
        self.check_link(request.get_ref().leader_id)?;
        self.inner.append_entries(request).await
    }
}

struct Running {
    sender: mpsc::Sender<Inbound>,
    status: watch::Receiver<Status>,
    token: CancellationToken,
    tasks: JoinSet<Result<()>>,
}

struct Cluster {
    _dir: TempDir,
    configs: Vec<Config>,
    running: Vec<Option<Running>>,
    partition: Arc<AtomicU64>,
}

impl Cluster {
    async fn new(count: usize) -> Self {
        let dir = tempdir().unwrap();
        let mut listeners = Vec::new();
        for _ in 0..count {
            listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
        }
        let peers: Vec<_> = listeners
            .iter()
            .enumerate()
            .map(|(i, listener)| Peer {
                id: i as u64 + 1,
                address: format!("http://{}", listener.local_addr().unwrap()),
            })
            .collect();
        let configs = listeners
            .iter()
            .enumerate()
            .map(|(i, listener)| Config {
                node_id: Some(i as u64 + 1),
                grpc_bind: listener.local_addr().unwrap(),
                data_dir: dir.path().join(format!("node-{}", i + 1)),
                nodes: peers.clone(),
                heartbeat_interval_ms: 40,
                election_timeout_min_ms: 400,
                election_timeout_max_ms: 700,
                rpc_timeout_ms: 150,
                ..Config::default()
            })
            .collect();
        let mut cluster = Self {
            _dir: dir,
            configs,
            running: (0..count).map(|_| None).collect(),
            partition: Arc::new(AtomicU64::new(0)),
        };
        for (i, listener) in listeners.into_iter().enumerate() {
            cluster.start_with(i, listener).await;
        }
        cluster
    }

    async fn start_with(&mut self, i: usize, listener: TcpListener) {
        let (node, sender, status) = Node::new(self.configs[i].clone()).await.unwrap();
        let token = CancellationToken::new();
        let mut tasks = JoinSet::new();
        let child = token.clone();
        tasks.spawn(async move { node.run(child).await });
        let service = ElectionServer::new(PartitionableService {
            id: i as u64 + 1,
            partition: self.partition.clone(),
            inner: ElectionService::new(sender.clone(), self.configs[i].rpc_timeout()),
        })
        .max_decoding_message_size(MAX_RPC_BYTES)
        .max_encoding_message_size(MAX_RPC_BYTES);
        let child = token.clone();
        tasks.spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    child.cancelled_owned(),
                )
                .await?;
            Ok(())
        });
        self.running[i] = Some(Running {
            sender,
            status,
            token,
            tasks,
        });
    }

    async fn start(&mut self, i: usize) {
        assert!(self.running[i].is_none());
        let listener = TcpListener::bind(self.configs[i].grpc_bind).await.unwrap();
        self.start_with(i, listener).await;
    }

    async fn stop(&mut self, i: usize) {
        if let Some(mut running) = self.running[i].take() {
            running.token.cancel();
            timeout(Duration::from_secs(3), async {
                while let Some(result) = running.tasks.join_next().await {
                    result.unwrap().unwrap();
                }
            })
            .await
            .expect("node shutdown stalled");
        }
    }

    fn sender(&self, i: usize) -> mpsc::Sender<Inbound> {
        self.running[i].as_ref().unwrap().sender.clone()
    }

    async fn leader(&self) -> usize {
        self.leader_at_least(0).await
    }

    async fn leader_at_least(&self, min_term: u64) -> usize {
        timeout(Duration::from_secs(10), async {
            loop {
                let mut leaders: Vec<_> = self
                    .running
                    .iter()
                    .enumerate()
                    .filter_map(|(i, node)| {
                        let status = node.as_ref()?.status.borrow().clone();
                        (status.role == Role::Leader
                            && status.commit_index > 0
                            && status.current_term >= min_term)
                            .then_some((status.current_term, i))
                    })
                    .collect();
                leaders.sort_unstable();
                if let Some(&(_, i)) = leaders.last() {
                    return i;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("no leader elected")
    }

    async fn propose(&self, i: usize, command: Value) -> Proposal {
        let (reply, receiver) = oneshot::channel();
        self.sender(i)
            .send(Inbound::Propose(command, reply))
            .await
            .unwrap();
        timeout(Duration::from_secs(8), receiver)
            .await
            .expect("proposal did not commit")
            .unwrap()
            .unwrap()
    }

    async fn logs(&self, i: usize) -> LogPage {
        let (reply, receiver) = oneshot::channel();
        self.sender(i)
            .send(Inbound::ReadLog {
                from: 1,
                limit: 100,
                reply,
            })
            .await
            .unwrap();
        timeout(Duration::from_secs(3), receiver)
            .await
            .unwrap()
            .unwrap()
    }

    async fn caught_up(&self, index: u64) {
        timeout(Duration::from_secs(10), async {
            loop {
                let mut all = true;
                for i in 0..self.running.len() {
                    if self.running[i].is_some() {
                        let page = self.logs(i).await;
                        all &= page.commit_index >= index;
                    }
                }
                if all {
                    return;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("followers did not catch up");
    }

    async fn shutdown(&mut self) {
        for i in 0..self.running.len() {
            self.stop(i).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicates_batches_catches_up_offline_follower_and_recovers_cluster() {
    let mut cluster = Cluster::new(3).await;
    let leader = cluster.leader().await;
    let lagging = (leader + 1) % 3;
    cluster.stop(lagging).await;
    let mut last = 0;
    // More than a single count-bounded batch, including a null command.
    for n in 0..70 {
        last = cluster
            .propose(leader, if n == 0 { Value::Null } else { json!({"n": n}) })
            .await
            .index;
    }
    let expected = serde_json::to_value(cluster.logs(leader).await.entries).unwrap();
    cluster.start(lagging).await;
    cluster.caught_up(last).await;
    for i in 0..3 {
        assert_eq!(
            serde_json::to_value(cluster.logs(i).await.entries).unwrap(),
            expected
        );
    }

    cluster.shutdown().await;
    for i in 0..3 {
        cluster.start(i).await;
    }
    let new_leader = cluster.leader().await;
    let proposal = cluster.propose(new_leader, json!("after restart")).await;
    cluster.caught_up(proposal.index).await;
    for i in 0..3 {
        let page = cluster.logs(i).await;
        assert_eq!(
            serde_json::to_value(&page.entries[..last as usize]).unwrap(),
            expected
        );
        assert_eq!(
            page.entries[(proposal.index - 1) as usize].command,
            Some(json!("after restart"))
        );
    }
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_quorum_never_succeeds_and_new_leader_repairs_divergent_suffix() {
    let mut cluster = Cluster::new(3).await;
    let old = cluster.leader().await;
    let base = cluster.propose(old, json!("committed prefix")).await;
    cluster.caught_up(base.index).await;
    let old_term = cluster.running[old]
        .as_ref()
        .unwrap()
        .status
        .borrow()
        .current_term;
    cluster.partition.store(1 << (old + 1), Ordering::SeqCst);
    let (reply, mut receiver) = oneshot::channel();
    cluster
        .sender(old)
        .send(Inbound::Propose(
            json!("isolated uncommitted command"),
            reply,
        ))
        .await
        .unwrap();
    let page = cluster.logs(old).await;
    assert_eq!(page.last_log_index, base.index + 1);
    assert_eq!(page.commit_index, base.index);
    assert!(matches!(
        receiver.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    let leader = cluster.leader_at_least(old_term + 1).await;
    let committed = cluster.propose(leader, json!("majority command")).await;
    assert!(matches!(
        receiver.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    cluster.partition.store(0, Ordering::SeqCst);
    assert!(matches!(
        timeout(Duration::from_secs(5), receiver)
            .await
            .unwrap()
            .unwrap(),
        Err(ClientError::OutcomeUnknown)
    ));
    cluster.caught_up(committed.index).await;
    let expected = serde_json::to_value(cluster.logs(leader).await.entries).unwrap();
    for i in 0..3 {
        let page = cluster.logs(i).await;
        assert_eq!(serde_json::to_value(&page.entries).unwrap(), expected);
        assert_eq!(
            page.entries[(base.index - 1) as usize].command,
            Some(json!("committed prefix"))
        );
        assert!(
            !page
                .entries
                .iter()
                .any(|entry| entry.command == Some(json!("isolated uncommitted command")))
        );
    }
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rest_proposals_and_paginated_inspection_on_every_node() {
    let mut cluster = Cluster::new(3).await;
    let leader = cluster.leader().await;
    let running = cluster.running[leader].as_ref().unwrap();
    let app = raft::api::router(running.status.clone(), running.sender.clone());
    let response = app
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri("/api/logs")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"command":{"operation":"example"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let proposal: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
    let index = proposal["index"].as_u64().unwrap();
    cluster.caught_up(index).await;

    for i in 0..3 {
        let running = cluster.running[i].as_ref().unwrap();
        let app = raft::api::router(running.status.clone(), running.sender.clone());
        let response = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .uri(format!("/api/logs?from={index}&limit=1"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let page: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(page["node_id"], i as u64 + 1);
        assert_eq!(page["entries"][0]["index"], index);
        assert_eq!(
            page["entries"][0]["command"],
            json!({"operation": "example"})
        );
        assert_eq!(page["entries"][0]["committed"], true);
        if i != leader {
            let response = app
                .oneshot(
                    HttpRequest::builder()
                        .method("POST")
                        .uri("/api/logs")
                        .header("content-type", "application/json")
                        .body(Body::from(r#"{"command":null}"#))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::CONFLICT);
        }
    }
    cluster.shutdown().await;
}
