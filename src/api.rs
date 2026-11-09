use std::time::Duration;

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use log::trace;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::timeout,
};

use crate::{
    election::Status,
    node::{ClientError, Inbound, LogPage, Proposal},
    replication::{MAX_COMMAND_BYTES, MAX_PAGE_ENTRIES, validate_command},
};

const CLIENT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
struct ApiState {
    status: watch::Receiver<Status>,
    sender: mpsc::Sender<Inbound>,
}

pub fn router(status: watch::Receiver<Status>, sender: mpsc::Sender<Inbound>) -> Router {
    Router::new()
        .route("/api/status", get(get_status))
        .route("/api/logs", get(get_logs).post(post_log))
        .layer(DefaultBodyLimit::max(MAX_COMMAND_BYTES + 1024))
        .with_state(ApiState { status, sender })
}

async fn get_status(State(state): State<ApiState>) -> Result<Json<Status>, ClientError> {
    if state.sender.is_closed() {
        return Err(ClientError::Unavailable);
    }
    trace!("node={} GET /api/status", state.status.borrow().node_id);
    Ok(Json(state.status.borrow().clone()))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LogQuery {
    #[serde(default = "first_index")]
    from: u64,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn first_index() -> u64 {
    1
}
fn default_limit() -> usize {
    MAX_PAGE_ENTRIES
}

async fn get_logs(
    State(state): State<ApiState>,
    Query(query): Query<LogQuery>,
) -> Result<Json<LogPage>, ClientError> {
    if query.from == 0 || query.limit == 0 || query.limit > MAX_PAGE_ENTRIES {
        return Err(ClientError::InvalidCommand);
    }
    trace!(
        "node={} GET /api/logs from={} limit={}",
        state.status.borrow().node_id,
        query.from,
        query.limit
    );
    timeout(CLIENT_TIMEOUT, async {
        let (reply, receiver) = oneshot::channel();
        state
            .sender
            .send(Inbound::ReadLog {
                from: query.from,
                limit: query.limit,
                reply,
            })
            .await
            .map_err(|_| ClientError::Unavailable)?;
        receiver
            .await
            .map(Json)
            .map_err(|_| ClientError::Unavailable)
    })
    .await
    .map_err(|_| ClientError::Busy)?
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Command {
    command: Value,
}

async fn post_log(
    State(state): State<ApiState>,
    Json(body): Json<Command>,
) -> Result<(StatusCode, Json<Proposal>), ClientError> {
    if validate_command(&body.command).is_err() {
        return Err(ClientError::InvalidCommand);
    }
    timeout(CLIENT_TIMEOUT, async {
        let (reply, receiver) = oneshot::channel();
        state
            .sender
            .send(Inbound::Propose(body.command, reply))
            .await
            .map_err(|_| ClientError::Unavailable)?;
        let result = receiver.await.map_err(|_| ClientError::OutcomeUnknown)??;
        Ok((StatusCode::CREATED, Json(result)))
    })
    .await
    .map_err(|_| ClientError::OutcomeUnknown)?
}

impl IntoResponse for ClientError {
    fn into_response(self) -> Response {
        let (status, body) = match self {
            Self::NotLeader { leader_id } => (
                StatusCode::CONFLICT,
                json!({ "error": "not_leader", "leader_id": leader_id }),
            ),
            Self::Busy => (StatusCode::SERVICE_UNAVAILABLE, json!({ "error": "busy" })),
            Self::InvalidCommand => (
                StatusCode::BAD_REQUEST,
                json!({ "error": "invalid_request" }),
            ),
            Self::OutcomeUnknown => (
                StatusCode::GATEWAY_TIMEOUT,
                json!({ "error": "outcome_unknown", "message": "The entry may still commit; retrying can duplicate the command." }),
            ),
            Self::Unavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                json!({ "error": "unavailable" }),
            ),
        };
        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::election::Role;
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use tower::ServiceExt;

    fn app() -> (Router, mpsc::Receiver<Inbound>) {
        let (_, status) = watch::channel(Status {
            node_id: 1,
            role: Role::Follower,
            current_term: 3,
            leader_id: Some(2),
            last_log_index: 5,
            commit_index: 4,
        });
        let (sender, receiver) = mpsc::channel(8);
        (router(status, sender), receiver)
    }

    async fn body(response: Response) -> Value {
        serde_json::from_slice(
            &to_bytes(response.into_body(), 4 * 1024 * 1024)
                .await
                .unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn status_includes_log_positions() {
        let (app, _receiver) = app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let value = body(response).await;
        assert_eq!(value["last_log_index"], 5);
        assert_eq!(value["commit_index"], 4);
    }

    #[tokio::test]
    async fn log_query_is_bounded_and_passes_pagination_to_actor() {
        for query in ["from=0", "limit=0", "limit=101", "from=wrong", "unknown=1"] {
            let (app, _receiver) = app();
            let response = app
                .oneshot(
                    Request::builder()
                        .uri(format!("/api/logs?{query}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
        let (app, mut receiver) = app();
        let call = tokio::spawn(
            app.oneshot(
                Request::builder()
                    .uri("/api/logs?from=3&limit=2")
                    .body(Body::empty())
                    .unwrap(),
            ),
        );
        let Inbound::ReadLog { from, limit, reply } = receiver.recv().await.unwrap() else {
            panic!("expected log query")
        };
        assert_eq!((from, limit), (3, 2));
        reply
            .send(LogPage {
                node_id: 1,
                current_term: 3,
                commit_index: 4,
                last_log_index: 5,
                entries: vec![],
                next_index: Some(5),
            })
            .unwrap();
        let response = call.await.unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body(response).await["next_index"], 5);
    }

    #[tokio::test]
    async fn proposal_preserves_null_and_returns_known_leader_on_follower() {
        let (app, mut receiver) = app();
        let call = tokio::spawn(
            app.oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/logs")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"command":null}"#))
                    .unwrap(),
            ),
        );
        let Inbound::Propose(command, reply) = receiver.recv().await.unwrap() else {
            panic!("expected proposal")
        };
        assert_eq!(command, Value::Null);
        reply
            .send(Err(ClientError::NotLeader { leader_id: Some(2) }))
            .unwrap();
        let response = call.await.unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let value = body(response).await;
        assert_eq!(value["error"], "not_leader");
        assert_eq!(value["leader_id"], 2);
    }

    #[tokio::test(start_paused = true)]
    async fn proposal_timeout_is_unknown_and_does_not_claim_rollback() {
        let (app, mut receiver) = app();
        let call = tokio::spawn(
            app.oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/logs")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"command":"test"}"#))
                    .unwrap(),
            ),
        );
        let Inbound::Propose(_, reply) = receiver.recv().await.unwrap() else {
            panic!("expected proposal")
        };
        let response = call.await.unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(body(response).await["error"], "outcome_unknown");
        assert!(reply.is_closed());
    }

    #[tokio::test]
    async fn rejects_oversized_proposals_and_reports_stopped_actor() {
        let (app, _receiver) = app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/logs")
                    .header("content-type", "application/json")
                    .body(Body::from("x".repeat(MAX_COMMAND_BYTES + 2048)))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let (app, receiver) = self::app();
        drop(receiver);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
