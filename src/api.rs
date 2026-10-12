use axum::{Json, Router, extract::State, routing::get};
use log::trace;
use tokio::sync::watch;

use crate::election::Status;

pub fn router(status: watch::Receiver<Status>) -> Router {
    Router::new()
        .route("/api/status", get(get_status))
        .with_state(status)
}

async fn get_status(State(status): State<watch::Receiver<Status>>) -> Json<Status> {
    let snapshot = status.borrow().clone();
    trace!("node={} GET /api/status", snapshot.node_id);
    Json(snapshot)
}
