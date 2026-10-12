use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use log::{error, info};
use raft::{
    api, config::Config, node::Node, proto::election_server::ElectionServer,
    transport::ElectionService,
};
use tokio::{net::TcpListener, task::JoinSet};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(version, about = "Raft leader election node")]
struct Args {
    #[arg(long, env = "RAFT_CONFIG_PATH", default_value = "config.yaml")]
    config: PathBuf,
    #[arg(long, env = "RAFT_NODE_ID")]
    node_id: Option<u64>,
    #[arg(long, env = "RAFT_DATA_DIR")]
    data_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            error!("node stopped: {error:#}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let args = Args::parse();
    let mut config = Config::load(&args.config)?;
    if let Some(id) = args.node_id {
        config.node_id = Some(id);
    }
    if let Some(dir) = args.data_dir {
        config.data_dir = dir;
    }
    config.validate()?;
    let grpc_listener = TcpListener::bind(config.grpc_bind)
        .await
        .context("binding gRPC listener")?;
    let http_listener = TcpListener::bind(config.http_bind)
        .await
        .context("binding HTTP listener")?;
    let (node, sender, status) = Node::new(config.clone()).await?;
    let shutdown = CancellationToken::new();
    let mut tasks = JoinSet::new();
    let token = shutdown.clone();
    tasks.spawn(async move { node.run(token).await });
    let service = ElectionServer::new(ElectionService::new(sender, config.rpc_timeout()))
        .max_decoding_message_size(4096)
        .max_encoding_message_size(4096);
    let token = shutdown.clone();
    tasks.spawn(async move {
        tonic::transport::Server::builder()
            .concurrency_limit_per_connection(64)
            .add_service(service)
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(grpc_listener),
                token.cancelled_owned(),
            )
            .await
            .context("gRPC server")
    });
    let token = shutdown.clone();
    tasks.spawn(async move {
        axum::serve(http_listener, api::router(status))
            .with_graceful_shutdown(token.cancelled_owned())
            .await
            .context("HTTP server")
    });
    info!(
        "node={} listening grpc={} http={} members={} data={}",
        config.id(),
        config.grpc_bind,
        config.http_bind,
        config.nodes.len(),
        config.data_dir.display()
    );
    let first = tokio::select! {
        signal = shutdown_signal() => signal,
        result = tasks.join_next() => result.context("no server tasks")?.context("server task panicked")?,
    };
    shutdown.cancel();
    // Bound draining so a client holding a connection cannot stall SIGTERM forever.
    let drain = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Some(result) = tasks.join_next().await {
            result.context("server task panicked")??;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await;
    tasks.abort_all();
    first?;
    if let Ok(result) = drain {
        result?;
    }
    Ok(())
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = term.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    info!("shutdown requested");
    Ok(())
}
