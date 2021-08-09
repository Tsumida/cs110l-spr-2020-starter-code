mod request;
mod response;
mod scheduler;
use clap::Clap;
use std::{
    collections::VecDeque,
    sync::{atomic::AtomicU8, Arc},
};
use tokio::{
    net::TcpListener,
    sync::{mpsc::channel, Mutex},
};

use crate::scheduler::{dispatcher, ProxyState, DEFAULT_MAX_CONN, STATUS_HEALTHY};

/// Contains information parsed from the command-line invocation of balancebeam. The Clap macros
/// provide a fancy way to automatically construct a command-line argument parser.
#[derive(Clap, Debug)]
#[clap(about = "Fun with load balancing")]
struct CmdOptions {
    #[clap(
        short,
        long,
        about = "IP/port to bind to",
        default_value = "127.0.0.1:2333"
    )]
    bind: String,
    #[clap(short, long, about = "Upstream host to forward requests to")]
    upstream: Vec<String>,
    #[clap(
        long,
        about = "Perform active health checks on this interval (in seconds)",
        default_value = "10"
    )]
    active_health_check_interval: usize,
    #[clap(
        long,
        about = "Path to send request to for active health checks",
        default_value = "/"
    )]
    active_health_check_path: String,
    #[clap(
        long,
        about = "Maximum number of requests to accept per IP per minute (0 = unlimited)",
        default_value = "0"
    )]
    max_requests_per_minute: usize,
    #[clap(
        long,
        about = "Maximum number of connection per upstream(default = 1)",
        default_value = "0"
    )]
    max_conn: usize,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Initialize the logging library. You can print log messages using the `log` macros:
    // https://docs.rs/log/0.4.8/log/ You are welcome to continue using print! statements; this
    // just looks a little prettier.
    if let Err(_) = std::env::var("RUST_LOG") {
        std::env::set_var("RUST_LOG", "debug");
    }
    pretty_env_logger::init();

    // Parse the command line arguments passed to this program
    let options = CmdOptions::parse();
    if options.upstream.len() < 1 {
        log::error!("At least one upstream server must be specified using the --upstream option.");
        std::process::exit(1);
    }

    let max_conn = usize::max(DEFAULT_MAX_CONN, options.max_conn);

    // Start listening for connections
    let listener = match TcpListener::bind(&options.bind).await {
        Ok(listener) => listener,
        Err(err) => {
            log::error!("Could not bind to {}: {}", options.bind, err);
            std::process::exit(1);
        }
    };
    log::info!("Listening for requests on {}", options.bind);

    // Handle incoming connections

    let mut upstreams = vec![];
    let mut proxy_states = Vec::new();

    for upstream in options.upstream {
        println!("add upstream sever addr: {}", upstream);

        let upstream_status = Arc::new(AtomicU8::new(STATUS_HEALTHY));
        upstreams.push((upstream_status.clone(), upstream.clone()));

        let state = Arc::new(Mutex::new(ProxyState::new(
            options.active_health_check_interval,
            options.active_health_check_path.clone(),
            options.max_requests_per_minute,
            upstream,
            max_conn,
            VecDeque::with_capacity(max_conn),
            0,
            upstream_status,
        )));

        proxy_states.push(state);
    }

    // Global task queue for all incoming connections.
    let task_q_size = 256usize;
    let (task_sender, task_queue) = channel(task_q_size);

    tokio::spawn(dispatcher(
        upstreams,
        proxy_states,
        task_queue,
        task_sender.clone(),
    ));

    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            task_sender.send(stream).await.expect("dispatcher died");
        }
    });

    if tokio::signal::ctrl_c().await.is_ok() {
        log::info!("BB down");
    }

    Ok(())
}
