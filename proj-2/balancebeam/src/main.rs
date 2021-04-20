mod request;
mod response;

use clap::Clap;
use std::{
    collections::VecDeque,
    sync::{atomic::AtomicU8, Arc},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{channel, Receiver, Sender},
        Mutex,
    },
};

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
}

type UpStreamStatus = AtomicU8;
const STATUS_HEATHLY: u8 = 0;
const STATUS_UNAVAILABLE: u8 = 1;

#[derive(Debug, thiserror::Error)]
enum BBErr {
    #[error("fail to reuse tcp connection {0}")]
    ReuseConnectionErr(String),

    #[error("fail to connection upstream {0}")]
    BuildConnectionErr(String),
}

/// Contains information about the state of balancebeam (e.g. what servers we are currently proxying
/// to, what servers have failed, rate limiting counts, etc.)
///
/// You should add fields to this struct in later milestones.
struct ProxyState {
    /// How frequently we check whether upstream servers are alive (Milestone 4)
    #[allow(dead_code)]
    active_health_check_interval: usize,
    /// Where we should send requests when doing active health checks (Milestone 4)
    #[allow(dead_code)]
    active_health_check_path: String,
    /// Maximum number of requests an individual IP can make in a minute (Milestone 5)
    #[allow(dead_code)]
    max_requests_per_minute: usize,

    upstream_address: String,

    max_conn: usize,

    pool: VecDeque<TcpStream>,

    conn_cnt: usize,

    upstream_status: Arc<UpStreamStatus>,
}

impl ProxyState {
    /// Return None if there are no idle connections for task and task should wait for a while or be attach to another UpStream.
    /// Note: This method return error if and only if it can't build new connections.
    async fn get_idle_conn(&mut self) -> Result<Option<TcpStream>, BBErr> {
        if self.conn_cnt < self.max_conn {
            let s = connect_to_upstream(self).await.map_err(|e| {
                self.try_mark_unavailable();
                e
            })?;
            self.pool.push_back(s);
            self.conn_cnt += 1;
        }

        Ok(self.pool.pop_front())
    }

    /// Task use this function to destroy unavailable functions.
    fn destory_conn(&mut self, _: TcpStream) {
        self.conn_cnt -= 1;
    }

    fn try_mark_unavailable(&mut self) {
        // Double check
        if self.conn_cnt == 0 {
            self.upstream_status
                .store(STATUS_UNAVAILABLE, std::sync::atomic::Ordering::SeqCst);
        }
    }
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
        let upstream_status = Arc::new(AtomicU8::new(STATUS_HEATHLY));
        upstreams.push((upstream_status.clone(), upstream.clone()));

        let state = Arc::new(Mutex::new(ProxyState {
            upstream_address: upstream.clone(),
            active_health_check_interval: options.active_health_check_interval,

            // refactor
            active_health_check_path: options.active_health_check_path.clone(),
            max_requests_per_minute: options.max_requests_per_minute,

            max_conn: 128,
            pool: VecDeque::with_capacity(128),
            conn_cnt: 0,
            upstream_status,
        }));

        proxy_states.push(state);
    }

    let task_q_size = 128usize;
    let (task_sender, task_queue) = channel(task_q_size);
    let task_sender_backup = task_sender.clone();
    tokio::spawn(dispatcher(
        upstreams,
        proxy_states,
        task_queue,
        task_sender_backup,
    ));

    while let Ok((stream, _)) = listener.accept().await {
        task_sender.send(stream).await.expect("dispatcher died");
    }

    Ok(())
}

/// Dispatcher owns ProxyState list.
async fn dispatcher(
    upstreams: Vec<(Arc<AtomicU8>, String)>,
    proxy_states: Vec<Arc<Mutex<ProxyState>>>,
    mut task_queue: Receiver<TcpStream>,
    task_sender: Sender<TcpStream>,
) {
    let mut index = 0usize;

    while let Some(stream) = task_queue.recv().await {
        for _ in 0..upstreams.len() {
            index = (index + 1) % upstreams.len();
            if let Some((state, _)) = upstreams.get(index) {
                if state.load(std::sync::atomic::Ordering::SeqCst) == STATUS_HEATHLY {
                    break;
                }
            }
        }

        let redispatcher = task_sender.clone();
        let state = proxy_states.get(index).unwrap().clone();
        tokio::spawn(handle_connection(stream, state, redispatcher));
    }
}

/// Return error if failed to connect upstream server.
async fn connect_to_upstream(state: &ProxyState) -> Result<TcpStream, BBErr> {
    let upstream_ip = &state.upstream_address;

    // Keep-alive ?
    TcpStream::connect(upstream_ip).await.or_else(|err| {
        log::error!("Failed to connect to upstream {}: {}", upstream_ip, err);
        Err(BBErr::BuildConnectionErr(upstream_ip.clone()))
    })
    // TODO: implement failover (milestone 3)
}

async fn send_response(client_conn: &mut TcpStream, response: &http::Response<Vec<u8>>) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!(
        "{} <- {}",
        client_ip,
        response::format_response_line(&response)
    );
    if let Err(error) = response::write_to_stream(&response, client_conn).await {
        log::warn!("Failed to send response to client: {}", error);
        return;
    }
}

/// Handling incomming connection.
/// This function try to return back the TcpStream for reusing or destory it while unavailable.
async fn handle_connection(
    mut client_conn: TcpStream,
    state: Arc<Mutex<ProxyState>>,
    redispatcher: Sender<TcpStream>,
) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!("Connection received from {}", client_ip);

    // Open a connection to a random destination server
    let conn = state.lock().await.get_idle_conn().await;

    match conn {
        Ok(Some(mut upstream_conn)) => {
            match process_task(client_ip, &mut client_conn, &mut upstream_conn).await {
                // Return the connection back for re-using.
                Ok(()) => state.lock().await.pool.push_back(upstream_conn),
                Err(BBErr::ReuseConnectionErr(_)) => state.lock().await.destory_conn(upstream_conn),
                _ => unreachable!(),
            }
        }
        Ok(None) => {
            let _ = redispatcher.send(client_conn).await;
            return;
        }
        Err(BBErr::BuildConnectionErr(_)) => {
            // Error is throwed from `connect_to_upstream` and hints that upstream is unavailable.
            let _ = redispatcher.send(client_conn).await;
            state.lock().await.try_mark_unavailable();
            return;
        }
        _ => unreachable!(),
    }
}

/// Relay requests from client to an upstream server.
/// Return true if the connectin is available.
async fn process_task(
    client_ip: String,
    client_conn: &mut TcpStream,
    upstream_conn: &mut TcpStream,
) -> Result<(), BBErr> {
    loop {
        // Read a request from the client
        let upstream_ip = upstream_conn.peer_addr().unwrap().ip().to_string();

        let mut request = match request::read_from_stream(client_conn).await {
            Ok(request) => request,
            // Handle case where client closed connection and is no longer sending requests
            Err(request::Error::IncompleteRequest(0)) => {
                log::debug!("Client finished sending requests. Shutting down connection");
                return Ok(());
            }
            // Handle I/O error in reading from the client
            Err(request::Error::ConnectionError(io_err)) => {
                log::info!("Error reading request from client stream: {}", io_err);
                return Ok(());
            }
            Err(error) => {
                log::debug!("Error parsing request: {:?}", error);
                let response = response::make_http_error(match error {
                    request::Error::IncompleteRequest(_)
                    | request::Error::MalformedRequest(_)
                    | request::Error::InvalidContentLength
                    | request::Error::ContentLengthMismatch => http::StatusCode::BAD_REQUEST,
                    request::Error::RequestBodyTooLarge => http::StatusCode::PAYLOAD_TOO_LARGE,
                    request::Error::ConnectionError(_) => http::StatusCode::SERVICE_UNAVAILABLE,
                });
                send_response(client_conn, &response).await;
                continue;
            }
        };
        log::info!(
            "{} -> {}: {}",
            client_ip,
            upstream_ip,
            request::format_request_line(&request)
        );

        // Add X-Forwarded-For header so that the upstream server knows the client's IP address.
        // (We're the ones connecting directly to the upstream server, so without this header, the
        // upstream server will only know our IP, not the client's.)
        request::extend_header_value(&mut request, "x-forwarded-for", &client_ip);

        // Forward the request to the server
        if let Err(error) = request::write_to_stream(&request, upstream_conn).await {
            log::error!(
                "Failed to send request to upstream {}: {}",
                upstream_ip,
                error
            );
            let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
            send_response(client_conn, &response).await;
            return Err(BBErr::ReuseConnectionErr(upstream_ip.clone()));
        }
        log::debug!("Forwarded request to server");

        // Read the server's response
        let response = match response::read_from_stream(upstream_conn, request.method()).await {
            Ok(response) => response,
            Err(error) => {
                log::error!("Error reading response from server: {:?}", error);
                let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
                send_response(client_conn, &response).await;
                return Err(BBErr::ReuseConnectionErr(upstream_ip.clone()));
            }
        };
        // Forward the response to the client
        send_response(client_conn, &response).await;
        log::debug!("Forwarded response to client");
    }
}
