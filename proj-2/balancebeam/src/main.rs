mod request;
mod response;

use clap::Clap;
use std::{
    collections::{HashMap, VecDeque},
    sync::{atomic::AtomicU64, Arc},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
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
}

impl ProxyState {
    async fn get_avaliable_conn(&mut self) -> Result<Option<TcpStream>, std::io::Error> {
        // drop som conn.
        while self.pool.len() > self.max_conn {
            self.pool.pop_front();
        }

        if self.pool.len() == 0 {
            let s = connect_to_upstream(self).await?;
            self.pool.push_back(s);
        }

        Ok(self.pool.pop_front())
    }

    fn close_conn() {}
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
        let state = Arc::new(Mutex::new(ProxyState {
            upstream_address: upstream.clone(),
            active_health_check_interval: options.active_health_check_interval,

            // refactor
            active_health_check_path: options.active_health_check_path.clone(),
            max_requests_per_minute: options.max_requests_per_minute,

            max_conn: 128,
            pool: VecDeque::with_capacity(128),
        }));

        upstreams.push(upstream.clone());
        proxy_states.push(state);
    }

    let mut index = 0usize;
    while let Ok((stream, _)) = listener.accept().await {
        // Handle the connection!

        index = (index + 1) % upstreams.len();
        let state = proxy_states.get(index).unwrap().clone();
        tokio::spawn(handle_connection(stream, state));
    }

    Ok(())
}

async fn connect_to_upstream(state: &ProxyState) -> Result<TcpStream, std::io::Error> {
    let upstream_ip = &state.upstream_address;
    TcpStream::connect(upstream_ip).await.or_else(|err| {
        log::error!("Failed to connect to upstream {}: {}", upstream_ip, err);
        Err(err)
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

/// Handling incomming connection and open a TCP connection to upstream server randomly chosen.
async fn handle_connection(mut client_conn: TcpStream, state: Arc<Mutex<ProxyState>>) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!("Connection received from {}", client_ip);

    // Open a connection to a random destination server
    let conn = state.lock().await.get_avaliable_conn().await;

    match conn {
        Ok(Some(mut upstream_conn)) => {
            process_task(client_ip, &mut client_conn, &mut upstream_conn).await;
            state.lock().await.pool.push_back(upstream_conn);
        }
        _ => {
            let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
            send_response(&mut client_conn, &response).await;
            return;
        }
    }
}

async fn process_task(
    client_ip: String,
    client_conn: &mut TcpStream,
    upstream_conn: &mut TcpStream,
) {
    loop {
        // Read a request from the client
        let upstream_ip = upstream_conn.peer_addr().unwrap().ip().to_string();

        let mut request = match request::read_from_stream(client_conn).await {
            Ok(request) => request,
            // Handle case where client closed connection and is no longer sending requests
            Err(request::Error::IncompleteRequest(0)) => {
                log::debug!("Client finished sending requests. Shutting down connection");
                return;
            }
            // Handle I/O error in reading from the client
            Err(request::Error::ConnectionError(io_err)) => {
                log::info!("Error reading request from client stream: {}", io_err);
                return;
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
            return;
        }
        log::debug!("Forwarded request to server");

        // Read the server's response
        let response = match response::read_from_stream(upstream_conn, request.method()).await {
            Ok(response) => response,
            Err(error) => {
                log::error!("Error reading response from server: {:?}", error);
                let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
                send_response(client_conn, &response).await;
                return;
            }
        };
        // Forward the response to the client
        send_response(client_conn, &response).await;
        log::debug!("Forwarded response to client");
    }
}
