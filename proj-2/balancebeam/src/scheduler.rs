use crate::{request, response};

use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::{
        mpsc::{channel, Receiver, Sender},
        Mutex,
    },
};

pub(crate) type UpStreamStatus = AtomicU8;
pub(crate) const STATUS_HEALTHY: u8 = 0;
pub(crate) const STATUS_UNAVAILABLE: u8 = 1;
pub(crate) const STATUS_ACTIVATING: u8 = 2; // middle state in case of concurrent re-activating
pub(crate) const DEFAULT_MAX_CONN: usize = 64;

#[derive(Debug, thiserror::Error)]
pub enum BBErr {
    #[error("fail to reuse tcp connection {0}")]
    ReuseConnectionErr(String),

    #[error("fail to connection upstream {0}")]
    BuildConnectionErr(String),
}

/// ProxyState maintains infomations about an upstream server, like TCP connections and server status.
///
///
///     state machine:
///
///                        conn_cnt == 0                (by destroy_conn())
///                     or can't build new connection   (by get_idle_conn())
///              health ----------------------->  unavailable
///                  |                             |
///                  <-------- activating ---------
///          build new conn      ↑  ↓
///                              ↑  ↓
///                              ↑  ↓
///                              <---
///                        reactivate (by reactivation)
///                     
///
pub(crate) struct ProxyState {
    /// How frequently we check whether upstream servers are alive (Milestone 4)
    // Seconds
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
    pub fn new(
        #[allow(dead_code)] active_health_check_interval: usize,
        active_health_check_path: String,
        max_requests_per_minute: usize,
        upstream_address: String,
        max_conn: usize,
        pool: VecDeque<TcpStream>,
        conn_cnt: usize,
        upstream_status: Arc<UpStreamStatus>,
    ) -> ProxyState {
        ProxyState {
            upstream_address,
            active_health_check_interval,
            active_health_check_path,
            max_requests_per_minute,
            max_conn,
            pool,
            conn_cnt,
            upstream_status,
        }
    }

    /// Return None if there are no idle connections for task and task should wait for a while or be attach to another UpStream.
    /// Note: This method return error if and only if it can't build new connections.
    pub(crate) async fn get_idle_conn(&mut self) -> Result<Option<TcpStream>, BBErr> {
        if self.upstream_status.load(Ordering::SeqCst) != STATUS_HEALTHY {
            return Err(BBErr::BuildConnectionErr(self.upstream_address.clone()));
        }

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
    pub(crate) fn destory_conn(&mut self, _: TcpStream) {
        log::info!("destroy conn");
        self.conn_cnt -= 1;
        self.try_mark_unavailable();
    }

    pub(crate) fn try_mark_unavailable(&mut self) {
        // Double check
        if self.conn_cnt == 0
            && self
                .upstream_status
                .compare_exchange(
                    STATUS_HEALTHY,
                    STATUS_UNAVAILABLE,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
        {
            println!(
                "mark upstream server {} as unavailable",
                &self.upstream_address
            );
        }
    }
}

/// Dispatcher is the first level scheduler. It takes incoming connections and choose a proper upstream
pub(crate) async fn dispatcher(
    upstreams: Vec<(Arc<AtomicU8>, String)>,
    mut proxy_states: Vec<Arc<Mutex<ProxyState>>>,
    mut task_queue: Receiver<TcpStream>,
    task_sender: Sender<TcpStream>,
) {
    let mut index = upstreams.len() - 1;
    let mut task_id = 0;

    let (reactive_sender, reactive_recv) = channel(16);

    // reactivate proxystate
    tokio::spawn(reactivator(reactive_recv));

    while let Some(client_conn) = task_queue.recv().await {
        let (all_unavailable, cur_index) =
            schedule(&upstreams, &mut proxy_states, index, &reactive_sender).await;
        if all_unavailable {
            drop(client_conn);
        } else {
            task_id += 1;
            let redispatcher = task_sender.clone();
            let state = proxy_states.get(cur_index).unwrap().clone();
            tokio::spawn(handle_connection(task_id, client_conn, state, redispatcher));
        }
        index = cur_index;
    }

    log::debug!("dispatcher down");
}

pub(crate) async fn schedule(
    upstreams: &Vec<(Arc<UpStreamStatus>, String)>,
    proxy_states: &mut Vec<Arc<Mutex<ProxyState>>>,
    mut cur_index: usize,
    reactive_sender: &Sender<Arc<Mutex<ProxyState>>>,
) -> (bool, usize) {
    assert!(upstreams.len() == proxy_states.len());

    let mut unavailable_cnt = 0;

    // First-fit scheduler.
    for _ in 0..upstreams.len() {
        cur_index = (cur_index + 1) % upstreams.len();
        if let Some((state, _)) = upstreams.get(cur_index) {
            let s = state.load(Ordering::SeqCst);
            match s {
                STATUS_HEALTHY => {
                    log::info!("sche ups with index={}", cur_index);
                    break;
                }
                STATUS_UNAVAILABLE | STATUS_ACTIVATING => {
                    if s == STATUS_UNAVAILABLE {
                        if let Err(_) = reactive_sender
                            .send(proxy_states.get(cur_index).unwrap().clone())
                            .await
                        {
                            panic!("reactivator died");
                        }
                    }
                    unavailable_cnt += 1;
                }
                _ => unreachable!(),
            }
        }
    }

    (unavailable_cnt == upstreams.len(), cur_index)
}

/// For milestone 4: active health check.
pub(crate) async fn reactivator(mut ps_recv: Receiver<Arc<Mutex<ProxyState>>>) {
    while let Some(ps) = ps_recv.recv().await {
        let psu = ps.lock().await;
        let flag = psu
            .upstream_status
            .compare_exchange(
                STATUS_UNAVAILABLE,
                STATUS_ACTIVATING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok();

        let addr = if flag {
            psu.upstream_address.clone()
        } else {
            String::default()
        };

        let active_health_check_interval = psu.active_health_check_interval;
        let active_health_check_path = psu.active_health_check_path.clone();

        drop(psu);

        if flag {
            tokio::spawn(try_activate_ups(
                addr,
                ps,
                active_health_check_path,
                active_health_check_interval,
            ));
        }
    }
}

/// Keep building Tcp connection.
pub(crate) async fn try_activate_ups(
    addr: String,
    ps: Arc<Mutex<ProxyState>>,
    active_health_check_url: String,
    active_health_check_interval: usize,
) {
    let client = reqwest::Client::new();
    loop {
        log::info!("try reactivating {}", &addr);
        if let Ok(resp) = client
            .get(&format!("http://{}{}", addr, active_health_check_url))
            .send()
            .await
        {
            if resp.status().is_success() {
                break;
            }
        }
        tokio::time::sleep(Duration::from_secs(active_health_check_interval as u64)).await;
    }

    let psu = ps.lock().await;
    if psu
        .upstream_status
        .compare_exchange(
            STATUS_ACTIVATING,
            STATUS_HEALTHY,
            Ordering::SeqCst,
            Ordering::SeqCst,
        )
        .is_ok()
    {
        log::debug!("mark ups {} as healthy", &psu.upstream_address);
    }
    log::debug!("re-activate upstream {}", &addr);
}

/// Return error if failed to connect upstream server.
pub(crate) async fn connect_to_upstream(state: &ProxyState) -> Result<TcpStream, BBErr> {
    let upstream_ip = &state.upstream_address;

    log::debug!("try to build connection to ups {}", &upstream_ip);
    TcpStream::connect(upstream_ip).await.or_else(|err| {
        log::error!("Failed to connect to upstream {}: {}", upstream_ip, err);
        Err(BBErr::BuildConnectionErr(upstream_ip.clone()))
    })

    // TODO: implement failover (milestone 3)
}

pub(crate) async fn send_response(client_conn: &mut TcpStream, response: &http::Response<Vec<u8>>) {
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
pub(crate) async fn handle_connection(
    task_id: usize,
    mut client_conn: TcpStream,
    state: Arc<Mutex<ProxyState>>,
    redispatcher: Sender<TcpStream>,
) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();

    // Open a connection to a random destination server
    let conn = state.lock().await.get_idle_conn().await;
    match conn {
        Ok(Some(mut upstream_conn)) => {
            let ups = (
                upstream_conn.peer_addr().unwrap().ip().to_string(),
                upstream_conn.peer_addr().unwrap().port().to_string(),
            );
            log::debug!(
                "Connection received from {}. task_id={}, ups={:?}",
                client_ip,
                task_id,
                ups
            );
            match process_task(client_ip, &mut client_conn, &mut upstream_conn).await {
                // Return the connection back for re-using.
                Ok(()) => state.lock().await.pool.push_back(upstream_conn),
                Err(BBErr::ReuseConnectionErr(_)) => state.lock().await.destory_conn(upstream_conn),
                _ => unreachable!(),
            }
        }
        Ok(None) => {
            log::debug!("can't get idle conn, re-dispatch task");
            let _ = redispatcher.send(client_conn).await;
            return;
        }
        Err(BBErr::BuildConnectionErr(_)) => {
            // Error is throwed from `connect_to_upstream` and hints that upstream is unavailable.
            log::debug!("can't build connection, consider upstream unavailable. re-dispatch task");
            let _ = redispatcher.send(client_conn).await;
            state.lock().await.try_mark_unavailable();
            return;
        }
        _ => unreachable!(),
    }
}

/// Relay requests from client to an upstream server.
/// Return true if the connectin is available.
pub(crate) async fn process_task(
    client_ip: String,
    client_conn: &mut TcpStream,
    upstream_conn: &mut TcpStream,
) -> Result<(), BBErr> {
    // Read a request from the client
    let upstream_ip = upstream_conn.peer_addr().unwrap().ip().to_string();
    let upstream_port = upstream_conn.peer_addr().unwrap().port().to_string();

    let ups = (&upstream_ip, &upstream_port);

    loop {
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
            "{} -> {:?}: {}",
            client_ip,
            ups,
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
