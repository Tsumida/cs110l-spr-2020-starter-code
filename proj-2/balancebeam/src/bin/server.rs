//! Simulator for processing http query and io.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use warp::Filter;

async fn query_hello(cnt: Arc<AtomicUsize>) -> Result<impl warp::Reply, warp::Rejection> {
    let num = cnt.fetch_add(1, Ordering::SeqCst);
    let ts = rand::random::<u64>() % 20 + 20;
    tokio::time::sleep(Duration::from_millis(ts)).await;
    Ok(warp::reply::json(&num))
}

async fn heavy_io(cnt: Arc<AtomicUsize>) -> Result<impl warp::Reply, warp::Rejection> {
    let num = cnt.fetch_add(1, Ordering::SeqCst);
    let ts = rand::random::<u64>() % 500 + 500;
    tokio::time::sleep(Duration::from_millis(ts)).await;
    Ok(warp::reply::json(&num))
}

#[tokio::main]
async fn main() -> () {
    let io_cnt = Arc::new(AtomicUsize::new(0));
    let query_cnt = Arc::new(AtomicUsize::new(0));

    let query_cnt_copy = query_cnt.clone();
    let shared_query_cnt = warp::any().map(move || query_cnt_copy.clone());
    let io_cnt_copy = io_cnt.clone();
    let shared_io_cnt = warp::any().map(move || io_cnt_copy.clone());

    let hello = warp::get()
        .and(warp::path("hello"))
        .and(shared_query_cnt.clone())
        .and(warp::path::end())
        .and_then(query_hello);

    let io_task = warp::get()
        .and(warp::path("io-task"))
        .and(shared_io_cnt.clone())
        .and(warp::path::end())
        .and_then(heavy_io);

    let bind_ip = [127, 0, 0, 1];
    let port = 6666;

    println!("listening at {:?}:{}", bind_ip, port);

    warp::serve(hello.or(io_task)).run((bind_ip, port)).await;
}
