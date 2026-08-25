use crossbeam_channel::{Receiver, Sender, TrySendError};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use zmq_poc_proto::{decode, encode, ControlRequest, ControlResponse, Tick};

pub struct SubscriberConfig {
    pub sub_addr: String,
    pub req_addr: String,
    pub frame_interval: Duration,
    pub channel_capacity: usize,
    pub symbols: Vec<String>,
}

impl Default for SubscriberConfig {
    fn default() -> Self {
        Self {
            sub_addr: "tcp://127.0.0.1:5555".into(),
            req_addr: "tcp://127.0.0.1:5556".into(),
            frame_interval: Duration::from_millis(16),
            channel_capacity: 10_000,
            symbols: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FrameBatch {
    pub ticks: HashMap<String, Tick>,
    pub coalesced_count: u64,
}

#[derive(Debug)]
pub struct Metrics {
    pub received: AtomicU64,
    pub dropped: AtomicU64,
    pub flushes: AtomicU64,
    pub coalesced: AtomicU64,
    pub seq_gaps: AtomicU64,
}

impl Metrics {
    fn new() -> Self {
        Self {
            received: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            flushes: AtomicU64::new(0),
            coalesced: AtomicU64::new(0),
            seq_gaps: AtomicU64::new(0),
        }
    }
}

pub struct SubscriberHandle {
    pub batches: Receiver<FrameBatch>,
    pub metrics: Arc<Metrics>,
    shutdown: Arc<AtomicBool>,
    zmq_thread: Option<JoinHandle<()>>,
    coalescer_thread: Option<JoinHandle<()>>,
}

impl SubscriberHandle {
    pub fn shutdown(mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(h) = self.zmq_thread.take() {
            let _ = h.join();
        }
        if let Some(h) = self.coalescer_thread.take() {
            let _ = h.join();
        }
    }
}

impl Drop for SubscriberHandle {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

pub fn start(config: SubscriberConfig) -> SubscriberHandle {
    let metrics = Arc::new(Metrics::new());
    let shutdown = Arc::new(AtomicBool::new(false));

    let (tick_tx, tick_rx) = crossbeam_channel::bounded::<Tick>(config.channel_capacity);
    let (batch_tx, batch_rx) = crossbeam_channel::bounded::<FrameBatch>(16);

    let zmq_handle = {
        let metrics = Arc::clone(&metrics);
        let shutdown = Arc::clone(&shutdown);
        let config_sub = config.sub_addr.clone();
        let config_req = config.req_addr.clone();
        let symbols = config.symbols.clone();
        thread::spawn(move || {
            zmq_recv_loop(
                config_sub, config_req, symbols, tick_tx, &metrics, &shutdown,
            );
        })
    };

    let coalescer_handle = {
        let metrics = Arc::clone(&metrics);
        let shutdown = Arc::clone(&shutdown);
        let frame_interval = config.frame_interval;
        thread::spawn(move || {
            coalescer_loop(tick_rx, batch_tx, frame_interval, &metrics, &shutdown);
        })
    };

    SubscriberHandle {
        batches: batch_rx,
        metrics,
        shutdown,
        zmq_thread: Some(zmq_handle),
        coalescer_thread: Some(coalescer_handle),
    }
}

fn zmq_recv_loop(
    sub_addr: String,
    req_addr: String,
    symbols: Vec<String>,
    tick_tx: Sender<Tick>,
    metrics: &Metrics,
    shutdown: &AtomicBool,
) {
    let ctx = zmq::Context::new();
    let sub_sock = ctx.socket(zmq::SUB).expect("SUB socket");
    sub_sock.set_rcvhwm(1_000).unwrap();
    sub_sock.set_linger(0).unwrap();
    sub_sock.set_reconnect_ivl(100).unwrap();
    sub_sock.connect(&sub_addr).expect("SUB connect");

    for sym in &symbols {
        sub_sock.set_subscribe(sym.as_bytes()).unwrap();
    }

    let req_sock = ctx.socket(zmq::REQ).expect("REQ socket");
    req_sock.set_linger(0).unwrap();
    req_sock.set_rcvtimeo(1_000).unwrap();
    req_sock.connect(&req_addr).expect("REQ connect");

    let subscribed: std::collections::HashSet<&str> = symbols.iter().map(|s| s.as_str()).collect();

    // Request initial snapshot (filtered to subscribed symbols)
    request_snapshot(&req_sock, &tick_tx, metrics, &subscribed);

    let mut last_seq: HashMap<String, u64> = HashMap::new();

    while !shutdown.load(Ordering::Acquire) {
        if sub_sock.poll(zmq::POLLIN, 10).unwrap_or(0) <= 0 {
            continue;
        }

        // Multipart: [topic, payload]
        let topic = match sub_sock.recv_bytes(0) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let payload = match sub_sock.recv_bytes(0) {
            Ok(p) => p,
            Err(_) => continue,
        };

        let tick: Tick = match decode(&payload) {
            Ok(t) => t,
            Err(_) => continue,
        };

        // Detect sequence gaps per symbol
        let sym = String::from_utf8_lossy(&topic).to_string();
        if let Some(&prev) = last_seq.get(&sym) {
            if tick.seq != prev + symbols_stride(prev, &sym)
                && tick.seq > prev
                && tick.seq > prev + 1000
            {
                metrics.seq_gaps.fetch_add(1, Ordering::Relaxed);
                request_snapshot(&req_sock, &tick_tx, metrics, &subscribed);
            }
        }
        last_seq.insert(sym, tick.seq);

        metrics.received.fetch_add(1, Ordering::Relaxed);
        match tick_tx.try_send(tick) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                metrics.dropped.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Disconnected(_)) => break,
        }
    }
}

fn symbols_stride(_seq: u64, _sym: &str) -> u64 {
    // In this PoC the publisher round-robins symbols, so the stride between
    // consecutive ticks for the same symbol equals the total number of symbols.
    // But we don't know that here, so we just accept any forward jump.
    // Real gap detection uses the global seq counter.
    1
}

fn request_snapshot(
    req_sock: &zmq::Socket,
    tick_tx: &Sender<Tick>,
    metrics: &Metrics,
    subscribed: &std::collections::HashSet<&str>,
) {
    let req = encode(&ControlRequest::Snapshot);
    if req_sock.send(&req, 0).is_err() {
        return;
    }
    if let Ok(resp_bytes) = req_sock.recv_bytes(0) {
        if let Ok(ControlResponse::Snapshot { ticks }) = decode(&resp_bytes) {
            for tick in ticks {
                if !subscribed.contains(tick.symbol.as_str()) {
                    continue;
                }
                metrics.received.fetch_add(1, Ordering::Relaxed);
                if tick_tx.try_send(tick).is_err() {
                    metrics.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

fn coalescer_loop(
    tick_rx: Receiver<Tick>,
    batch_tx: Sender<FrameBatch>,
    frame_interval: Duration,
    metrics: &Metrics,
    shutdown: &AtomicBool,
) {
    let mut cache: HashMap<String, Tick> = HashMap::new();
    let mut coalesced_this_frame: u64 = 0;
    let mut last_flush = Instant::now();

    while !shutdown.load(Ordering::Acquire) {
        // Drain available ticks (non-blocking after first)
        match tick_rx.recv_timeout(Duration::from_millis(1)) {
            Ok(tick) => {
                if cache.contains_key(&tick.symbol) {
                    coalesced_this_frame += 1;
                    metrics.coalesced.fetch_add(1, Ordering::Relaxed);
                }
                cache.insert(tick.symbol.clone(), tick);
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        }

        while let Ok(tick) = tick_rx.try_recv() {
            if cache.contains_key(&tick.symbol) {
                coalesced_this_frame += 1;
                metrics.coalesced.fetch_add(1, Ordering::Relaxed);
            }
            cache.insert(tick.symbol.clone(), tick);
        }

        if last_flush.elapsed() >= frame_interval && !cache.is_empty() {
            let batch = FrameBatch {
                ticks: std::mem::take(&mut cache),
                coalesced_count: coalesced_this_frame,
            };
            coalesced_this_frame = 0;
            metrics.flushes.fetch_add(1, Ordering::Relaxed);

            if batch_tx.try_send(batch).is_err() {
                // UI is behind; drop the frame
            }

            last_flush = Instant::now();
        }
    }
}

pub fn ping(req_addr: &str) -> Result<bool, zmq::Error> {
    let ctx = zmq::Context::new();
    let sock = ctx.socket(zmq::REQ)?;
    sock.set_linger(0)?;
    sock.set_rcvtimeo(1_000)?;
    sock.connect(req_addr)?;
    sock.send(encode(&ControlRequest::Ping), 0)?;
    match sock.recv_bytes(0) {
        Ok(bytes) => Ok(matches!(
            decode::<ControlResponse>(&bytes),
            Ok(ControlResponse::Pong)
        )),
        Err(_) => Ok(false),
    }
}
