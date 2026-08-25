use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use zmq_poc_proto::{decode, encode, ControlRequest, ControlResponse, Tick};

pub struct PublisherConfig {
    pub pub_addr: String,
    pub rep_addr: String,
    pub rate: u64,
    pub symbols: Vec<String>,
}

impl Default for PublisherConfig {
    fn default() -> Self {
        Self {
            pub_addr: "tcp://127.0.0.1:5555".into(),
            rep_addr: "tcp://127.0.0.1:5556".into(),
            rate: 10_000,
            symbols: (0..20).map(|i| format!("SYM{i:03}")).collect(),
        }
    }
}

pub struct PublisherHandle {
    pub sent: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl PublisherHandle {
    pub fn shutdown(mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(h) = self.thread.take() {
            let _ = h.join();
        }
    }

    pub fn is_running(&self) -> bool {
        !self.shutdown.load(Ordering::Acquire)
    }
}

impl Drop for PublisherHandle {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

pub fn start(config: PublisherConfig) -> PublisherHandle {
    let sent = Arc::new(AtomicU64::new(0));
    let shutdown = Arc::new(AtomicBool::new(false));

    let handle = {
        let sent = Arc::clone(&sent);
        let shutdown = Arc::clone(&shutdown);
        thread::spawn(move || {
            run_publisher(config, &sent, &shutdown);
        })
    };

    PublisherHandle {
        sent,
        shutdown,
        thread: Some(handle),
    }
}

fn run_publisher(config: PublisherConfig, sent: &AtomicU64, shutdown: &AtomicBool) {
    let ctx = zmq::Context::new();

    let pub_sock = ctx.socket(zmq::PUB).expect("PUB socket");
    pub_sock.set_sndhwm(1_000).unwrap();
    pub_sock.set_linger(0).unwrap();
    pub_sock.bind(&config.pub_addr).expect("PUB bind");

    let rep_sock = ctx.socket(zmq::REP).expect("REP socket");
    rep_sock.set_linger(0).unwrap();
    rep_sock.bind(&config.rep_addr).expect("REP bind");

    let symbols = &config.symbols;
    let mut snapshot: HashMap<String, Tick> = HashMap::new();
    let mut prices: Vec<f64> = symbols
        .iter()
        .enumerate()
        .map(|(i, _)| 100.0 + i as f64 * 5.0)
        .collect();
    let mut seq: u64 = 0;

    let interval = Duration::from_secs_f64(1.0 / config.rate as f64);
    let mut next_send = Instant::now();

    while !shutdown.load(Ordering::Acquire) {
        let now = Instant::now();

        while next_send <= now {
            let idx = (seq as usize) % symbols.len();
            let sym = &symbols[idx];

            let drift = ((seq.wrapping_mul(6364136223846793005).wrapping_add(1)) as f64
                / u64::MAX as f64
                - 0.5)
                * 0.02;
            prices[idx] = (prices[idx] + drift).max(1.0);
            let price = prices[idx];

            let tick = Tick {
                symbol: sym.clone(),
                bid: price - 0.01,
                ask: price + 0.01,
                last: price,
                size: 100 + (seq % 900) as u32,
                seq,
                ts_nanos: Tick::now_nanos(),
            };

            let payload = encode(&tick);
            if pub_sock.send(sym.as_bytes(), zmq::SNDMORE).is_err() {
                break;
            }
            if pub_sock.send(&payload, 0).is_err() {
                break;
            }

            snapshot.insert(sym.clone(), tick);
            seq += 1;
            sent.fetch_add(1, Ordering::Relaxed);
            next_send += interval;
        }

        if rep_sock.poll(zmq::POLLIN, 0).unwrap_or(0) > 0 {
            if let Ok(msg) = rep_sock.recv_bytes(0) {
                let resp = match decode::<ControlRequest>(&msg) {
                    Ok(ControlRequest::Ping) => ControlResponse::Pong,
                    Ok(ControlRequest::Snapshot) => {
                        let ticks: Vec<Tick> = snapshot.values().cloned().collect();
                        ControlResponse::Snapshot { ticks }
                    }
                    Ok(ControlRequest::Subscribe { .. } | ControlRequest::Unsubscribe { .. }) => {
                        ControlResponse::Ok
                    }
                    Err(e) => ControlResponse::Error {
                        msg: format!("decode error: {e}"),
                    },
                };
                let _ = rep_sock.send(encode(&resp), 0);
            }
        }

        let until_next = next_send.saturating_duration_since(Instant::now());
        if until_next > Duration::from_micros(100) {
            std::thread::sleep(Duration::from_micros(50));
        } else {
            std::hint::spin_loop();
        }
    }
}
