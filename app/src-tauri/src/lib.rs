use serde::Serialize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tauri::{AppHandle, Emitter, Manager};
use zmq_poc_subscriber::{start, SubscriberConfig};

#[derive(Debug, Clone, Serialize)]
struct TickUpdate {
    symbol: String,
    bid: f64,
    ask: f64,
    last: f64,
    size: u32,
    seq: u64,
}

#[derive(Debug, Clone, Serialize)]
struct MetricsUpdate {
    received: u64,
    flushes: u64,
    coalesced: u64,
    dropped: u64,
    gaps: u64,
}

#[tauri::command]
fn get_metrics(state: tauri::State<'_, AppState>) -> MetricsUpdate {
    let m = &state.metrics;
    MetricsUpdate {
        received: m.received.load(Ordering::Relaxed),
        flushes: m.flushes.load(Ordering::Relaxed),
        coalesced: m.coalesced.load(Ordering::Relaxed),
        dropped: m.dropped.load(Ordering::Relaxed),
        gaps: m.seq_gaps.load(Ordering::Relaxed),
    }
}

struct AppState {
    metrics: std::sync::Arc<zmq_poc_subscriber::Metrics>,
}

pub fn run() {
    tauri::Builder::default()
        .setup(|app| {
            let symbols: Vec<String> = (0..10).map(|i| format!("SYM{i:03}")).collect();
            let handle = start(SubscriberConfig {
                sub_addr: "tcp://127.0.0.1:5555".into(),
                req_addr: "tcp://127.0.0.1:5556".into(),
                frame_interval: Duration::from_millis(16),
                symbols,
                ..Default::default()
            });

            let metrics = std::sync::Arc::clone(&handle.metrics);
            app.manage(AppState { metrics });

            let app_handle: AppHandle = app.handle().clone();
            let batches = handle.batches.clone();

            std::thread::spawn(move || {
                loop {
                    match batches.recv_timeout(Duration::from_millis(50)) {
                        Ok(batch) => {
                            let updates: Vec<TickUpdate> = batch
                                .ticks
                                .into_values()
                                .map(|tick| TickUpdate {
                                    symbol: tick.symbol,
                                    bid: tick.bid,
                                    ask: tick.ask,
                                    last: tick.last,
                                    size: tick.size,
                                    seq: tick.seq,
                                })
                                .collect();
                            let _ = app_handle.emit("tick-batch", &updates);
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                    }
                }
                drop(handle);
            });

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![get_metrics])
        .run(tauri::generate_context!())
        .expect("error running tauri app");
}
