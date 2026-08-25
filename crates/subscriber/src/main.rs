use anyhow::Result;
use clap::Parser;
use std::time::{Duration, Instant};
use zmq_poc_subscriber::{start, SubscriberConfig};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "tcp://127.0.0.1:5555")]
    sub_addr: String,
    #[arg(long, default_value = "tcp://127.0.0.1:5556")]
    req_addr: String,
    /// Symbols to subscribe (comma-separated, e.g. SYM000,SYM001)
    #[arg(long, default_value = "SYM000,SYM001,SYM002,SYM003,SYM004")]
    symbols: String,
    /// Frame interval in ms
    #[arg(long, default_value_t = 16)]
    frame_ms: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let symbols: Vec<String> = args
        .symbols
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    eprintln!(
        "subscriber: SUB={} REQ={} symbols={} frame={}ms",
        args.sub_addr,
        args.req_addr,
        symbols.join(","),
        args.frame_ms,
    );

    let handle = start(SubscriberConfig {
        sub_addr: args.sub_addr,
        req_addr: args.req_addr,
        frame_interval: Duration::from_millis(args.frame_ms),
        symbols,
        ..Default::default()
    });

    let mut stats_time = Instant::now();
    let mut batch_count: u64 = 0;

    loop {
        match handle.batches.recv_timeout(Duration::from_millis(100)) {
            Ok(batch) => {
                batch_count += 1;
                // Print the latest values in the batch
                let mut syms: Vec<_> = batch.ticks.iter().collect();
                syms.sort_by_key(|(k, _)| (*k).clone());
                print!("\x1b[2J\x1b[H"); // clear screen
                println!(
                    "--- Frame {batch_count} ({} symbols, {} coalesced) ---",
                    batch.ticks.len(),
                    batch.coalesced_count
                );
                for (sym, tick) in &syms {
                    println!(
                        "{sym:>8}  bid={:.4}  ask={:.4}  last={:.4}  size={:>5}  seq={}",
                        tick.bid, tick.ask, tick.last, tick.size, tick.seq
                    );
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        }

        if stats_time.elapsed() >= Duration::from_secs(5) {
            let m = &handle.metrics;
            let recv = m.received.load(std::sync::atomic::Ordering::Relaxed);
            let drops = m.dropped.load(std::sync::atomic::Ordering::Relaxed);
            let flushes = m.flushes.load(std::sync::atomic::Ordering::Relaxed);
            let coalesced = m.coalesced.load(std::sync::atomic::Ordering::Relaxed);
            let gaps = m.seq_gaps.load(std::sync::atomic::Ordering::Relaxed);
            eprintln!(
                "metrics: received={recv} dropped={drops} flushes={flushes} coalesced={coalesced} gaps={gaps}"
            );
            stats_time = Instant::now();
        }
    }
    Ok(())
}
