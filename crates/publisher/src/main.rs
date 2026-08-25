use anyhow::Result;
use clap::Parser;
use std::sync::atomic::Ordering;
use zmq_poc_publisher::{start, PublisherConfig};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "tcp://127.0.0.1:5555")]
    pub_addr: String,
    #[arg(long, default_value = "tcp://127.0.0.1:5556")]
    rep_addr: String,
    #[arg(long, default_value_t = 10_000)]
    rate: u64,
    #[arg(long, default_value_t = 20)]
    symbols: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let symbols: Vec<String> = (0..args.symbols).map(|i| format!("SYM{i:03}")).collect();

    eprintln!(
        "publisher: PUB={} REP={} rate={}/s symbols={}",
        args.pub_addr, args.rep_addr, args.rate, args.symbols
    );

    let handle = start(PublisherConfig {
        pub_addr: args.pub_addr,
        rep_addr: args.rep_addr,
        rate: args.rate,
        symbols,
    });

    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
        let sent = handle.sent.load(Ordering::Relaxed);
        eprintln!("pub: total sent={sent}");
        if !handle.is_running() {
            break Ok(());
        }
    }
}
