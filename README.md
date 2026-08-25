# zero-mq-poc

Proof of concept: wiring a high-rate ZeroMQ message stream into a real-time UI without flooding the render path. A synthetic market-data publisher pushes 50k+ ticks/sec over a PUB socket; a subscriber consumes them on a dedicated ZMQ thread, coalesces per-symbol into a latest-value cache, and flushes one delta batch per frame (~16 ms). The result: the UI sees ~60 updates/sec regardless of inbound message rate.

```
publisher (PUB+REP)
    |
    | tcp, multipart [topic, bincode tick]
    v
subscriber ZMQ thread (SUB+REQ)
    |
    | bounded crossbeam channel (10k slots, drop on full)
    v
coalescer thread
    | per-symbol HashMap, overwrite-on-update
    | flush every ~16 ms
    v
UI (Tauri 2 webview grid / ratatui TUI)
```

## Measured numbers (Apple M2 Pro, macOS, release build)

| Metric | Value |
|--------|-------|
| Publish rate | ~50,000 msg/s sustained |
| Subscriber receive rate (10 of 20 symbols) | ~25,000 msg/s |
| UI flush rate | ~62 flushes/s (~16 ms interval) |
| Coalesced (overwritten before flush) | 121,835 over 5s |
| Dropped at bounded channel | 0 |
| Sequence gaps detected | 0 |
| Coalescing ratio (recv / flushes) | ~400:1 |

## ZeroMQ patterns

**PUB/SUB** -- the publisher binds a PUB socket and sends multipart messages: frame 0 is the symbol (topic), frame 1 is a bincode-encoded tick struct. Subscribers set topic filters per symbol; ZMQ does prefix-match filtering at the transport layer.

**REQ/REP** -- a control channel for snapshots and health. The subscriber requests a full snapshot on startup and after detecting a sequence gap (publisher restart). Ping/pong for liveness.

## Backpressure and HWM

Two layers of backpressure:

1. **ZMQ send-side HWM** (set to 1000 on PUB socket). When a slow subscriber's kernel buffer fills, ZMQ drops messages at the publisher -- the publisher never blocks.
2. **Bounded crossbeam channel** (10k slots between ZMQ thread and coalescer). If the coalescer falls behind, ticks are dropped and counted. In practice the coalescer drains fast enough that channel drops are rare; ZMQ HWM is the first line of defense.

The policy is deliberate drop, not block. A stale tick is worthless in a trading UI; the latest value is all that matters.

## Reconnect sequence

1. Publisher stops (crash, restart, maintenance)
2. ZMQ's built-in reconnect logic re-establishes the TCP connection automatically (reconnect interval: 100 ms)
3. Subscriber detects a sequence gap (global seq jumps by >1000)
4. Subscriber sends a Snapshot request over REQ/REP
5. Snapshot response carries the latest tick for every symbol; subscriber applies it to the cache
6. Normal PUB/SUB flow resumes

## How to run

Prerequisites: Rust toolchain, libzmq (`brew install zeromq` on macOS, `apt install libzmq3-dev` on Ubuntu).

```bash
# Terminal 1: publisher
cargo run --release -p zmq-poc-publisher -- --rate 50000 --symbols 20

# Terminal 2: TUI subscriber
cargo run --release -p zmq-poc-tui

# Or: headless subscriber with printed metrics
cargo run --release -p zmq-poc-subscriber -- --symbols SYM000,SYM001,SYM002

# Tauri 2 grid (requires Tauri v2 CLI)
cd app && cargo tauri dev

# Tests
cargo test
```

## Crate layout

- `crates/proto/` -- tick and control message types, bincode encoding
- `crates/publisher/` -- synthetic market-data feed (PUB + REP sockets)
- `crates/subscriber/` -- ZMQ recv thread, bounded channel, frame coalescer, reconnect, metrics
- `crates/smoke/` -- five integration tests (pub/sub filtering, snapshot, coalescing, reconnect, slow consumer)
- `crates/tui/` -- ratatui grid with per-cell flash on change
- `app/` -- Tauri 2 webview grid (vanilla JS, Tauri events from Rust subscriber)

## Why `zmq` (libzmq bindings), not `zeromq` (pure Rust)

The `zmq` crate wraps libzmq, the C reference implementation. Bank and trading-firm ZMQ deployments run libzmq; using the same library means the PoC exercises the same socket behavior, HWM semantics, and reconnect logic that production would see. The pure-Rust `zeromq` crate is younger and may diverge on edge cases.

## Non-goals

- No FIX protocol, no real market data, no order entry
- No auth, no TLS (CurveZMQ noted as a next step, not implemented)
- Not a library; a demonstrator with measurements

## Next steps

- CurveZMQ encryption
- FIX/SBE adapter for real market data
- Multi-window layout (portfolio, order book, time and sales)
- Virtualized grid rows (currently all rows are DOM elements)
- Tick-to-screen latency histogram (p50/p99)
