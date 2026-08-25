# Install system dependencies (libzmq)
deps:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ "$(uname)" == "Darwin" ]]; then
        if ! brew list zeromq &>/dev/null; then
            echo "Installing zeromq via brew..."
            brew install zeromq
        else
            echo "zeromq already installed"
        fi
    elif [[ -f /etc/debian_version ]]; then
        if ! dpkg -s libzmq3-dev &>/dev/null 2>&1; then
            echo "Installing libzmq3-dev..."
            sudo apt-get update && sudo apt-get install -y libzmq3-dev
        else
            echo "libzmq3-dev already installed"
        fi
    else
        echo "Unknown platform -- install libzmq manually"
        exit 1
    fi

# Ensure the Tauri CLI is available, install if missing
ensure-tauri-cli:
    #!/usr/bin/env bash
    set -euo pipefail
    if command -v tauri &>/dev/null; then
        echo "tauri CLI found: $(command -v tauri)"
    elif command -v cargo-tauri &>/dev/null; then
        echo "cargo-tauri found: $(command -v cargo-tauri)"
    elif cargo tauri --version &>/dev/null 2>&1; then
        echo "cargo tauri available via cargo"
    else
        echo "Tauri CLI not found -- installing via cargo..."
        cargo install tauri-cli --locked
    fi

# Build everything
build: deps
    cargo build --release

# Run all checks (fmt, clippy, test)
check:
    cargo fmt --check
    cargo clippy --workspace --exclude zmq-poc-app --exclude zmq-poc-tui -- -D warnings
    cargo test

# Run tests
test:
    cargo test

# Start the publisher (default: 50k msg/s, 20 symbols)
pub rate="50000" symbols="20":
    cargo run --release -p zmq-poc-publisher -- --rate {{rate}} --symbols {{symbols}}

# Start the TUI subscriber
tui symbols="SYM000,SYM001,SYM002,SYM003,SYM004,SYM005,SYM006,SYM007,SYM008,SYM009":
    cargo run --release -p zmq-poc-tui -- --symbols {{symbols}}

# Start the headless subscriber
sub symbols="SYM000,SYM001,SYM002,SYM003,SYM004,SYM005,SYM006,SYM007,SYM008,SYM009":
    cargo run --release -p zmq-poc-subscriber -- --symbols {{symbols}}

# Tauri 2 app: installs CLI if needed, starts publisher + webview grid (Ctrl-C to stop)
app: deps ensure-tauri-cli
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build --release -p zmq-poc-publisher
    trap 'kill $PUB_PID 2>/dev/null; wait $PUB_PID 2>/dev/null' EXIT
    ./target/release/zmq-poc-publisher --rate 50000 --symbols 20 &
    PUB_PID=$!
    sleep 0.5
    cd app && cargo tauri dev

# Quick demo: publisher in background + TUI (Ctrl-C to stop both)
demo: build
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'kill $PUB_PID 2>/dev/null; wait $PUB_PID 2>/dev/null' EXIT
    ./target/release/zmq-poc-publisher --rate 50000 --symbols 20 &
    PUB_PID=$!
    sleep 0.5
    ./target/release/zmq-poc-tui
