use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tick {
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub last: f64,
    pub size: u32,
    pub seq: u64,
    pub ts_nanos: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlRequest {
    Subscribe { symbol: String },
    Unsubscribe { symbol: String },
    Snapshot,
    Ping,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlResponse {
    Ok,
    Snapshot { ticks: Vec<Tick> },
    Pong,
    Error { msg: String },
}

pub fn encode<T: Serialize>(val: &T) -> Vec<u8> {
    bincode::serialize(val).expect("serialization should not fail")
}

pub fn decode<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T, bincode::Error> {
    bincode::deserialize(bytes)
}

impl Tick {
    pub fn now_nanos() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}
