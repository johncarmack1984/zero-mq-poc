#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use zmq_poc_proto::{decode, encode, ControlRequest, ControlResponse};
    use zmq_poc_publisher::PublisherConfig;
    use zmq_poc_subscriber::SubscriberConfig;

    fn free_ports() -> (u16, u16) {
        let a = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let b = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let pa = a.local_addr().unwrap().port();
        let pb = b.local_addr().unwrap().port();
        drop(a);
        drop(b);
        (pa, pb)
    }

    fn addrs() -> (String, String) {
        let (pub_port, rep_port) = free_ports();
        (
            format!("tcp://127.0.0.1:{pub_port}"),
            format!("tcp://127.0.0.1:{rep_port}"),
        )
    }

    #[test]
    fn pubsub_delivers_subscribed_and_filters_unsubscribed() {
        let (pub_addr, rep_addr) = addrs();
        let symbols: Vec<String> = (0..5).map(|i| format!("SYM{i:03}")).collect();

        let pub_handle = zmq_poc_publisher::start(PublisherConfig {
            pub_addr: pub_addr.clone(),
            rep_addr: rep_addr.clone(),
            rate: 5_000,
            symbols: symbols.clone(),
        });

        std::thread::sleep(Duration::from_millis(100));

        // Subscribe to only SYM000 and SYM001
        let sub_handle = zmq_poc_subscriber::start(SubscriberConfig {
            sub_addr: pub_addr,
            req_addr: rep_addr,
            frame_interval: Duration::from_millis(50),
            channel_capacity: 1_000,
            symbols: vec!["SYM000".into(), "SYM001".into()],
        });

        // Collect batches for 1 second
        let mut seen_symbols = std::collections::HashSet::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while std::time::Instant::now() < deadline {
            if let Ok(batch) = sub_handle.batches.recv_timeout(Duration::from_millis(100)) {
                for sym in batch.ticks.keys() {
                    seen_symbols.insert(sym.clone());
                }
            }
        }

        assert!(seen_symbols.contains("SYM000"), "should receive SYM000");
        assert!(seen_symbols.contains("SYM001"), "should receive SYM001");
        assert!(
            !seen_symbols.contains("SYM002"),
            "should not receive SYM002"
        );
        assert!(
            !seen_symbols.contains("SYM003"),
            "should not receive SYM003"
        );

        let recv = sub_handle.metrics.received.load(Ordering::Relaxed);
        assert!(recv > 0, "should have received ticks");

        sub_handle.shutdown();
        pub_handle.shutdown();
    }

    #[test]
    fn reqrep_snapshot_and_ping() {
        let (pub_addr, rep_addr) = addrs();
        let symbols: Vec<String> = (0..5).map(|i| format!("SYM{i:03}")).collect();

        let pub_handle = zmq_poc_publisher::start(PublisherConfig {
            pub_addr: pub_addr.clone(),
            rep_addr: rep_addr.clone(),
            rate: 5_000,
            symbols: symbols.clone(),
        });

        // Let it publish for a bit so snapshots have data
        std::thread::sleep(Duration::from_millis(500));

        // Test ping
        assert!(
            zmq_poc_subscriber::ping(&rep_addr).unwrap(),
            "ping should return true"
        );

        // Test snapshot
        let ctx = zmq::Context::new();
        let req = ctx.socket(zmq::REQ).unwrap();
        req.set_linger(0).unwrap();
        req.set_rcvtimeo(2_000).unwrap();
        req.connect(&rep_addr).unwrap();
        req.send(&encode(&ControlRequest::Snapshot), 0).unwrap();
        let resp_bytes = req.recv_bytes(0).unwrap();
        let resp: ControlResponse = decode(&resp_bytes).unwrap();
        match resp {
            ControlResponse::Snapshot { ticks } => {
                assert!(!ticks.is_empty(), "snapshot should contain ticks");
                let snap_symbols: std::collections::HashSet<String> =
                    ticks.iter().map(|t| t.symbol.clone()).collect();
                for sym in &symbols {
                    assert!(snap_symbols.contains(sym), "snapshot should contain {sym}");
                }
            }
            other => panic!("expected Snapshot, got {other:?}"),
        }

        pub_handle.shutdown();
    }

    #[test]
    fn coalescing_reduces_ui_flushes() {
        let (pub_addr, rep_addr) = addrs();
        let symbols: Vec<String> = (0..3).map(|i| format!("SYM{i:03}")).collect();

        let pub_handle = zmq_poc_publisher::start(PublisherConfig {
            pub_addr: pub_addr.clone(),
            rep_addr: rep_addr.clone(),
            rate: 10_000,
            symbols: symbols.clone(),
        });

        std::thread::sleep(Duration::from_millis(100));

        let frame_interval = Duration::from_millis(16);
        let sub_handle = zmq_poc_subscriber::start(SubscriberConfig {
            sub_addr: pub_addr,
            req_addr: rep_addr,
            frame_interval,
            channel_capacity: 50_000,
            symbols: symbols.clone(),
        });

        // Run for 2 seconds
        let duration = Duration::from_secs(2);
        let start = std::time::Instant::now();
        let mut last_values: std::collections::HashMap<String, zmq_poc_proto::Tick> =
            std::collections::HashMap::new();

        while start.elapsed() < duration {
            if let Ok(batch) = sub_handle.batches.recv_timeout(Duration::from_millis(100)) {
                for (sym, tick) in batch.ticks {
                    last_values.insert(sym, tick);
                }
            }
        }

        let recv = sub_handle.metrics.received.load(Ordering::Relaxed);
        let flushes = sub_handle.metrics.flushes.load(Ordering::Relaxed);
        let coalesced = sub_handle.metrics.coalesced.load(Ordering::Relaxed);

        eprintln!("coalesce test: received={recv} flushes={flushes} coalesced={coalesced}");

        // With 10k msg/s and 16ms frames, we expect ~125 flushes over 2s.
        // The receive count should be much higher than the flush count.
        let max_expected_flushes = (duration.as_millis() / frame_interval.as_millis()) + 10;
        assert!(
            flushes <= max_expected_flushes as u64,
            "flushes ({flushes}) should be <= {max_expected_flushes}"
        );
        assert!(
            recv > flushes * 2,
            "received ({recv}) should be >> flushes ({flushes})"
        );

        // Last values should be correct (exist for each subscribed symbol)
        for sym in &symbols {
            assert!(
                last_values.contains_key(sym),
                "should have a value for {sym}"
            );
        }

        sub_handle.shutdown();
        pub_handle.shutdown();
    }

    #[test]
    fn reconnect_detects_gap_and_recovers() {
        let (pub_addr, rep_addr) = addrs();
        let symbols: Vec<String> = (0..3).map(|i| format!("SYM{i:03}")).collect();

        // Start publisher
        let pub_handle = zmq_poc_publisher::start(PublisherConfig {
            pub_addr: pub_addr.clone(),
            rep_addr: rep_addr.clone(),
            rate: 2_000,
            symbols: symbols.clone(),
        });

        std::thread::sleep(Duration::from_millis(100));

        let sub_handle = zmq_poc_subscriber::start(SubscriberConfig {
            sub_addr: pub_addr.clone(),
            req_addr: rep_addr.clone(),
            frame_interval: Duration::from_millis(50),
            channel_capacity: 1_000,
            symbols: symbols.clone(),
        });

        // Let it run for a bit
        std::thread::sleep(Duration::from_millis(500));
        let recv_before = sub_handle.metrics.received.load(Ordering::Relaxed);
        assert!(recv_before > 0, "should have received ticks before kill");

        // Kill the publisher
        pub_handle.shutdown();
        std::thread::sleep(Duration::from_millis(200));

        // Restart with the same addresses (seq restarts from 0)
        let pub_handle2 = zmq_poc_publisher::start(PublisherConfig {
            pub_addr: pub_addr.clone(),
            rep_addr: rep_addr.clone(),
            rate: 2_000,
            symbols: symbols.clone(),
        });

        // Let it reconnect and receive new ticks
        std::thread::sleep(Duration::from_secs(2));
        let recv_after = sub_handle.metrics.received.load(Ordering::Relaxed);
        assert!(
            recv_after > recv_before,
            "should receive more ticks after reconnect: before={recv_before} after={recv_after}"
        );

        // Verify we got data after reconnect
        let mut got_batch = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            if let Ok(batch) = sub_handle.batches.recv_timeout(Duration::from_millis(200)) {
                if !batch.ticks.is_empty() {
                    got_batch = true;
                    break;
                }
            }
        }
        assert!(got_batch, "should receive batches after publisher restart");

        sub_handle.shutdown();
        pub_handle2.shutdown();
    }

    #[test]
    fn slow_consumer_does_not_stall_publisher() {
        let (pub_addr, rep_addr) = addrs();
        let symbols: Vec<String> = (0..3).map(|i| format!("SYM{i:03}")).collect();

        let pub_handle = zmq_poc_publisher::start(PublisherConfig {
            pub_addr: pub_addr.clone(),
            rep_addr: rep_addr.clone(),
            rate: 20_000,
            symbols: symbols.clone(),
        });

        std::thread::sleep(Duration::from_millis(100));

        // Start subscriber with a tiny channel (simulates slow consumer)
        let sub_handle = zmq_poc_subscriber::start(SubscriberConfig {
            sub_addr: pub_addr,
            req_addr: rep_addr,
            frame_interval: Duration::from_millis(500), // very slow flushes
            channel_capacity: 10,                       // tiny queue
            symbols: symbols.clone(),
        });

        // Don't consume batches (simulating stalled UI)
        std::thread::sleep(Duration::from_secs(2));

        let pub_sent = pub_handle.sent.load(Ordering::Relaxed);
        let sub_recv = sub_handle.metrics.received.load(Ordering::Relaxed);
        let sub_dropped = sub_handle.metrics.dropped.load(Ordering::Relaxed);

        eprintln!(
            "slow consumer: pub_sent={pub_sent} sub_recv={sub_recv} sub_dropped={sub_dropped}"
        );

        // Publisher must not be stalled by a slow consumer.
        // At 20k/s for 2s we expect ~40k. Allow wide tolerance.
        assert!(
            pub_sent > 10_000,
            "publisher should not be stalled by slow consumer: sent={pub_sent}"
        );

        // Backpressure proof: the subscriber received fewer ticks than the publisher
        // sent (ZMQ HWM drops at the PUB socket) and/or the bounded channel dropped
        // ticks. Either mechanism is fine -- the point is the publisher keeps going.
        let total_loss = pub_sent.saturating_sub(sub_recv) + sub_dropped;
        assert!(
        total_loss > 0 || sub_recv < pub_sent,
        "some messages should be lost to backpressure: sent={pub_sent} recv={sub_recv} dropped={sub_dropped}"
    );

        sub_handle.shutdown();
        pub_handle.shutdown();
    }
}
