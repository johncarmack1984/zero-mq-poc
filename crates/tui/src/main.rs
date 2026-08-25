use anyhow::Result;
use clap::Parser;
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Layout},
    style::{Color, Modifier, Style},
    text::Span,
    widgets::{Block, Borders, Cell, Paragraph, Row, Table},
    Terminal,
};
use std::collections::HashMap;
use std::io::stdout;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use zmq_poc_proto::Tick;
use zmq_poc_subscriber::{start, SubscriberConfig};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "tcp://127.0.0.1:5555")]
    sub_addr: String,
    #[arg(long, default_value = "tcp://127.0.0.1:5556")]
    req_addr: String,
    #[arg(
        long,
        default_value = "SYM000,SYM001,SYM002,SYM003,SYM004,SYM005,SYM006,SYM007,SYM008,SYM009"
    )]
    symbols: String,
}

struct TickState {
    tick: Tick,
    changed_at: Instant,
    prev_last: f64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let symbols: Vec<String> = args
        .symbols
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    let handle = start(SubscriberConfig {
        sub_addr: args.sub_addr,
        req_addr: args.req_addr,
        frame_interval: Duration::from_millis(16),
        symbols: symbols.clone(),
        ..Default::default()
    });

    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))?;

    let mut state: HashMap<String, TickState> = HashMap::new();
    let mut frame_count: u64 = 0;

    loop {
        // Poll for keyboard input
        if event::poll(Duration::from_millis(8))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press && key.code == KeyCode::Char('q') {
                    break;
                }
            }
        }

        // Drain available batches
        while let Ok(batch) = handle.batches.try_recv() {
            for (sym, tick) in batch.ticks {
                let prev_last = state.get(&sym).map(|s| s.tick.last).unwrap_or(tick.last);
                state.insert(
                    sym,
                    TickState {
                        tick,
                        changed_at: Instant::now(),
                        prev_last,
                    },
                );
            }
        }

        frame_count += 1;

        let m = &handle.metrics;
        let recv = m.received.load(Ordering::Relaxed);
        let flushes = m.flushes.load(Ordering::Relaxed);
        let coalesced = m.coalesced.load(Ordering::Relaxed);
        let dropped = m.dropped.load(Ordering::Relaxed);
        let gaps = m.seq_gaps.load(Ordering::Relaxed);

        terminal.draw(|f| {
            let chunks = Layout::vertical([
                Constraint::Min(5),
                Constraint::Length(3),
            ])
            .split(f.area());

            // Build table rows
            let now = Instant::now();
            let flash_duration = Duration::from_millis(300);

            let mut sorted_syms: Vec<&String> = state.keys().collect();
            sorted_syms.sort();

            let rows: Vec<Row> = sorted_syms
                .iter()
                .map(|sym| {
                    let s = &state[*sym];
                    let age = now.duration_since(s.changed_at);
                    let flash = age < flash_duration;

                    let direction = if s.tick.last > s.prev_last {
                        Color::Green
                    } else if s.tick.last < s.prev_last {
                        Color::Red
                    } else {
                        Color::White
                    };

                    let cell_style = if flash {
                        Style::default().fg(direction).add_modifier(Modifier::BOLD)
                    } else {
                        Style::default().fg(Color::White)
                    };

                    let last_style = if flash {
                        Style::default()
                            .fg(Color::Black)
                            .bg(direction)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        Style::default().fg(direction)
                    };

                    Row::new(vec![
                        Cell::from(Span::styled(sym.to_string(), cell_style)),
                        Cell::from(Span::styled(format!("{:.4}", s.tick.bid), cell_style)),
                        Cell::from(Span::styled(format!("{:.4}", s.tick.ask), cell_style)),
                        Cell::from(Span::styled(format!("{:.4}", s.tick.last), last_style)),
                        Cell::from(Span::styled(format!("{}", s.tick.size), cell_style)),
                        Cell::from(Span::styled(format!("{}", s.tick.seq), cell_style)),
                    ])
                })
                .collect();

            let header = Row::new(vec!["SYMBOL", "BID", "ASK", "LAST", "SIZE", "SEQ"])
                .style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD));

            let widths = [
                Constraint::Length(10),
                Constraint::Length(12),
                Constraint::Length(12),
                Constraint::Length(12),
                Constraint::Length(8),
                Constraint::Length(12),
            ];

            let table = Table::new(rows, widths)
                .header(header)
                .block(
                    Block::default()
                        .title(" ZeroMQ Market Data Grid ")
                        .borders(Borders::ALL),
                );

            f.render_widget(table, chunks[0]);

            let stats = format!(
                " recv={recv}  flushes={flushes}  coalesced={coalesced}  dropped={dropped}  gaps={gaps}  frames={frame_count}  [q]uit ",
            );
            let status = Paragraph::new(stats)
                .block(Block::default().title(" Metrics ").borders(Borders::ALL))
                .style(Style::default().fg(Color::Cyan));
            f.render_widget(status, chunks[1]);
        })?;

        std::thread::sleep(Duration::from_millis(8));
    }

    disable_raw_mode()?;
    stdout().execute(LeaveAlternateScreen)?;
    handle.shutdown();
    Ok(())
}
