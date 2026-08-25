#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    zmq_poc_app::run();
}
