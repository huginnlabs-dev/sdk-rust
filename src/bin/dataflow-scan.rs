//! CLI entry for the static route scanner. All logic lives in the library
//! ([`dataflow_rs::scan::run`]); this thin wrapper maps the returned code
//! to the process exit status (0 success, 1 runtime failure, 2 usage).

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    std::process::exit(dataflow_rs::scan::run(&argv));
}
