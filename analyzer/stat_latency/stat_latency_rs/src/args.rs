use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(about = "Analyze Conflux massive-test latency logs (memory-optimized)")]
pub struct Args {
    /// Log directory containing host subdirs with blocks.log or output*.7z
    #[arg(short = 'l', long = "log-path")]
    pub log_path: PathBuf,

    /// Only analyze the earliest N blocks (optional)
    #[arg(short = 'n', long = "max-blocks")]
    pub max_blocks: Option<usize>,
}
