mod analyzer;
mod args;
mod config;
mod host_processing;
mod io_utils;
mod model;
mod quantile;
mod report;
mod stats;

use anyhow::{anyhow, Result};
use clap::Parser;

use analyzer::{
    analyze_txs, build_block_row_values, build_tx_rows, collect_block_scalars,
    print_throughput_and_slowest,
};
use args::Args;
use config::{default_latency_key_names, pivot_event_key_names};
use host_processing::{load_and_merge_hosts, validate_and_filter_blocks};
use model::AnalysisData;
use report::{
    add_block_rows, add_block_scalar_rows, add_custom_block_rows, add_sync_gap_rows, add_tx_rows,
    build_table_title,
};

fn main() -> Result<()> {
    let args = Args::parse();
    if !args.log_path.exists() {
        return Err(anyhow!("log path not found: {}", args.log_path.display()));
    }

    let default_keys = default_latency_key_names();
    let pivot_keys = pivot_event_key_names();
    let mut data = AnalysisData::default();
    load_and_merge_hosts(&args.log_path, &mut data)?;

    if data.node_count == 0 {
        return Err(anyhow!("no nodes found (sync_cons_gap_stats empty)"));
    }

    validate_and_filter_blocks(&mut data, args.max_blocks);
    println!("{} nodes in total", data.node_count);
    println!("{} blocks generated", data.blocks.len());

    let tx_analysis = analyze_txs(&data);
    let (mut row_values, custom_keys) = build_block_row_values(&data, &default_keys, &pivot_keys);
    let (mut tx_latency_rows, mut tx_packed_rows) = build_tx_rows(&data);

    let scalars = collect_block_scalars(&data);
    print_throughput_and_slowest(&scalars, &tx_analysis.slowest_packed_hash);

    let mut table = build_table_title();
    add_block_rows(&mut table, &mut row_values);
    add_custom_block_rows(&mut table, &mut row_values, &custom_keys);
    add_tx_rows(
        &mut table,
        &mut tx_latency_rows,
        &mut tx_packed_rows,
        &tx_analysis,
        &data,
    );
    add_block_scalar_rows(&mut table, &scalars);
    add_sync_gap_rows(&mut table, &data);
    table.printstd();

    Ok(())
}
