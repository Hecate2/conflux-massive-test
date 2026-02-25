extern crate tree_graph_parse_rust;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use rayon::prelude::*;
use std::ffi::OsStr;
use std::process::Command;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use tree_graph_parse_rust::graph::Graph;

#[derive(Parser, Debug)]
#[command(about = "Analyze all nodes from raw logs or a whole .7z archive")]
struct Args {
    #[arg(short = 'l', long = "log-path")]
    log_path: PathBuf,

    #[arg(long = "adv-percent", default_value_t = 10)]
    adv_percent: usize,

    #[arg(long = "risk", default_value_t = 1e-6)]
    risk: f64,
}

fn find_files(root_path: &Path, pattern: &str) -> Vec<PathBuf> {
    let mut matching_files = Vec::new();
    for entry in WalkDir::new(root_path)
        .follow_links(true)
        .into_iter()
        .filter_map(|entry| entry.ok())
    {
        let path = entry.path();
        if path.is_file() && path.file_name().and_then(|name| name.to_str()) == Some(pattern) {
            matching_files.push(path.to_path_buf());
        }
    }

    matching_files.sort();
    matching_files
}

fn sevenz_binary() -> Result<&'static str> {
    for bin in ["7zz", "7z"] {
        if Command::new(bin).arg("-h").output().is_ok() {
            return Ok(bin);
        }
    }
    Err(anyhow!("7z/7zz binary not found in PATH"))
}

fn list_new_blocks_members(path: &Path) -> Result<Vec<String>> {
    let bin = sevenz_binary()?;
    let output = Command::new(bin)
        .arg("l")
        .arg("-slt")
        .arg(path)
        .output()
        .with_context(|| format!("failed to list archive {}", path.display()))?;
    if !output.status.success() {
        return Err(anyhow!(
            "failed to list archive {}: {}",
            path.display(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut members = Vec::new();
    for line in stdout.lines() {
        let Some(path_part) = line.strip_prefix("Path = ") else {
            continue;
        };
        if path_part.ends_with("conflux.log.new_blocks") {
            members.push(path_part.to_string());
        }
    }

    members.sort();
    Ok(members)
}

fn extract_member(path: &Path, member: &str) -> Result<Vec<u8>> {
    let bin = sevenz_binary()?;
    let output = Command::new(bin)
        .arg("x")
        .arg("-so")
        .arg(path)
        .arg(member)
        .output()
        .with_context(|| format!("failed to extract member {} from {}", member, path.display()))?;
    if !output.status.success() {
        return Err(anyhow!(
            "failed to extract member {} from {}: {}",
            member,
            path.display(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok(output.stdout)
}

enum GraphInput {
    Path(PathBuf),
    ArchiveMember(String, String),
}

fn collect_inputs(input: &Path) -> Result<Vec<GraphInput>> {
    if input.is_dir() {
        let paths = find_files(input, "conflux.log.new_blocks")
            .into_iter()
            .map(GraphInput::Path)
            .collect();
        return Ok(paths);
    }

    if input.is_file() && input.extension() == Some(OsStr::new("7z")) {
        let members = list_new_blocks_members(input)?;
        if members.is_empty() {
            return Err(anyhow!(
                "no conflux.log.new_blocks found in archive {}",
                input.display()
            ));
        }

        let mut result = Vec::with_capacity(members.len());
        for member in members {
            let bytes = extract_member(input, &member)?;
            let text = String::from_utf8(bytes)
                .with_context(|| format!("member {} is not valid UTF-8", member))?;
            result.push(GraphInput::ArchiveMember(member, text));
        }

        return Ok(result);
    }

    Err(anyhow!(
        "invalid log path {}: expected a directory or .7z archive",
        input.display()
    ))
}

fn load_all_graphs(inputs: &[GraphInput]) -> Result<Vec<Graph>> {
    inputs
        .par_iter()
        .map(|input| match input {
            GraphInput::Path(path) => Graph::load(path.to_string_lossy().as_ref())
                .with_context(|| format!("failed to load {}", path.display())),
            GraphInput::ArchiveMember(name, content) => {
                Graph::load_from_text(content).with_context(|| format!("failed to load {}", name))
            }
        })
        .collect()
}

fn run_main() -> Result<()> {
    let _ = rayon::ThreadPoolBuilder::new()
        .stack_size(32 * 1024 * 1024)
        .build_global();

    let args = Args::parse();
    let inputs = collect_inputs(&args.log_path)?;
    if inputs.is_empty() {
        println!(
            "No conflux.log.new_blocks found under {}",
            args.log_path.display()
        );
        return Ok(());
    }
    println!("Found {} matching files", inputs.len());

    let graphs = load_all_graphs(&inputs)?;
    println!("Successfully loaded {} graphs", graphs.len());

    graphs.par_iter().for_each(|x| {
        x.avg_confirm_time(args.adv_percent, args.risk);
    });

    Ok(())
}

fn main() -> Result<()> {
    let handle = std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(run_main)
        .map_err(|e| anyhow!("failed to start worker thread: {}", e))?;

    let result = handle
        .join()
        .map_err(|_| anyhow!("worker thread panicked"))?;

    result
}
