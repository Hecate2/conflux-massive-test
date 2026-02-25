import argparse
from pathlib import Path

import numpy as np
from analyzer.sevenz_utils import is_supported_input
from tg_parse_rpy import RustGraph
from .analyze_rust_graph import load_network_result, median_graph, worst_graph, describe_blocks, best_graph
from .plot import plot_percentiles

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="绘制节点指标图表")
    
    parser.add_argument(
        "-l", "--log-path",
        type=str,
        required=True,
        help="日志根目录路径"
    )
    
    parser.add_argument(
        "-o", "--output-path",
        type=str,
        required=True,
        help="Figure 输出目录路径"
    )
    
    args = parser.parse_args()
    
    path = args.log_path
    if not is_supported_input(path):
        raise ValueError("Only directory logs and whole .7z archive inputs are supported")

    output_path = args.output_path
    Path(output_path).mkdir(parents=True, exist_ok=True)

    
    net_info = load_network_result(path)
    _labels, confirm_times, blocks, _graphs = net_info
    if len(confirm_times) == 0:
        print(f"No conflux.log.new_blocks found in {path}")
        raise SystemExit(0)
    print(f"Network avg time {confirm_times.mean(): .1f} median {np.median(confirm_times): .1f} with {blocks[2].mean(): .1f} blocks confirmed")
    

    
    print("\n==== The best node ====")
    print(describe_blocks(best_graph(net_info)))
    
    print("\n==== The median node ====")
    print(describe_blocks(median_graph(net_info)))
    
    print("\n==== The worst node ====")
    print(describe_blocks(worst_graph(net_info)))
    

    
    plot_percentiles(confirm_times, save_fig=f"{output_path}/confirm_delay.pdf")
    plot_percentiles(confirm_times, max_percentile = 95, save_fig=f"{output_path}/confirm_delay_no_tail.pdf")
