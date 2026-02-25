import argparse
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
from datetime import datetime
import numpy as np
from analyzer.sevenz_utils import is_supported_input, iter_selected_file_bytes
from .log_data_manage import GlobalMetricsStats, SingleNodeMetrics, NodeMetricsStats
from .utils import sanitize_metric_name
from .parse_metrics import plot_metrics_by_pecentiles


def _ms_to_datetime(ms: int):
    return datetime.fromtimestamp(ms / 1000)


def _plot_from_single_nodes(single_nodes: list[SingleNodeMetrics], metric_name: str, save_fig: str):
    node_stats_list = [NodeMetricsStats.load_percentiles(node, percentiles=(90,)) for node in single_nodes]
    global_stats = GlobalMetricsStats("in_memory_archive", node_stats_list)
    selected_nodes = global_stats.query_node_stat_at_percentiles(metric_name, "p90", [0, 10, 50, 90, 100])
    if selected_nodes is None:
        return

    node_by_path = {str(node.path): node for node in single_nodes}
    selected_nodes.reverse()
    tags = ["P100", "P90", "P50", "P10", "P0"]

    plt.figure(figsize=(12, 6))
    for tag, (node_path, _) in zip(tags, selected_nodes):
        node_metrics = node_by_path[str(node_path)]
        timestamps, values = node_metrics.query_metric(metric_name)
        dates = [_ms_to_datetime(int(ts)) for ts in timestamps]
        plt.plot(dates, values, linewidth=2, label=tag)

    plt.title(f"Compare of {metric_name}", fontsize=18)
    plt.xlabel("Time", fontsize=12)
    plt.ylabel("Value", fontsize=12)
    plt.ylim(bottom=0)
    ax = plt.gca()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.yaxis.set_major_formatter(ticker.EngFormatter())
    plt.grid(True, linestyle="--", alpha=0.7)
    plt.legend(fontsize=12, loc="upper right")
    plt.tight_layout()
    plt.savefig(save_fig)
    plt.close()


if __name__ == "__main__":    
    parser = argparse.ArgumentParser(description="绘制节点指标图表")
    
    parser.add_argument(
        "-l", "--log-path",
        type=str,
        required=True,
        help="日志节点目录路径"
    )
    
    parser.add_argument(
        "-o", "--output-path",
        type=str,
        required=True,
        help="输出目录路径"
    )
    
    parser.add_argument(
        "-m", "--metrics",
        type=str,
        nargs="*",
        default=[],
        help="绘制指标列表"
    )
    
    args = parser.parse_args()
    
    
    path = args.log_path
    output_path = args.output_path

    if not is_supported_input(path):
        raise ValueError("Only directory logs and whole .7z archive inputs are supported")
    
    Path(output_path).mkdir(parents=True, exist_ok=True)
    if Path(path).is_dir():
        GlobalMetricsStats.preprocessing(path)
        for metric in args.metrics:
            sanitized_metric = sanitize_metric_name(metric)
            plot_metrics_by_pecentiles(path, metric, figsize=(12, 6), save_fig = f"{output_path}/{sanitized_metric}.pdf")
    else:
        single_nodes = [
            SingleNodeMetrics.load_from_bytes(name, payload)
            for name, payload in iter_selected_file_bytes(path, "metrics.log")
        ]
        for metric in args.metrics:
            sanitized_metric = sanitize_metric_name(metric)
            _plot_from_single_nodes(single_nodes, metric, f"{output_path}/{sanitized_metric}.pdf")
