#!/usr/bin/env python3
"""
Plot benchmark results and generate analysis.

Usage:
    python scripts/plot_results.py --results-dir results --output-dir plots
"""

import argparse
import csv
import glob
import os
import sys


def load_results(results_dir: str):
    """
    Load all CSV results and group by quorum value.

    Args:
        results_dir: Directory containing result CSV files

    Returns:
        Dictionary mapping quorum value to list of latencies
    """
    quorum_data = {}

    pattern = os.path.join(results_dir, "quorum_*_trial_*.csv")
    files = glob.glob(pattern)

    if not files:
        print(f"No result files found matching pattern: {pattern}")
        return quorum_data

    for file in files:
        # Extract quorum from filename
        basename = os.path.basename(file)
        parts = basename.split("_")
        try:
            quorum = int(parts[1])
        except (IndexError, ValueError):
            print(f"Skipping file with unexpected format: {basename}")
            continue

        # Read latencies
        latencies = []
        with open(file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("success") == "True":
                    try:
                        latencies.append(float(row["latency_ms"]))
                    except (ValueError, KeyError):
                        pass

        if quorum not in quorum_data:
            quorum_data[quorum] = []
        quorum_data[quorum].extend(latencies)

    return quorum_data


def generate_analysis(quorum_data, output_file: str, plot_file: str):
    """
    Generate dynamic analysis of results with embedded plot.

    Args:
        quorum_data: Dictionary mapping quorum to list of latencies
        output_file: Path to save analysis markdown
        plot_file: Path to the generated plot image
    """
    with open(output_file, "w") as f:
        f.write("# Performance Analysis: Write Quorum vs Latency\n\n")
        
        # Embed the plot
        plot_filename = os.path.basename(plot_file)
        f.write(f"![Write Quorum vs Latency]({plot_filename})\n\n")
        
        f.write("## Benchmark Configuration\n\n")
        f.write("- **Total Writes**: ~10,000 concurrent writes\n")
        f.write("- **Concurrency**: >10 threads\n")
        f.write("- **Key Space**: 100 unique keys\n")
        f.write("- **Quorum Values Tested**: 1-5\n")
        f.write("- **Network Delay**: 1-10ms simulated per follower\n\n")
        
        f.write("## Results Summary\n\n")
        f.write(
            "| Quorum | Avg Latency (ms) | Std Dev (ms) | Min (ms) | Max (ms) | P50 (ms) | P95 (ms) | P99 (ms) | Samples |\n"
        )
        f.write(
            "|--------|------------------|--------------|----------|----------|----------|----------|----------|----------|\n"
        )

        stats = {}
        for q in sorted(quorum_data.keys()):
            latencies = quorum_data[q]
            if not latencies:
                continue

            avg = sum(latencies) / len(latencies)
            variance = sum((x - avg) ** 2 for x in latencies) / len(latencies)
            std = variance**0.5
            min_lat = min(latencies)
            max_lat = max(latencies)

            sorted_lat = sorted(latencies)
            p50 = sorted_lat[len(sorted_lat) // 2]
            p95 = sorted_lat[int(len(sorted_lat) * 0.95)]
            p99 = sorted_lat[int(len(sorted_lat) * 0.99)]
            
            stats[q] = {'avg': avg, 'std': std, 'min': min_lat, 'max': max_lat, 
                       'p50': p50, 'p95': p95, 'p99': p99, 'samples': len(latencies)}

            f.write(
                f"| {q} | {avg:.2f} | {std:.2f} | {min_lat:.2f} | {max_lat:.2f} | {p50:.2f} | {p95:.2f} | {p99:.2f} | {len(latencies)} |\n"
            )

        f.write("\n## Performance Analysis\n\n")

        if len(stats) > 1:
            quorums = sorted(stats.keys())
            avgs = [stats[q]['avg'] for q in quorums]
            
            min_q = quorums[avgs.index(min(avgs))]
            max_q = quorums[avgs.index(max(avgs))]
            min_avg = min(avgs)
            max_avg = max(avgs)
            
            f.write(f"### Latency vs Quorum Relationship\n\n")
            f.write(f"- **Minimum latency**: {min_avg:.2f}ms at quorum={min_q}\n")
            f.write(f"- **Maximum latency**: {max_avg:.2f}ms at quorum={max_q}\n")
            f.write(f"- **Latency increase**: {max_avg - min_avg:.2f}ms ({((max_avg/min_avg - 1) * 100):.1f}% increase from quorum={min_q} to quorum={max_q})\n\n")
            
            # Calculate trend
            if max_avg > min_avg * 1.1:  # More than 10% increase
                f.write("The data shows that **higher quorum values result in increased write latency**. ")
                f.write("This is expected because the leader must wait for acknowledgments from more followers ")
                f.write("before responding to the client.\n\n")
            else:
                f.write("The data shows **minimal latency variation** across quorum values. ")
                f.write("This suggests that network delays and parallel replication effectively mask ")
                f.write("the quorum synchronization overhead in this configuration.\n\n")
            
            # Per-quorum breakdown
            f.write("### Per-Quorum Analysis\n\n")
            for q in quorums:
                s = stats[q]
                f.write(f"**Quorum {q}**:\n")
                f.write(f"- Average latency: {s['avg']:.2f}ms (±{s['std']:.2f}ms)\n")
                f.write(f"- Median (P50): {s['p50']:.2f}ms\n")
                f.write(f"- 95th percentile: {s['p95']:.2f}ms\n")
                f.write(f"- Consistency guarantee: At least {q} out of 5 followers synchronized\n\n")

    print(f"Analysis saved to: {output_file}")


def plot_with_matplotlib(quorum_data, output_file: str):
    """
    Generate plot using matplotlib.

    Args:
        quorum_data: Dictionary mapping quorum to list of latencies
        output_file: Path to save plot image
    """
    try:
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print("matplotlib not available, skipping plot generation")
        print("Install with: pip install matplotlib numpy")
        return

    quorums = sorted(quorum_data.keys())
    avg_latencies = []
    std_latencies = []

    for q in quorums:
        latencies = quorum_data[q]
        avg_latencies.append(np.mean(latencies))
        std_latencies.append(np.std(latencies))

    plt.figure(figsize=(10, 6))
    plt.errorbar(
        quorums,
        avg_latencies,
        yerr=std_latencies,
        marker="o",
        capsize=5,
        capthick=2,
        linewidth=2,
        markersize=8,
        label="Average Latency ± Std Dev",
    )
    plt.xlabel("Write Quorum", fontsize=12)
    plt.ylabel("Average Write Latency (ms)", fontsize=12)
    plt.title("Write Quorum vs Average Latency", fontsize=14)
    plt.grid(True, alpha=0.3)
    plt.xticks(quorums)
    plt.legend()

    # Add value labels
    for q, lat in zip(quorums, avg_latencies):
        plt.text(
            q,
            lat + max(avg_latencies) * 0.02,
            f"{lat:.2f}ms",
            ha="center",
            va="bottom",
            fontsize=10,
        )

    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    print(f"Plot saved to: {output_file}")
    plt.close()


def main():
    parser = argparse.ArgumentParser(description="Plot benchmark results")
    parser.add_argument("--results-dir", default="results", help="Results directory (also used for output)")
    parser.add_argument("--output-dir", default="results", help="Output directory (default: same as results-dir)")
    parser.add_argument("--no-plot", action="store_true", help="Skip plot generation")

    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    # Load data
    print(f"Loading results from: {args.results_dir}")
    quorum_data = load_results(args.results_dir)

    if not quorum_data:
        print("No results found!")
        sys.exit(1)

    print(f"Found data for quorum values: {sorted(quorum_data.keys())}")

    # Generate plot first
    plot_file = os.path.join(args.output_dir, "quorum_vs_latency.png")
    if not args.no_plot:
        plot_with_matplotlib(quorum_data, plot_file)
    
    # Generate analysis with embedded plot
    analysis_file = os.path.join(args.output_dir, "analysis.md")
    generate_analysis(quorum_data, analysis_file, plot_file)


if __name__ == "__main__":
    main()
