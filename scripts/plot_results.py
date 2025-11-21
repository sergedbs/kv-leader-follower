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


def generate_analysis(quorum_data, output_file: str):
    """
    Generate textual analysis of results.

    Args:
        quorum_data: Dictionary mapping quorum to list of latencies
        output_file: Path to save analysis markdown
    """
    with open(output_file, "w") as f:
        f.write("# Performance Analysis: Write Quorum vs Latency\n\n")
        f.write("## Results Summary\n\n")
        f.write(
            "| Quorum | Avg Latency (ms) | Std Dev (ms) | Min (ms) | Max (ms) | P50 (ms) | P95 (ms) | P99 (ms) | Samples |\n"
        )
        f.write(
            "|--------|------------------|--------------|----------|----------|----------|----------|----------|----------|\n"
        )

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

            f.write(
                f"| {q} | {avg:.2f} | {std:.2f} | {min_lat:.2f} | {max_lat:.2f} | {p50:.2f} | {p95:.2f} | {p99:.2f} | {len(latencies)} |\n"
            )

        f.write("\n## Analysis\n\n")
        f.write("### Key Observations:\n\n")

        if len(quorum_data) > 1:
            quorums = sorted(quorum_data.keys())
            avgs = [sum(quorum_data[q]) / len(quorum_data[q]) for q in quorums]

            f.write(
                f"1. **Latency Range**: Write latencies range from {min(avgs):.2f}ms (quorum={quorums[avgs.index(min(avgs))]}) "
            )
            f.write(
                f"to {max(avgs):.2f}ms (quorum={quorums[avgs.index(max(avgs))]})\n\n"
            )

            if avgs[-1] > avgs[0]:
                f.write(
                    "2. **Quorum Impact**: Higher quorum values show increased latency "
                )
                f.write(
                    f"({avgs[-1] / avgs[0]:.1f}x difference between quorum={quorums[-1]} and quorum={quorums[0]})\n\n"
                )
            else:
                f.write(
                    "2. **Quorum Impact**: Latency differences are minimal across quorum values in this environment\n\n"
                )

            # Calculate recommended quorum (majority: 3 out of 5 followers)
            recommended_quorum = 3
            f.write(
                f"3. **Recommended Quorum**: Quorum={recommended_quorum} (majority) provides good balance "
            )
            f.write("between consistency and performance\n\n")

        f.write("### System Characteristics:\n\n")
        f.write("- **Replication**: Semi-synchronous with configurable quorum\n")
        f.write("- **Network Delay**: Simulated 0.1-1ms per follower\n")
        f.write("- **Consistency**: At least N followers synchronized before success\n")
        f.write("- **Failure Handling**: Returns error when quorum not reached\n\n")

        f.write("### Trade-offs:\n\n")
        f.write(
            "- **Lower Quorum (1-2)**: Faster writes, lower consistency guarantee\n"
        )
        f.write("- **Medium Quorum (3)**: Balanced consistency and performance\n")
        f.write("- **Higher Quorum (4-5)**: Stronger consistency, higher latency\n")

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
    parser.add_argument("--results-dir", default="results", help="Results directory")
    parser.add_argument("--output-dir", default="plots", help="Output directory")
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

    # Generate analysis
    analysis_file = os.path.join(args.output_dir, "analysis.md")
    generate_analysis(quorum_data, analysis_file)

    # Generate plot
    if not args.no_plot:
        plot_file = os.path.join(args.output_dir, "quorum_vs_latency.png")
        plot_with_matplotlib(quorum_data, plot_file)


if __name__ == "__main__":
    main()
