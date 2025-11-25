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


def plot_with_matplotlib(quorum_data, output_file: str):
    """
    Generate simple statistics plot using matplotlib.

    Args:
        quorum_data: Dictionary mapping quorum to list of latencies
        output_file: Path to save plot image
    """
    try:
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print("matplotlib not available, skipping plot generation")
        return

    quorums = sorted(quorum_data.keys())
    means = []
    medians = []
    mins = []
    maxs = []

    for q in quorums:
        data = quorum_data[q]
        means.append(np.mean(data))
        medians.append(np.median(data))
        mins.append(np.min(data))
        maxs.append(np.max(data))

    plt.figure(figsize=(10, 6))

    # Plot lines
    plt.plot(quorums, maxs, "r--", label="Max", alpha=0.5)
    plt.plot(quorums, means, "b-o", label="Mean", linewidth=2)
    plt.plot(quorums, medians, "g-s", label="Median", linewidth=2)
    plt.plot(quorums, mins, "k--", label="Min", alpha=0.5)

    # Fill between min and max
    plt.fill_between(quorums, mins, maxs, color="gray", alpha=0.1)

    plt.xlabel("Write Quorum", fontsize=12)
    plt.ylabel("Latency (ms)", fontsize=12)
    plt.title("Write Latency Statistics by Quorum", fontsize=14)
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xticks(quorums)

    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    print(f"Plot saved to: {output_file}")
    plt.close()


def print_summary_table(quorum_data):
    """Print a summary table of results to the console."""
    print("\n" + "=" * 65)
    print(
        f"{'Quorum':<8} | {'Avg (ms)':<12} | {'Min (ms)':<10} | {'Max (ms)':<10} | {'Samples':<8}"
    )
    print("-" * 65)

    for q in sorted(quorum_data.keys()):
        latencies = quorum_data[q]
        if not latencies:
            continue

        avg = sum(latencies) / len(latencies)
        min_lat = min(latencies)
        max_lat = max(latencies)

        print(
            f"{q:<8} | {avg:<12.2f} | {min_lat:<10.2f} | {max_lat:<10.2f} | {len(latencies):<8}"
        )

    print("=" * 65 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Plot benchmark results")
    parser.add_argument(
        "--results-dir",
        default="results",
        help="Results directory (also used for output)",
    )
    parser.add_argument(
        "--output-dir",
        default="results",
        help="Output directory (default: same as results-dir)",
    )
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

    # Print summary table
    print_summary_table(quorum_data)

    # Generate plot
    plot_file = os.path.join(args.output_dir, "quorum_vs_latency.png")
    if not args.no_plot:
        plot_with_matplotlib(quorum_data, plot_file)


if __name__ == "__main__":
    main()
