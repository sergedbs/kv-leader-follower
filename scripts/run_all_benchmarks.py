#!/usr/bin/env python3
"""
Run complete benchmark suite: benchmarks, plots, and consistency checks.

This script:
1. Ensures Docker services are running
2. Runs benchmarks for multiple quorum values
3. Generates plots and analysis
4. Checks consistency across followers
5. Produces a comprehensive report

Usage:
    python scripts/run_all_benchmarks.py
    python scripts/run_all_benchmarks.py --quorums 1 3 5 --trials 3
    python scripts/run_all_benchmarks.py --quick  # Fast run with fewer samples
"""

import argparse
import subprocess
import sys
import time
import os
import requests


def run_command(cmd, description, check=True):
    """Run a shell command and handle errors."""
    print(f"\n{'=' * 70}")
    print(f"▶ {description}")
    print(f"{'=' * 70}")

    result = subprocess.run(cmd, shell=True, capture_output=False, text=True)

    if check and result.returncode != 0:
        print(f"\n❌ Failed: {description}")
        sys.exit(1)

    return result.returncode == 0


def check_docker_services():
    """Check if Docker services are running."""
    print("\n" + "=" * 70)
    print("Checking Docker Services")
    print("=" * 70)

    result = subprocess.run(
        "docker compose ps --format json", shell=True, capture_output=True, text=True
    )

    if result.returncode != 0:
        print("❌ Docker Compose not available")
        return False

    # Check if containers are running
    result = subprocess.run(
        "docker ps --filter name=kv-leader-follower --format '{{.Names}}' | wc -l",
        shell=True,
        capture_output=True,
        text=True,
    )

    running = int(result.stdout.strip())
    if running < 6:
        print(f"⚠️  Only {running}/6 containers running")
        return False

    print("✅ All 6 containers are running")
    return True


def start_docker_services():
    """Start Docker Compose services."""
    run_command("docker compose up -d", "Starting Docker services")

    # Wait for services to be healthy
    print("\nWaiting for services to be ready...", end="", flush=True)
    for i in range(10):
        time.sleep(1)
        try:
            resp = requests.get("http://localhost:8000/health", timeout=2)
            if resp.status_code == 200:
                print(" ✅")
                return True
        except Exception:
            print(".", end="", flush=True)

    print(" ⚠️  Services may not be fully ready")
    return True


def run_benchmarks(quorums, trials, writes, threads):
    """Run benchmarks for specified quorum values."""
    print("\n" + "=" * 70)
    print("Running Benchmarks")
    print("=" * 70)
    print(f"Quorums: {quorums}")
    print(f"Trials per quorum: {trials}")
    print(f"Writes per trial: {writes}")
    print(f"Concurrent threads: {threads}")

    total = len(quorums) * trials
    completed = 0

    for quorum in quorums:
        for trial in range(1, trials + 1):
            completed += 1
            print(f"\n[{completed}/{total}] Quorum={quorum}, Trial={trial}")

            cmd = (
                f"python scripts/run_benchmark.py "
                f"--leader-url http://localhost:8000 "
                f"--quorum {quorum} "
                f"--trial {trial} "
                f"--writes {writes} "
                f"--threads {threads}"
            )

            result = subprocess.run(cmd, shell=True)
            if result.returncode != 0:
                print(f"❌ Benchmark failed for quorum={quorum}, trial={trial}")
                return False

    print(f"\n✅ All {total} benchmarks completed successfully")
    return True


def generate_plots():
    """Generate plots and analysis from benchmark results."""
    return run_command(
        "python scripts/plot_results.py", "Generating plots and analysis"
    )


def check_consistency(sample_size):
    """Check consistency across followers."""
    return run_command(
        f"python scripts/check_consistency.py --sample-size {sample_size}",
        "Checking consistency across followers",
        check=False,  # Don't fail if consistency check fails
    )


def display_summary():
    """Display summary of results."""
    print("\n" + "=" * 70)
    print("BENCHMARK SUITE COMPLETE")
    print("=" * 70)

    # Check for results
    if os.path.exists("results"):
        result_files = [f for f in os.listdir("results") if f.endswith(".csv")]
        print(f"\n✅ Results: {len(result_files)} benchmark files in results/")

    if os.path.exists("plots/analysis.md"):
        print("✅ Analysis: plots/analysis.md")

    if os.path.exists("plots/quorum_vs_latency.png"):
        print("✅ Plot: plots/quorum_vs_latency.png")

    # Show analysis summary
    if os.path.exists("plots/analysis.md"):
        print("\n" + "-" * 70)
        print("Analysis Summary:")
        print("-" * 70)
        with open("plots/analysis.md", "r") as f:
            lines = f.readlines()
            # Print the results table
            in_table = False
            for line in lines:
                if line.startswith("|"):
                    in_table = True
                    print(line.rstrip())
                elif in_table and not line.strip():
                    break

    print("\n" + "=" * 70)
    print("View full analysis: cat plots/analysis.md")
    print("View plot: open plots/quorum_vs_latency.png")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Run complete benchmark suite with analysis and consistency checks"
    )
    parser.add_argument(
        "--quorums",
        type=int,
        nargs="+",
        default=[1, 3, 5],
        help="Quorum values to test (default: 1 3 5)",
    )
    parser.add_argument(
        "--trials", type=int, default=2, help="Number of trials per quorum (default: 2)"
    )
    parser.add_argument(
        "--writes",
        type=int,
        default=1000,
        help="Number of writes per trial (default: 1000)",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=10,
        help="Number of concurrent threads (default: 10)",
    )
    parser.add_argument(
        "--consistency-sample",
        type=int,
        default=20,
        help="Number of keys to check for consistency (default: 20)",
    )
    parser.add_argument(
        "--quick", action="store_true", help="Quick mode: fewer writes and trials"
    )
    parser.add_argument(
        "--skip-docker-check", action="store_true", help="Skip Docker service checks"
    )
    parser.add_argument(
        "--clean", action="store_true", help="Clean results before running"
    )

    args = parser.parse_args()

    # Quick mode overrides
    if args.quick:
        args.trials = 1
        args.writes = 500
        args.consistency_sample = 10
        print("\n🚀 Quick mode enabled: reduced trials and writes")

    # Clean old results
    if args.clean:
        run_command("rm -rf results/* plots/*", "Cleaning old results", check=False)

    # Check/start Docker services
    if not args.skip_docker_check:
        if not check_docker_services():
            print("\n⚠️  Docker services not running. Starting them...")
            if not start_docker_services():
                print("\n❌ Failed to start Docker services")
                sys.exit(1)
        else:
            # Verify leader is accessible
            try:
                resp = requests.get("http://localhost:8000/health", timeout=5)
                if resp.status_code != 200:
                    print("\n❌ Leader not responding correctly")
                    sys.exit(1)
            except Exception as e:
                print(f"\n❌ Cannot reach leader: {e}")
                sys.exit(1)

    # Run benchmark suite
    start_time = time.time()

    # 1. Run benchmarks
    if not run_benchmarks(args.quorums, args.trials, args.writes, args.threads):
        print("\n❌ Benchmarks failed")
        sys.exit(1)

    # 2. Generate plots
    if not generate_plots():
        print("\n❌ Plot generation failed")
        sys.exit(1)

    # 3. Check consistency
    if not check_consistency(args.consistency_sample):
        print("\n⚠️  Consistency check detected inconsistencies")

    # Calculate duration
    duration = time.time() - start_time
    minutes = int(duration // 60)
    seconds = int(duration % 60)

    print(f"\n⏱️  Total time: {minutes}m {seconds}s")

    # Display summary
    display_summary()


if __name__ == "__main__":
    main()
