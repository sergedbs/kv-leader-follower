#!/usr/bin/env python3
"""
Benchmark script for measuring KV store write performance.

Usage:
    python scripts/run_benchmark.py --writes 1000 --threads 10 --quorum 3
"""

import argparse
import time
import random
import requests
import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict


def write_one(leader_url: str, key: str, value: str) -> Dict:
    """
    Perform one write and measure latency.

    Args:
        leader_url: Base URL of the leader
        key: Key to write
        value: Value to write

    Returns:
        Dictionary with write result and metrics
    """
    start = time.time()
    try:
        response = requests.post(
            f"{leader_url}/set", json={"key": key, "value": value}, timeout=10
        )
        latency_ms = (time.time() - start) * 1000

        return {
            "key": key,
            "success": response.status_code == 200,
            "latency_ms": latency_ms,
            "acks": response.json().get("acks", 0)
            if response.status_code == 200
            else 0,
        }
    except Exception as e:
        latency_ms = (time.time() - start) * 1000
        return {
            "key": key,
            "success": False,
            "latency_ms": latency_ms,
            "acks": 0,
            "error": str(e),
        }


def run_benchmark(
    leader_url: str, num_writes: int, num_threads: int, num_keys: int, output_file: str
) -> float:
    """
    Run benchmark with specified parameters.

    Args:
        leader_url: Base URL of the leader
        num_writes: Total number of writes to perform
        num_threads: Number of concurrent threads
        num_keys: Number of unique keys to use
        output_file: Path to save results CSV

    Returns:
        Average latency in milliseconds
    """
    print(
        f"Starting benchmark: {num_writes} writes, {num_threads} threads, {num_keys} unique keys"
    )

    # Generate key pool
    keys = [f"key_{i}" for i in range(num_keys)]

    results = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []

        for i in range(num_writes):
            key = random.choice(keys)
            value = f"value_{i}"
            future = executor.submit(write_one, leader_url, key, value)
            futures.append(future)

        # Collect results
        for future in as_completed(futures):
            result = future.result()
            results.append(result)

    total_time = time.time() - start_time

    # Save to CSV
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["key", "success", "latency_ms", "acks", "error"]
        )
        writer.writeheader()
        writer.writerows(results)

    # Compute statistics
    successful = [r for r in results if r["success"]]
    latencies = [r["latency_ms"] for r in successful]

    if latencies:
        avg_latency = sum(latencies) / len(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        latencies_sorted = sorted(latencies)
        p50 = latencies_sorted[len(latencies_sorted) // 2]
        p95 = latencies_sorted[int(len(latencies_sorted) * 0.95)]
        p99 = latencies_sorted[int(len(latencies_sorted) * 0.99)]

        print("\n=== Results ===")
        print(f"Total time: {total_time:.2f}s")
        print(f"Throughput: {num_writes / total_time:.1f} writes/sec")
        print(f"Total writes: {len(results)}")
        print(
            f"Successful: {len(successful)} ({len(successful) / len(results) * 100:.1f}%)"
        )
        print(f"Failed: {len(results) - len(successful)}")
        print("\nLatency Statistics (ms):")
        print(f"  Average: {avg_latency:.2f}")
        print(f"  Min: {min_latency:.2f}")
        print(f"  Max: {max_latency:.2f}")
        print(f"  P50: {p50:.2f}")
        print(f"  P95: {p95:.2f}")
        print(f"  P99: {p99:.2f}")
        print(f"\nResults saved to: {output_file}")

        return avg_latency
    else:
        print("No successful writes!")
        return 0.0


def main():
    parser = argparse.ArgumentParser(description="Benchmark KV store write performance")
    parser.add_argument(
        "--leader-url", default="http://localhost:8000", help="Leader URL"
    )
    parser.add_argument(
        "--writes", type=int, default=10000, help="Number of writes (default: 10000)"
    )
    parser.add_argument(
        "--threads", type=int, default=15, help="Number of threads (default: 15)"
    )
    parser.add_argument(
        "--keys", type=int, default=100, help="Number of unique keys (default: 100)"
    )
    parser.add_argument("--quorum", type=int, help="Write quorum value (for naming)")
    parser.add_argument("--trial", type=int, default=1, help="Trial number")
    parser.add_argument("--output-dir", default="results", help="Output directory")

    args = parser.parse_args()

    # Create output file name
    os.makedirs(args.output_dir, exist_ok=True)

    if args.quorum:
        output_file = f"{args.output_dir}/quorum_{args.quorum}_trial_{args.trial}.csv"
    else:
        output_file = f"{args.output_dir}/benchmark_trial_{args.trial}.csv"

    run_benchmark(args.leader_url, args.writes, args.threads, args.keys, output_file)


if __name__ == "__main__":
    main()
