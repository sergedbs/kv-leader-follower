#!/usr/bin/env python3
"""
Check consistency across leader and followers using Docker exec.

Usage:
    python scripts/check_consistency.py
    python scripts/check_consistency.py --sample-size 20
"""

import argparse
import json
import subprocess
import sys
import requests


def docker_exec_get(container: str, port: int, key: str):
    """Query a key from a container using docker exec."""
    try:
        cmd = [
            "docker",
            "exec",
            container,
            "python",
            "-c",
            f"import requests; r = requests.get('http://localhost:{port}/get', params={{'key': '{key}'}}); print(r.text)",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            data = json.loads(result.stdout.strip())
            if data.get("status") == "ok":
                return data.get("value")
        return None
    except Exception:
        return None


def read_leader_key(leader_url: str, key: str):
    """Read a key from the leader via localhost."""
    try:
        resp = requests.get(f"{leader_url}/get", params={"key": key}, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "ok":
                return data.get("value")
        return None
    except Exception:
        return None


def get_sample_keys(sample_size: int):
    """Generate a diverse sample of keys to check."""
    # Distribute sample across the key space (assuming 100 unique keys)
    step = 100 // sample_size
    return [f"key_{i}" for i in range(0, 100, step)][:sample_size]


def check_consistency(leader_url: str, containers: dict, sample_keys: list):
    """
    Check if all followers are consistent with leader.

    Args:
        leader_url: Leader service URL (localhost)
        containers: Dict mapping names to (container, port) tuples
        sample_keys: List of keys to check

    Returns:
        Dictionary with consistency results
    """
    results = {
        "leader": {"url": leader_url, "checked_keys": len(sample_keys)},
        "followers": [],
        "consistent": True,
        "summary": {},
    }

    consistent_count = 0
    inconsistent_keys = []

    for key in sample_keys:
        # Read from leader
        leader_value = read_leader_key(leader_url, key)
        if leader_value is None:
            continue

        # Check each follower
        key_consistent = True
        for name, (container, port) in list(containers.items())[1:]:  # Skip leader
            follower_value = docker_exec_get(container, port, key)
            if follower_value != leader_value:
                key_consistent = False
                inconsistent_keys.append(key)
                break

        if key_consistent:
            consistent_count += 1
        else:
            results["consistent"] = False

    # Per-follower summary
    for name, (container, port) in list(containers.items())[1:]:
        follower_result = {
            "name": name,
            "container": container,
            "port": port,
        }
        results["followers"].append(follower_result)

    # Summary
    results["summary"] = {
        "total_followers": len(containers) - 1,
        "checked_keys": len(sample_keys),
        "consistent_keys": consistent_count,
        "inconsistent_keys": inconsistent_keys[:10],  # Show first 10
        "consistency_percentage": (consistent_count / len(sample_keys) * 100)
        if sample_keys
        else 0,
    }

    return results


def print_results(results):
    """Print consistency check results in human-readable format."""
    print("\n" + "=" * 70)
    print("CONSISTENCY CHECK RESULTS")
    print("=" * 70)

    if "error" in results:
        print(f"\n❌ ERROR: {results['error']}")
        return

    # Leader info
    print(f"\nLeader: {results['leader']['url']}")
    print(f"Checked Keys: {results['leader']['checked_keys']}")

    # Overall status
    print(
        f"\nOverall Status: {'✅ CONSISTENT' if results['consistent'] else '❌ INCONSISTENT'}"
    )
    print(
        f"Consistent Keys: {results['summary']['consistent_keys']}/{results['summary']['checked_keys']} "
        f"({results['summary']['consistency_percentage']:.1f}%)"
    )

    # Follower info
    print(f"\nFollowers Checked: {results['summary']['total_followers']}")
    for follower in results["followers"]:
        print(
            f"  - {follower['name']}: {follower['container']} (port {follower['port']})"
        )

    # Inconsistencies
    if results["summary"]["inconsistent_keys"]:
        print(f"\nInconsistent Keys (first 10):")
        for key in results["summary"]["inconsistent_keys"]:
            print(f"  - {key}")

    print("\n" + "=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Check consistency across leader and followers using Docker"
    )
    parser.add_argument(
        "--leader-url", default="http://localhost:8000", help="Leader service URL"
    )
    parser.add_argument(
        "--sample-size", type=int, default=20, help="Number of keys to sample"
    )
    parser.add_argument("--json", action="store_true", help="Output results as JSON")

    args = parser.parse_args()

    # Container configuration
    containers = {
        "leader": ("kv-leader-follower-leader-1", 8000),
        "follower1": ("kv-leader-follower-follower1-1", 8001),
        "follower2": ("kv-leader-follower-follower2-1", 8002),
        "follower3": ("kv-leader-follower-follower3-1", 8003),
        "follower4": ("kv-leader-follower-follower4-1", 8004),
        "follower5": ("kv-leader-follower-follower5-1", 8005),
    }

    # Generate sample keys
    sample_keys = get_sample_keys(args.sample_size)

    # Check consistency
    results = check_consistency(args.leader_url, containers, sample_keys)

    # Output
    if args.json:
        print(json.dumps(results, indent=2))
    else:
        print_results(results)

    # Exit code
    if not results.get("consistent", False):
        sys.exit(1)


if __name__ == "__main__":
    main()
