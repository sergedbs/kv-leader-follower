#!/usr/bin/env python3
"""
Check consistency across leader and followers using /dump endpoint.

Usage:
    python scripts/check_consistency.py
"""

import argparse
import json
import subprocess
import sys
import requests


def get_leader_dump(leader_url: str):
    """Fetch leader's complete data store via /dump."""
    try:
        resp = requests.get(f"{leader_url}/dump", timeout=5)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") == "ok":
            return data.get("store", {})
        return None
    except requests.RequestException as e:
        print(f"Error fetching leader dump: {e}")
        return None


def get_follower_dump(container: str, port: int):
    """Query follower's complete data store via docker exec."""
    try:
        cmd = [
            "docker",
            "exec",
            container,
            "python",
            "-c",
            f"import requests; r = requests.get('http://localhost:{port}/dump'); print(r.text)",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            data = json.loads(result.stdout.strip())
            if data.get("status") == "ok":
                return data.get("store", {})
        return None
    except Exception:
        return None


def compare_stores(leader_data, follower_data):
    """
    Compare leader and follower data stores.

    Returns:
        (missing_keys, extra_keys, mismatched_values)
    """
    leader_keys = set(leader_data.keys())
    follower_keys = set(follower_data.keys())

    missing_keys = leader_keys - follower_keys
    extra_keys = follower_keys - leader_keys

    mismatched_values = []
    for key in leader_keys & follower_keys:
        if leader_data[key] != follower_data[key]:
            mismatched_values.append((key, leader_data[key], follower_data[key]))

    return missing_keys, extra_keys, mismatched_values


def check_consistency(leader_url: str, containers: dict):
    """
    Check if all followers are consistent with leader.

    Args:
        leader_url: Leader service URL (localhost)
        containers: Dict mapping names to (container, port) tuples

    Returns:
        Dictionary with consistency results
    """
    leader_data = get_leader_dump(leader_url)
    if leader_data is None:
        return {"error": "Failed to get leader data"}

    results = {
        "leader": {"url": leader_url, "keys": len(leader_data)},
        "followers": [],
        "consistent": True,
        "summary": {},
    }

    for name, (container, port) in list(containers.items())[1:]:  # Skip leader
        follower_data = get_follower_dump(container, port)
        if follower_data is None:
            results["followers"].append(
                {"name": name, "container": container, "error": "Failed to fetch data"}
            )
            results["consistent"] = False
            continue

        missing, extra, mismatched = compare_stores(leader_data, follower_data)

        is_consistent = not (missing or extra or mismatched)
        if not is_consistent:
            results["consistent"] = False

        follower_result = {
            "name": name,
            "container": container,
            "port": port,
            "keys": len(follower_data),
            "consistent": is_consistent,
            "missing_keys": list(missing),
            "extra_keys": list(extra),
            "mismatched_values": [
                {"key": k, "leader_value": lv, "follower_value": fv}
                for k, lv, fv in mismatched
            ],
        }

        results["followers"].append(follower_result)

    # Summary
    total_followers = len(containers) - 1
    consistent_followers = sum(
        1 for f in results["followers"] if f.get("consistent", False)
    )
    results["summary"] = {
        "total_followers": total_followers,
        "consistent_followers": consistent_followers,
        "total_keys": len(leader_data),
        "consistency_percentage": (consistent_followers / total_followers * 100)
        if total_followers > 0
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
    print(f"Total Keys: {results['leader']['keys']}")

    # Overall status
    print(
        f"\nOverall Status: {'✅ CONSISTENT' if results['consistent'] else '❌ INCONSISTENT'}"
    )
    print(
        f"Consistent Followers: {results['summary']['consistent_followers']}/{results['summary']['total_followers']} "
        f"({results['summary']['consistency_percentage']:.1f}%)"
    )

    # Follower details
    print(f"\nFollower Details:")
    for follower in results["followers"]:
        if "error" in follower:
            print(f"  ❌ {follower['name']}: {follower['error']}")
        elif follower["consistent"]:
            print(
                f"  ✅ {follower['name']}: {follower['keys']} keys (port {follower['port']})"
            )
        else:
            print(
                f"  ❌ {follower['name']}: {follower['keys']} keys (port {follower['port']}) - INCONSISTENT"
            )
            if follower["missing_keys"]:
                print(f"     Missing keys: {len(follower['missing_keys'])}")
            if follower["extra_keys"]:
                print(f"     Extra keys: {len(follower['extra_keys'])}")
            if follower["mismatched_values"]:
                print(f"     Mismatched values: {len(follower['mismatched_values'])}")

    print("\n" + "=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Check consistency across leader and followers using /dump"
    )
    parser.add_argument(
        "--leader-url", default="http://localhost:8000", help="Leader service URL"
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

    # Check consistency
    results = check_consistency(args.leader_url, containers)

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
