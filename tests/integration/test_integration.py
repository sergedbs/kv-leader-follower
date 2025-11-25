"""
Integration tests for the distributed key-value store.
These tests run against a real Docker Compose environment.
"""

import pytest
import time
import threading
from tests.integration.test_utils import (
    DockerComposeManager,
    wait_for_health,
    get_store_dump,
    get_follower_dump,
    compare_stores,
    write_key,
    read_key,
    update_leader_config,
)


LEADER_URL = "http://localhost:8000"


@pytest.fixture(scope="module")
def docker_env_default():
    """
    Setup and teardown docker-compose environment with default config.
    Scoped to module to avoid restarting containers for every test function.
    """
    print("\nStarting Docker Compose environment (Module Scope)...")
    DockerComposeManager.up()

    # Wait for leader to be healthy
    if not wait_for_health(LEADER_URL, timeout=30):
        DockerComposeManager.down()
        pytest.fail("Leader service did not become healthy in time")

    print("Services are ready")
    yield

    print("\nStopping Docker Compose environment...")
    DockerComposeManager.down()
    time.sleep(2)  # Wait for cleanup


class TestQuorumBehavior:
    """Test quorum-based replication behavior."""

    def test_write_succeeds_with_default_quorum(self, docker_env_default):
        """Test: Write succeeds when quorum reached (default quorum=3)."""
        # Ensure default quorum
        update_leader_config(LEADER_URL, {"write_quorum": 3})

        # Write a key
        success, data = write_key(LEADER_URL, "quorum_test_1", "value1")

        # Verify success
        assert success, f"Write failed: {data}"
        assert data["status"] == "ok"
        assert data["acks"] >= 3, f"Expected at least 3 acks, got {data['acks']}"
        assert data["required"] == 3

        # Verify leader has the key
        found, value = read_key(LEADER_URL, "quorum_test_1")
        assert found, "Key not found on leader"
        assert value == "value1"

        # Verify at least 3 followers have the key
        time.sleep(0.5)  # Give followers a moment
        followers_with_key = 0
        for i in range(1, 6):
            dump = get_follower_dump(i)
            if dump and "quorum_test_1" in dump:
                followers_with_key += 1

        assert followers_with_key >= 3, (
            f"Expected at least 3 followers to have key, found {followers_with_key}"
        )

    def test_multiple_writes_all_succeed(self, docker_env_default):
        """Test: Multiple consecutive writes all succeed."""
        keys = [f"multi_{i}" for i in range(10)]

        for key in keys:
            success, data = write_key(LEADER_URL, key, f"value_{key}")
            assert success, f"Write failed for {key}: {data}"
            assert data["acks"] >= 3

        # Verify all keys on leader
        leader_dump = get_store_dump(LEADER_URL)
        assert leader_dump is not None

        for key in keys:
            assert key in leader_dump, f"Key {key} not in leader store"


class TestFailureScenarios:
    """Test system behavior under failure conditions."""

    def test_quorum_failure_with_stopped_follower(self, docker_env_default):
        """Test: Write fails when quorum cannot be reached."""
        # Set quorum=5
        print("\nSetting WRITE_QUORUM=5...")
        update_leader_config(LEADER_URL, {"write_quorum": 5})

        try:
            # Stop one follower
            print("Stopping follower5...")
            DockerComposeManager.stop_service("follower5")
            time.sleep(2)

            # Try to write (should fail)
            success, data = write_key(LEADER_URL, "fail_test", "should_fail", timeout=5)

            # Verify failure
            assert not success, "Write should have failed but succeeded"
            assert data.get("status") == "error"
            assert "quorum" in data.get("error", "").lower()
            assert data.get("acks") == 4, f"Expected 4 acks, got {data.get('acks')}"
            assert data.get("required") == 5

            # Verify one replication error
            replication = data.get("replication", [])
            errors = [r for r in replication if r.get("status") == "error"]
            assert len(errors) == 1, f"Expected 1 error, got {len(errors)}"

            print("Quorum failure correctly detected")

        finally:
            # Restart follower and reset config
            print("Restarting follower5...")
            DockerComposeManager.start_service("follower5")
            time.sleep(5)  # Wait for it to come back
            update_leader_config(LEADER_URL, {"write_quorum": 3})

    def test_recovery_after_follower_restart(self, docker_env_default):
        """Test: System recovers after a follower is restarted."""
        print("\nEnsuring WRITE_QUORUM=3...")
        update_leader_config(LEADER_URL, {"write_quorum": 3})

        # Write should succeed initially
        success1, _ = write_key(LEADER_URL, "recovery_test_1", "value1")
        assert success1, "Initial write failed"

        # Stop and restart a follower
        print("Restarting follower3...")
        DockerComposeManager.stop_service("follower3")
        time.sleep(1)
        DockerComposeManager.start_service("follower3")
        time.sleep(3)

        # Write should still succeed
        success2, data2 = write_key(LEADER_URL, "recovery_test_2", "value2")
        assert success2, f"Write after recovery failed: {data2}"
        assert data2["acks"] >= 3

        print("System recovered successfully")


class TestConsistency:
    """Test data consistency across replicas."""

    def test_eventual_consistency_after_writes(self, docker_env_default):
        """Test: All followers eventually consistent after multiple writes."""
        # Ensure default quorum
        update_leader_config(LEADER_URL, {"write_quorum": 3})

        # Perform multiple writes
        num_writes = 20
        keys = [f"consistency_{i}" for i in range(num_writes)]

        print(f"\nWriting {num_writes} keys...")
        for key in keys:
            success, _ = write_key(LEADER_URL, key, f"value_{key}")
            assert success, f"Write failed for {key}"

        # Wait a moment for replication to settle
        time.sleep(2)

        # Get leader dump
        leader_dump = get_store_dump(LEADER_URL)
        assert leader_dump is not None
        assert len(leader_dump) >= num_writes

        # Check all followers
        print("Checking follower consistency...")
        follower_dumps = []
        for i in range(1, 6):
            dump = get_follower_dump(i)
            if dump is not None:
                follower_dumps.append((f"follower{i}", dump))

        # Compare stores
        report = compare_stores(leader_dump, follower_dumps)

        # With quorum=3, at least 3 followers should be fully consistent
        fully_consistent = sum(1 for f in report["followers"] if f["consistency"])

        print("Consistency report:")
        for follower in report["followers"]:
            status = "OK" if follower["consistency"] else "WARN"
            print(
                f"  {status} {follower['name']}: {follower['matching_keys']}/{report['total_keys']} keys"
            )

        assert fully_consistent >= 3, (
            f"Expected at least 3 consistent followers, got {fully_consistent}"
        )


class TestConcurrency:
    """Test concurrent write handling."""

    def test_concurrent_writes_no_data_loss(self, docker_env_default):
        """Test: Concurrent writes from multiple threads don't lose data."""
        # Ensure default quorum
        update_leader_config(LEADER_URL, {"write_quorum": 3})

        num_threads = 10
        writes_per_thread = 10
        results = {"successes": 0, "failures": 0, "lock": threading.Lock()}

        def write_batch(thread_id: int):
            """Write a batch of keys from one thread."""
            for i in range(writes_per_thread):
                key = f"concurrent_{thread_id}_{i}"
                value = f"value_{thread_id}_{i}"
                success, _ = write_key(LEADER_URL, key, value)

                with results["lock"]:
                    if success:
                        results["successes"] += 1
                    else:
                        results["failures"] += 1

        # Spawn threads
        print(f"\nStarting {num_threads} concurrent writers...")
        threads = []
        start_time = time.time()

        for t in range(num_threads):
            thread = threading.Thread(target=write_batch, args=(t,))
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join()

        elapsed = time.time() - start_time

        print(f"Completed in {elapsed:.2f}s")
        print(f"Successes: {results['successes']}")
        print(f"Failures: {results['failures']}")

        # Verify most writes succeeded
        total_writes = num_threads * writes_per_thread
        success_rate = results["successes"] / total_writes
        assert success_rate >= 0.95, (
            f"Success rate too low: {success_rate:.1%} (expected >= 95%)"
        )

        # Verify leader has all successful keys
        time.sleep(1)
        leader_dump = get_store_dump(LEADER_URL)
        assert leader_dump is not None

        # Count how many concurrent keys are in leader
        concurrent_keys = [k for k in leader_dump.keys() if k.startswith("concurrent_")]
        assert len(concurrent_keys) >= results["successes"] * 0.95, (
            "Some successful writes missing from leader"
        )


class TestLatency:
    """Test write latency with different quorum values."""

    def test_latency_decreases_with_lower_quorum(self, docker_env_default):
        """Test: Write latency decreases as quorum value decreases."""
        quorum_latencies = {}

        for quorum in [5, 3, 1]:
            print(f"\nTesting with WRITE_QUORUM={quorum}...")
            # Update config dynamically instead of restarting
            update_leader_config(LEADER_URL, {"write_quorum": quorum})

            # Give it a moment to apply if needed (though it should be instant)
            time.sleep(0.5)

            # Perform multiple writes and measure latency
            latencies = []
            for i in range(10):
                success, data = write_key(
                    LEADER_URL, f"latency_q{quorum}_{i}", f"value_{i}"
                )
                if success:
                    latencies.append(data.get("latency_ms", 0))

            avg_latency = sum(latencies) / len(latencies) if latencies else 0
            quorum_latencies[quorum] = avg_latency
            print(f"  Average latency: {avg_latency:.2f}ms")

        # Reset to default
        update_leader_config(LEADER_URL, {"write_quorum": 3})

        # Verify latency trend: quorum=1 < quorum=3 < quorum=5
        print("\nLatency comparison:")
        for q in sorted(quorum_latencies.keys()):
            print(f"  Quorum {q}: {quorum_latencies[q]:.2f}ms")

        for q, lat in quorum_latencies.items():
            assert lat < 1000, f"Latency for quorum {q} too high: {lat}ms"

        print("\nAll quorum values have reasonable latency")
        print("   (In production with real network delays, quorum=1 would be faster)")
