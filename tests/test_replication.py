import time
from unittest.mock import Mock, patch
from app.leader.replication import Replicator, ReplicationResult


def test_replication_result_to_dict():
    """Test ReplicationResult converts to dictionary correctly."""
    result = ReplicationResult("follower1", "ok", 12.345)
    d = result.to_dict()
    assert d["follower"] == "follower1"
    assert d["status"] == "ok"
    assert d["latency_ms"] == 12.345
    assert "error" not in d

    result_with_error = ReplicationResult(
        "follower2", "error", 5.678, "Connection timeout"
    )
    d = result_with_error.to_dict()
    assert d["error"] == "Connection timeout"


def test_replicator_initialization():
    """Test Replicator initializes correctly."""
    followers = ["host1:8001", "host2:8002"]
    replicator = Replicator(followers=followers, min_delay=0.001, max_delay=0.002)
    assert replicator.followers == followers
    assert replicator.min_delay == 0.001
    assert replicator.max_delay == 0.002
    replicator.close()


def test_default_delay_in_range():
    """Test default delay returns value in specified range."""
    replicator = Replicator(followers=["host1:8001"], min_delay=0.001, max_delay=0.002)

    for _ in range(10):
        delay = replicator._default_delay()
        assert 0.001 <= delay <= 0.002

    replicator.close()


def test_replicate_to_one_success():
    """Test successful replication to one follower."""
    replicator = Replicator(
        followers=["localhost:8001"],
        min_delay=0.0,
        max_delay=0.0,
        delay_func=lambda: 0.0,  # No delay for testing
    )

    with patch.object(replicator.session, "post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        result = replicator._replicate_to_one("localhost:8001", "key1", "value1")

        assert result.follower == "localhost:8001"
        assert result.status == "ok"
        assert result.latency_ms >= 0
        assert result.error is None

        mock_post.assert_called_once()

    replicator.close()


def test_replicate_to_one_http_error():
    """Test replication with HTTP error response."""
    replicator = Replicator(
        followers=["localhost:8001"],
        min_delay=0.0,
        max_delay=0.0,
        delay_func=lambda: 0.0,
    )

    with patch.object(replicator.session, "post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_post.return_value = mock_response

        result = replicator._replicate_to_one("localhost:8001", "key1", "value1")

        assert result.status == "error"
        assert result.error is not None
        assert "500" in result.error

    replicator.close()


def test_replicate_to_one_connection_error():
    """Test replication with connection error."""
    replicator = Replicator(
        followers=["localhost:8001"],
        min_delay=0.0,
        max_delay=0.0,
        delay_func=lambda: 0.0,
    )

    with patch.object(replicator.session, "post") as mock_post:
        mock_post.side_effect = Exception("Connection refused")

        result = replicator._replicate_to_one("localhost:8001", "key1", "value1")

        assert result.status == "error"
        assert result.error is not None
        assert "Connection refused" in result.error

    replicator.close()


def test_replicate_to_all_followers():
    """Test concurrent replication to all followers."""
    followers = ["host1:8001", "host2:8002", "host3:8003"]
    replicator = Replicator(
        followers=followers, min_delay=0.0, max_delay=0.0, delay_func=lambda: 0.0
    )

    with patch.object(replicator.session, "post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        results = replicator.replicate("key1", "value1")

        assert len(results) == 3
        assert all(r.status == "ok" for r in results)
        assert mock_post.call_count == 3

    replicator.close()


def test_replicate_concurrent_execution():
    """Test that replication is actually concurrent."""
    followers = ["host1:8001", "host2:8002", "host3:8003"]

    # Custom delay that takes 0.1 seconds
    def slow_delay():
        return 0.1

    replicator = Replicator(
        followers=followers, min_delay=0.1, max_delay=0.1, delay_func=slow_delay
    )

    with patch.object(replicator.session, "post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        start = time.time()
        results = replicator.replicate("key1", "value1")
        elapsed = time.time() - start

        # If concurrent, should take ~0.1s, if sequential would take ~0.3s
        assert elapsed < 0.2  # Some margin for execution overhead
        assert len(results) == 3

    replicator.close()


def test_replicate_with_shared_secret():
    """Test that shared secret is included in headers."""
    replicator = Replicator(
        followers=["localhost:8001"],
        min_delay=0.0,
        max_delay=0.0,
        repl_secret="my-secret-key",
        delay_func=lambda: 0.0,
    )

    with patch.object(replicator.session, "post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        replicator._replicate_to_one("localhost:8001", "key1", "value1")

        # Check that the secret was included in headers
        call_args = mock_post.call_args
        headers = call_args[1]["headers"]
        assert headers["X-Replication-Secret"] == "my-secret-key"

    replicator.close()


def test_deterministic_delay_function():
    """Test that custom delay function can be injected."""
    call_count = 0

    def custom_delay():
        nonlocal call_count
        call_count += 1
        return 0.001 * call_count  # Incrementing delay

    replicator = Replicator(
        followers=["host1:8001", "host2:8002"],
        min_delay=0.0,
        max_delay=0.0,
        delay_func=custom_delay,
    )

    with patch.object(replicator.session, "post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        replicator.replicate("key1", "value1")

        # Delay function should be called for each follower
        assert call_count == 2

    replicator.close()
