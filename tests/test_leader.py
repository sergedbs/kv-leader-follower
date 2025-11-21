import pytest
import os
from unittest.mock import patch
import app.leader.app as leader_module
from app.leader.replication import ReplicationResult


@pytest.fixture
def client():
    """Create a test client for the leader app."""
    leader_module.app.config["TESTING"] = True
    with leader_module.app.test_client() as client:
        # Clear store before each test
        leader_module.store.clear()
        yield client


@pytest.fixture(autouse=True)
def set_leader_env():
    """Set up leader environment variables."""
    original_env = os.environ.copy()
    os.environ["ROLE"] = "leader"
    os.environ["PORT"] = "8000"
    os.environ["FOLLOWERS"] = "host1:8001,host2:8002,host3:8003"
    os.environ["WRITE_QUORUM"] = "2"

    # Reload the config after setting environment
    from app.common.config import Config

    leader_module.config = Config.from_env()

    yield
    os.environ.clear()
    os.environ.update(original_env)


def test_health_endpoint(client):
    """Test GET /health returns healthy status."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.get_json()
    assert data["status"] == "healthy"
    assert data["role"] == "leader"


def test_get_endpoint(client):
    """Test GET /get works correctly."""
    leader_module.store.set("testkey", "testvalue")
    response = client.get("/get?key=testkey")
    assert response.status_code == 200
    data = response.get_json()
    assert data["value"] == "testvalue"


def test_dump_endpoint(client):
    """Test GET /dump returns all keys."""
    leader_module.store.set("key1", "value1")
    leader_module.store.set("key2", "value2")

    response = client.get("/dump")
    assert response.status_code == 200
    data = response.get_json()
    assert data["store"] == {"key1": "value1", "key2": "value2"}


def test_set_without_json(client):
    """Test POST /set without JSON returns 400."""
    response = client.post("/set", data="not json")
    assert response.status_code == 400
    data = response.get_json()
    assert data["status"] == "error"


def test_set_missing_key(client):
    """Test POST /set with missing key field returns 400."""
    response = client.post(
        "/set", json={"value": "myvalue"}, content_type="application/json"
    )
    assert response.status_code == 400
    data = response.get_json()
    assert "Missing key or value" in data["error"]


def test_set_missing_value(client):
    """Test POST /set with missing value field returns 400."""
    response = client.post(
        "/set", json={"key": "mykey"}, content_type="application/json"
    )
    assert response.status_code == 400
    data = response.get_json()
    assert "Missing key or value" in data["error"]


@patch("app.leader.app.replicator")
def test_set_success_with_quorum(mock_replicator, client):
    """Test POST /set succeeds when quorum is reached."""
    # Mock successful replication to 3 followers (quorum is 2)
    mock_replicator.replicate.return_value = [
        ReplicationResult("host1:8001", "ok", 5.0),
        ReplicationResult("host2:8002", "ok", 6.0),
        ReplicationResult("host3:8003", "ok", 7.0),
    ]

    response = client.post(
        "/set",
        json={"key": "testkey", "value": "testvalue"},
        content_type="application/json",
    )

    assert response.status_code == 200
    data = response.get_json()
    assert data["status"] == "ok"
    assert data["acks"] == 3
    assert data["required"] == 2
    assert "latency_ms" in data
    assert len(data["replication"]) == 3

    # Verify the key was stored locally
    assert leader_module.store.get("testkey") == "testvalue"


@patch("app.leader.app.replicator")
def test_set_failure_quorum_not_reached(mock_replicator, client):
    """Test POST /set fails when quorum is not reached."""
    # Mock only 1 successful replication (quorum is 2)
    mock_replicator.replicate.return_value = [
        ReplicationResult("host1:8001", "ok", 5.0),
        ReplicationResult("host2:8002", "error", 6.0, "Connection failed"),
        ReplicationResult("host3:8003", "error", 7.0, "Timeout"),
    ]

    response = client.post(
        "/set",
        json={"key": "testkey", "value": "testvalue"},
        content_type="application/json",
    )

    assert response.status_code == 500
    data = response.get_json()
    assert data["status"] == "error"
    assert data["acks"] == 1
    assert data["required"] == 2
    assert "Quorum not reached" in data["error"]

    # Verify the key was still stored locally
    assert leader_module.store.get("testkey") == "testvalue"


@patch("app.leader.app.replicator")
def test_set_writes_locally_before_replication(mock_replicator, client):
    """Test that leader writes locally before replicating."""
    mock_replicator.replicate.return_value = [
        ReplicationResult("host1:8001", "ok", 5.0),
        ReplicationResult("host2:8002", "ok", 6.0),
        ReplicationResult("host3:8003", "ok", 7.0),
    ]

    # Ensure store is empty
    assert leader_module.store.get("testkey") is None

    response = client.post(
        "/set",
        json={"key": "testkey", "value": "testvalue"},
        content_type="application/json",
    )

    assert response.status_code == 200

    # Verify replicate was called
    mock_replicator.replicate.assert_called_once_with("testkey", "testvalue")

    # Verify local write happened
    assert leader_module.store.get("testkey") == "testvalue"


@patch("app.leader.app.replicator")
def test_set_returns_replication_details(mock_replicator, client):
    """Test that POST /set returns detailed replication information."""
    mock_replicator.replicate.return_value = [
        ReplicationResult("host1:8001", "ok", 5.123),
        ReplicationResult("host2:8002", "error", 6.456, "Connection timeout"),
        ReplicationResult("host3:8003", "ok", 7.789),
    ]

    response = client.post(
        "/set",
        json={"key": "testkey", "value": "testvalue"},
        content_type="application/json",
    )

    assert response.status_code == 200
    data = response.get_json()

    # Check replication details
    assert len(data["replication"]) == 3

    # Check first follower (success)
    rep1 = data["replication"][0]
    assert rep1["follower"] == "host1:8001"
    assert rep1["status"] == "ok"
    assert rep1["latency_ms"] == 5.123

    # Check second follower (error)
    rep2 = data["replication"][1]
    assert rep2["status"] == "error"
    assert "error" in rep2


@patch("app.leader.app.replicator")
def test_set_measures_total_latency(mock_replicator, client):
    """Test that POST /set measures total latency correctly."""
    mock_replicator.replicate.return_value = [
        ReplicationResult("host1:8001", "ok", 5.0),
        ReplicationResult("host2:8002", "ok", 6.0),
        ReplicationResult("host3:8003", "ok", 7.0),
    ]

    response = client.post(
        "/set",
        json={"key": "testkey", "value": "testvalue"},
        content_type="application/json",
    )

    assert response.status_code == 200
    data = response.get_json()

    # Latency should be present and positive
    assert "latency_ms" in data
    assert data["latency_ms"] > 0
