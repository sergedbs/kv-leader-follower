import pytest
import os
from unittest.mock import patch
import app.leader.app as leader_module
from app.leader.replication import ReplicationResult
from app.common.config import Config


@pytest.fixture(scope="module")
def leader_env():
    """Set up leader environment variables once for the module."""
    original_env = os.environ.copy()
    os.environ.update(
        {
            "ROLE": "leader",
            "PORT": "8000",
            "FOLLOWERS": "host1:8001,host2:8002,host3:8003",
            "WRITE_QUORUM": "2",
        }
    )

    # Reload config
    leader_module.config = Config.from_env()

    yield

    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def mock_replicator():
    """Mock the replicator for all tests."""
    with patch("app.leader.app.replicator") as mock:
        yield mock


@pytest.fixture
def client(leader_env):
    """Create a test client for the leader app."""
    leader_module.app.config["TESTING"] = True
    with leader_module.app.test_client() as client:
        leader_module.store.clear()
        yield client


class TestLeaderApp:
    def test_health_endpoint(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.get_json() == {"status": "healthy", "role": "leader"}

    def test_get_endpoint(self, client):
        leader_module.store.set("testkey", "testvalue")
        response = client.get("/get?key=testkey")
        assert response.status_code == 200
        assert response.get_json()["value"] == "testvalue"

    def test_dump_endpoint(self, client):
        leader_module.store.set("key1", "value1")
        leader_module.store.set("key2", "value2")
        response = client.get("/dump")
        assert response.status_code == 200
        assert response.get_json()["store"] == {"key1": "value1", "key2": "value2"}

    def test_set_validation(self, client):
        # Not JSON
        assert client.post("/set", data="raw").status_code == 400
        # Missing key
        assert client.post("/set", json={"value": "v"}).status_code == 400
        # Missing value
        assert client.post("/set", json={"key": "k"}).status_code == 400

    def test_set_success_with_quorum(self, client, mock_replicator):
        mock_replicator.replicate.return_value = [
            ReplicationResult("h1", "ok", 5.0),
            ReplicationResult("h2", "ok", 6.0),
            ReplicationResult("h3", "ok", 7.0),
        ]

        response = client.post("/set", json={"key": "k", "value": "v"})

        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "ok"
        assert data["acks"] == 3
        assert leader_module.store.get("k") == "v"

    def test_set_failure_quorum_not_reached(self, client, mock_replicator):
        mock_replicator.replicate.return_value = [
            ReplicationResult("h1", "ok", 5.0),
            ReplicationResult("h2", "error", 6.0),
            ReplicationResult("h3", "error", 7.0),
        ]

        response = client.post("/set", json={"key": "k", "value": "v"})

        assert response.status_code == 500
        assert response.get_json()["status"] == "error"
        # Local write still happens
        assert leader_module.store.get("k") == "v"

    def test_set_writes_locally_before_replication(self, client, mock_replicator):
        mock_replicator.replicate.return_value = []

        client.post("/set", json={"key": "k", "value": "v"})

        # Fix: assert called with quorum argument and version
        # Version should be 1 for the first write
        mock_replicator.replicate.assert_called_once_with("k", "v", 1, quorum=2)
        assert leader_module.store.get("k") == "v"

    def test_set_returns_replication_details(self, client, mock_replicator):
        mock_replicator.replicate.return_value = [
            ReplicationResult("h1", "ok", 1.0),
            ReplicationResult("h2", "error", 2.0, "err"),
        ]

        data = client.post("/set", json={"key": "k", "value": "v"}).get_json()

        assert len(data["replication"]) == 2
        assert data["replication"][0]["status"] == "ok"
        assert data["replication"][1]["error"] == "err"
