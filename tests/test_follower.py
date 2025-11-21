import pytest
import os
from app.follower.app import app, store


@pytest.fixture
def client():
    """Create a test client for the follower app."""
    app.config["TESTING"] = True
    with app.test_client() as client:
        # Clear store before each test
        store.clear()
        yield client


@pytest.fixture(autouse=True)
def set_follower_env():
    """Set up follower environment variables."""
    original_env = os.environ.copy()
    os.environ["ROLE"] = "follower"
    os.environ["PORT"] = "8001"
    yield
    os.environ.clear()
    os.environ.update(original_env)


def test_health_endpoint(client):
    """Test GET /health returns healthy status."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.get_json()
    assert data["status"] == "healthy"
    assert data["role"] == "follower"


def test_get_missing_key_parameter(client):
    """Test GET /get without key parameter returns 400."""
    response = client.get("/get")
    assert response.status_code == 400
    data = response.get_json()
    assert data["status"] == "error"
    assert "Missing key parameter" in data["error"]


def test_get_non_existent_key(client):
    """Test GET /get with non-existent key returns 404."""
    response = client.get("/get?key=nonexistent")
    assert response.status_code == 404
    data = response.get_json()
    assert data["status"] == "error"
    assert "Key not found" in data["error"]


def test_get_existing_key(client):
    """Test GET /get with existing key returns value."""
    store.set("testkey", "testvalue")
    response = client.get("/get?key=testkey")
    assert response.status_code == 200
    data = response.get_json()
    assert data["status"] == "ok"
    assert data["value"] == "testvalue"


def test_dump_endpoint(client):
    """Test GET /dump returns all keys."""
    store.set("key1", "value1")
    store.set("key2", "value2")

    response = client.get("/dump")
    assert response.status_code == 200
    data = response.get_json()
    assert data["status"] == "ok"
    assert data["store"] == {"key1": "value1", "key2": "value2"}


def test_replicate_with_valid_data(client):
    """Test POST /replicate with valid data succeeds."""
    response = client.post(
        "/replicate",
        json={"key": "mykey", "value": "myvalue"},
        content_type="application/json",
    )
    assert response.status_code == 200
    data = response.get_json()
    assert data["status"] == "ok"

    # Verify the key was stored
    assert store.get("mykey") == "myvalue"


def test_replicate_without_json(client):
    """Test POST /replicate without JSON returns 400."""
    response = client.post("/replicate", data="not json")
    assert response.status_code == 400
    data = response.get_json()
    assert data["status"] == "error"
    assert "Content-Type must be application/json" in data["error"]


def test_replicate_missing_key_field(client):
    """Test POST /replicate with missing key field returns 400."""
    response = client.post(
        "/replicate", json={"value": "myvalue"}, content_type="application/json"
    )
    assert response.status_code == 400
    data = response.get_json()
    assert data["status"] == "error"
    assert "Missing key or value" in data["error"]


def test_replicate_missing_value_field(client):
    """Test POST /replicate with missing value field returns 400."""
    response = client.post(
        "/replicate", json={"key": "mykey"}, content_type="application/json"
    )
    assert response.status_code == 400
    data = response.get_json()
    assert data["status"] == "error"
    assert "Missing key or value" in data["error"]


def test_replicate_updates_store(client):
    """Test POST /replicate actually updates the store."""
    client.post(
        "/replicate",
        json={"key": "key1", "value": "value1"},
        content_type="application/json",
    )
    client.post(
        "/replicate",
        json={"key": "key2", "value": "value2"},
        content_type="application/json",
    )

    dump = store.dump_all()
    assert len(dump) == 2
    assert dump["key1"] == "value1"
    assert dump["key2"] == "value2"


def test_replicate_overwrites_existing_key(client):
    """Test POST /replicate overwrites existing keys."""
    store.set("existingkey", "oldvalue")

    response = client.post(
        "/replicate",
        json={"key": "existingkey", "value": "newvalue"},
        content_type="application/json",
    )
    assert response.status_code == 200
    assert store.get("existingkey") == "newvalue"
