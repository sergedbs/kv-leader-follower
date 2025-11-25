import pytest
from app.follower.app import app, store


class TestFollowerApp:
    @pytest.fixture(autouse=True)
    def setup_env(self, monkeypatch):
        """Set up follower environment variables."""
        monkeypatch.setenv("ROLE", "follower")
        monkeypatch.setenv("PORT", "8001")

    @pytest.fixture
    def client(self):
        """Create a test client for the follower app."""
        app.config["TESTING"] = True
        with app.test_client() as client:
            store.clear()
            yield client

    def test_health_endpoint(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.get_json() == {"status": "healthy", "role": "follower"}

    def test_get_validation(self, client):
        # Missing key
        assert client.get("/get").status_code == 400
        # Non-existent key
        assert client.get("/get?key=missing").status_code == 404

    def test_get_existing_key(self, client):
        store.set("k", "v")
        response = client.get("/get?key=k")
        assert response.status_code == 200
        assert response.get_json()["value"] == "v"

    def test_dump_endpoint(self, client):
        store.set("k1", "v1")
        store.set("k2", "v2")
        response = client.get("/dump")
        assert response.status_code == 200
        assert response.get_json()["store"] == {"k1": "v1", "k2": "v2"}

    def test_replicate_success(self, client):
        response = client.post("/replicate", json={"key": "k", "value": "v"})
        assert response.status_code == 200
        assert response.get_json()["status"] == "ok"
        assert store.get("k") == "v"

    def test_replicate_validation(self, client):
        # Not JSON
        assert client.post("/replicate", data="raw").status_code == 400
        # Missing fields
        assert client.post("/replicate", json={"value": "v"}).status_code == 400
        assert client.post("/replicate", json={"key": "k"}).status_code == 400

    def test_replicate_updates_store(self, client):
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

    def test_replicate_overwrites_existing_key(self, client):
        """Test POST /replicate overwrites existing keys."""
        store.set("existingkey", "oldvalue")

        response = client.post(
            "/replicate",
            json={"key": "existingkey", "value": "newvalue"},
            content_type="application/json",
        )
        assert response.status_code == 200
        assert store.get("existingkey") == "newvalue"
