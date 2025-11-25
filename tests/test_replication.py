import pytest
from unittest.mock import patch
from app.leader.replication import Replicator, ReplicationResult


class TestReplication:
    @pytest.fixture
    def replicator(self):
        rep = Replicator(
            followers=["host1:8001", "host2:8002"],
            min_delay=0.0,
            max_delay=0.0,
            delay_func=lambda: 0.0,
        )
        yield rep
        rep.close()

    def test_result_to_dict(self):
        res = ReplicationResult("f1", "ok", 1.0)
        assert res.to_dict() == {"follower": "f1", "status": "ok", "latency_ms": 1.0}

        res_err = ReplicationResult("f2", "error", 2.0, "fail")
        assert res_err.to_dict()["error"] == "fail"

    def test_initialization(self, replicator):
        assert replicator.followers == ["host1:8001", "host2:8002"]
        assert replicator.min_delay == 0.0

    def test_default_delay(self):
        rep = Replicator(["h1"], 0.1, 0.2)
        delay = rep._default_delay()
        assert 0.1 <= delay <= 0.2
        rep.close()

    def test_replicate_to_one_success(self, replicator):
        with patch.object(replicator.session, "post") as mock_post:
            mock_post.return_value.status_code = 200

            res = replicator._replicate_to_one("host1:8001", "k", "v", 1)

            assert res.status == "ok"
            assert res.error is None
            mock_post.assert_called_once()

    def test_replicate_to_one_http_error(self, replicator):
        with patch.object(replicator.session, "post") as mock_post:
            mock_post.return_value.status_code = 500
            mock_post.return_value.text = "Error"

            res = replicator._replicate_to_one("host1:8001", "k", "v", 1)

            assert res.status == "error"
            assert "500" in res.error

    def test_replicate_to_one_connection_error(self, replicator):
        with patch.object(
            replicator.session, "post", side_effect=Exception("ConnRefused")
        ):
            res = replicator._replicate_to_one("host1:8001", "k", "v", 1)
            assert res.status == "error"
            assert "ConnRefused" in res.error

    def test_replicate_all_concurrent(self, replicator):
        with patch.object(replicator.session, "post") as mock_post:
            mock_post.return_value.status_code = 200

            results = replicator.replicate("k", "v", 1)

            assert len(results) == 2
            assert all(r.status == "ok" for r in results)
            assert mock_post.call_count == 2

    def test_replicate_with_secret(self):
        rep = Replicator(["h1"], 0, 0, repl_secret="secret", delay_func=lambda: 0)
        with patch.object(rep.session, "post") as mock_post:
            mock_post.return_value.status_code = 200
            rep._replicate_to_one("h1", "k", "v", 1)

            headers = mock_post.call_args[1]["headers"]
            assert headers["X-Replication-Secret"] == "secret"
        rep.close()
