import pytest
import os
from app.common.config import Config


@pytest.fixture(autouse=True)
def clear_env_vars():
    """Clear relevant environment variables before each test."""
    original_env = os.environ.copy()
    for key in [
        "ROLE",
        "PORT",
        "FOLLOWERS",
        "WRITE_QUORUM",
        "MIN_DELAY",
        "MAX_DELAY",
        "REPL_SECRET",
        "LOG_LEVEL",
    ]:
        if key in os.environ:
            del os.environ[key]
    yield
    os.environ.clear()
    os.environ.update(original_env)


def test_load_valid_leader_config():
    """Test loading a valid leader configuration."""
    os.environ["ROLE"] = "leader"
    os.environ["FOLLOWERS"] = "host1:8001,host2:8002"
    os.environ["WRITE_QUORUM"] = "2"

    config = Config.from_env()

    assert config.role == "leader"
    assert config.port == 8000  # Default
    assert config.followers == ["host1:8001", "host2:8002"]
    assert config.write_quorum == 2


def test_load_valid_follower_config():
    """Test loading a valid follower configuration."""
    os.environ["ROLE"] = "follower"
    os.environ["PORT"] = "9000"

    config = Config.from_env()

    assert config.role == "follower"
    assert config.port == 9000
    assert config.followers == []
    assert config.write_quorum == 1  # Default


def test_invalid_role_raises_error():
    """Test that an invalid role raises a ValueError."""
    os.environ["ROLE"] = "invalid_role"
    with pytest.raises(ValueError, match="Invalid ROLE"):
        Config.from_env()


def test_leader_without_followers_raises_error():
    """Test that a leader without followers raises a ValueError."""
    os.environ["ROLE"] = "leader"
    with pytest.raises(ValueError, match="Leader must have FOLLOWERS configured"):
        Config.from_env()


def test_invalid_quorum_raises_error():
    """Test that an invalid write quorum raises a ValueError."""
    os.environ["ROLE"] = "leader"
    os.environ["FOLLOWERS"] = "host1:8001"

    os.environ["WRITE_QUORUM"] = "0"
    with pytest.raises(ValueError, match="WRITE_QUORUM must be >= 1"):
        Config.from_env()

    os.environ["WRITE_QUORUM"] = "2"
    with pytest.raises(ValueError, match="WRITE_QUORUM .* > followers"):
        Config.from_env()


def test_invalid_delay_range_raises_error():
    """Test that an invalid delay range raises a ValueError."""
    os.environ["ROLE"] = "leader"
    os.environ["FOLLOWERS"] = "host1:8001"
    os.environ["MIN_DELAY"] = "0.1"
    os.environ["MAX_DELAY"] = "0.05"
    with pytest.raises(ValueError, match="Invalid delay range"):
        Config.from_env()


def test_default_values():
    """Test that default values are used when environment variables are not set."""
    config = Config.from_env()
    assert config.role == "follower"
    assert config.port == 8000
    assert config.log_level == "INFO"
    assert config.min_delay == 0.0001
    assert config.max_delay == 0.001
