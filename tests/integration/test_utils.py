"""
Utility functions for integration testing.
"""

import subprocess
import time
import requests
from typing import List, Dict, Optional, Tuple


class DockerComposeManager:
    """Manage docker-compose lifecycle for integration tests."""

    @staticmethod
    def up(env: Optional[Dict[str, str]] = None, wait_time: int = 5) -> None:
        """
        Start services with docker-compose.

        Args:
            env: Environment variables to pass to docker-compose
            wait_time: Seconds to wait for services to be ready
        """
        import os

        # Merge with current environment
        full_env = os.environ.copy()
        if env:
            full_env.update(env)

        cmd = ["docker-compose", "up", "-d"]
        result = subprocess.run(cmd, env=full_env, capture_output=True, text=True)

        if result.returncode != 0:
            raise RuntimeError(f"docker-compose up failed: {result.stderr}")

        # Wait for services to be ready
        time.sleep(wait_time)

    @staticmethod
    def down() -> None:
        """Stop and remove all services."""
        cmd = ["docker-compose", "down"]
        subprocess.run(cmd, capture_output=True, text=True, check=False)

    @staticmethod
    def stop_service(service: str) -> None:
        """Stop a specific service."""
        cmd = ["docker-compose", "stop", service]
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            raise RuntimeError(f"Failed to stop {service}: {result.stderr}")

    @staticmethod
    def start_service(service: str) -> None:
        """Start a stopped service."""
        cmd = ["docker-compose", "start", service]
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            raise RuntimeError(f"Failed to start {service}: {result.stderr}")

    @staticmethod
    def exec_command(service: str, command: List[str]) -> str:
        """
        Execute command in a service container.

        Args:
            service: Service name
            command: Command to execute as list of strings

        Returns:
            Command output as string
        """
        cmd = ["docker-compose", "exec", "-T", service] + command
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            raise RuntimeError(f"Command failed: {result.stderr}")

        return result.stdout

    @staticmethod
    def logs(service: str, tail: int = 50) -> str:
        """Get logs from a service."""
        cmd = ["docker-compose", "logs", "--tail", str(tail), service]
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.stdout


def wait_for_health(url: str, timeout: int = 30, check_interval: float = 0.5) -> bool:
    """
    Wait for a service to be healthy.

    Args:
        url: Base URL of the service
        timeout: Maximum seconds to wait
        check_interval: Seconds between health checks

    Returns:
        True if service became healthy, False if timeout
    """
    start = time.time()

    while time.time() - start < timeout:
        try:
            response = requests.get(f"{url}/health", timeout=2)
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "healthy":
                    return True
        except requests.exceptions.RequestException:
            pass

        time.sleep(check_interval)

    return False


def get_store_dump(url: str) -> Optional[Dict[str, str]]:
    """
    Get full store dump from a service.

    Args:
        url: Base URL of the service

    Returns:
        Dictionary of key-value pairs, or None if request failed
    """
    try:
        response = requests.get(f"{url}/dump", timeout=5)
        if response.status_code == 200:
            return response.json().get("store", {})
    except requests.exceptions.RequestException:
        pass

    return None


def get_follower_dump(follower_num: int) -> Optional[Dict[str, str]]:
    """
    Get store dump from a follower by executing curl inside the container.

    Args:
        follower_num: Follower number (1-5)

    Returns:
        Dictionary of key-value pairs, or None if failed
    """
    try:
        import json

        output = DockerComposeManager.exec_command(
            f"follower{follower_num}",
            [
                "python",
                "-c",
                f"import requests; print(requests.get('http://localhost:800{follower_num}/dump').text)",
            ],
        )
        data = json.loads(output.strip())
        return data.get("store", {})
    except Exception:
        return None


def compare_stores(
    leader_store: Dict[str, str], follower_stores: List[Tuple[str, Dict[str, str]]]
) -> Dict:
    """
    Compare leader store with follower stores.

    Args:
        leader_store: Leader's key-value store
        follower_stores: List of (follower_name, store) tuples

    Returns:
        Report dictionary with comparison results
    """
    report = {"total_keys": len(leader_store), "followers": []}

    for name, store in follower_stores:
        matching = sum(1 for k in leader_store if store.get(k) == leader_store[k])
        missing = [k for k in leader_store if k not in store]
        mismatched = [
            k for k in leader_store if k in store and store[k] != leader_store[k]
        ]

        report["followers"].append(
            {
                "name": name,
                "matching_keys": matching,
                "missing_keys": missing,
                "mismatched_keys": mismatched,
                "consistency": matching == len(leader_store),
            }
        )

    return report


def write_key(
    leader_url: str, key: str, value: str, timeout: int = 10
) -> Tuple[bool, Dict]:
    """
    Write a key-value pair to the leader.

    Args:
        leader_url: Base URL of the leader
        key: Key to write
        value: Value to write
        timeout: Request timeout in seconds

    Returns:
        Tuple of (success, response_data)
    """
    try:
        response = requests.post(
            f"{leader_url}/set", json={"key": key, "value": value}, timeout=timeout
        )

        data = response.json()
        success = response.status_code == 200 and data.get("status") == "ok"
        return success, data
    except requests.exceptions.RequestException as e:
        return False, {"error": str(e)}


def read_key(url: str, key: str) -> Tuple[bool, Optional[str]]:
    """
    Read a key from a service.

    Args:
        url: Base URL of the service
        key: Key to read

    Returns:
        Tuple of (found, value)
    """
    try:
        response = requests.get(f"{url}/get?key={key}", timeout=5)
        if response.status_code == 200:
            data = response.json()
            return True, data.get("value")
        return False, None
    except requests.exceptions.RequestException:
        return False, None


def update_leader_config(leader_url: str, config: Dict) -> bool:
    """
    Update leader configuration dynamically.

    Args:
        leader_url: Base URL of the leader
        config: Dictionary of configuration updates

    Returns:
        True if update succeeded
    """
    try:
        response = requests.post(f"{leader_url}/config", json=config, timeout=5)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False
