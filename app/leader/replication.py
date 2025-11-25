from dataclasses import dataclass
import time
import random
import requests
from requests.adapters import HTTPAdapter
from typing import List, Dict, Callable, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from app.common.logging_setup import setup_logging


@dataclass
class ReplicationResult:
    """Result of replication to one follower."""

    follower: str
    status: str
    latency_ms: float
    error: Optional[str] = None

    def to_dict(self) -> Dict:
        result = {
            "follower": self.follower,
            "status": self.status,
            "latency_ms": round(self.latency_ms, 3),
        }
        if self.error:
            result["error"] = self.error
        return result


class Replicator:
    """Handles concurrent replication to followers."""

    def __init__(
        self,
        followers: List[str],
        min_delay: float,
        max_delay: float,
        repl_secret: Optional[str] = None,
        timeout: float = 5.0,
        delay_func: Optional[Callable[[], float]] = None,  # For testing
        log_level: str = "INFO",
    ):
        self.followers = followers
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.repl_secret = repl_secret
        self.timeout = timeout
        self.delay_func = delay_func or self._default_delay
        self.logger = setup_logging(log_level, "replicator")

        pool_size = len(followers) * 20

        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.executor = ThreadPoolExecutor(max_workers=pool_size)

    def _default_delay(self) -> float:
        return random.uniform(self.min_delay, self.max_delay)

    def _replicate_to_one(
        self, follower: str, key: str, value: str, version: int
    ) -> ReplicationResult:
        """Replicate to a single follower."""
        start = time.time()

        try:
            # Simulate network delay
            delay = self.delay_func()
            time.sleep(delay)

            # Send replication request
            url = f"http://{follower}/replicate"
            headers = {"Content-Type": "application/json"}
            if self.repl_secret:
                headers["X-Replication-Secret"] = self.repl_secret

            response = self.session.post(
                url,
                json={"key": key, "value": value, "version": version},
                headers=headers,
                timeout=self.timeout,
            )

            elapsed_ms = (time.time() - start) * 1000

            if response.status_code == 200:
                return ReplicationResult(follower, "ok", elapsed_ms)
            else:
                return ReplicationResult(
                    follower,
                    "error",
                    elapsed_ms,
                    f"HTTP {response.status_code}: {response.text}",
                )

        except Exception as e:
            elapsed_ms = (time.time() - start) * 1000
            return ReplicationResult(follower, "error", elapsed_ms, str(e))

    def replicate(
        self, key: str, value: str, version: int, quorum: int = 0
    ) -> List[ReplicationResult]:
        """
        Replicate key-value to all followers concurrently.

        Args:
            key: The key to replicate
            value: The value to replicate
            version: The version number of the write
            quorum: If > 0, return as soon as this many successful replications occur.
                   If 0, wait for all followers to respond.
        """
        futures = {
            self.executor.submit(
                self._replicate_to_one, follower, key, value, version
            ): follower
            for follower in self.followers
        }

        results = []
        acks = 0

        for future in as_completed(futures):
            result = future.result()
            results.append(result)

            if result.status == "ok":
                acks += 1

            self.logger.debug(
                f"Replication to {result.follower}: {result.status} ({result.latency_ms:.2f}ms)"
            )

            if quorum > 0 and acks >= quorum:
                self.logger.debug(f"Quorum of {quorum} reached, returning early")
                break

        return results

    def close(self):
        """Clean up resources."""
        self.session.close()
        self.executor.shutdown(wait=True)
