import threading
from typing import Optional, Dict


class KeyValueStore:
    """Thread-safe in-memory key-value store."""

    def __init__(self):
        # Stores key -> (value, version)
        self._store: Dict[str, tuple] = {}
        self._lock = threading.RLock()

    def get(self, key: str) -> Optional[str]:
        """Get value for key. Returns None if not found."""
        with self._lock:
            item = self._store.get(key)
            return item[0] if item else None

    def set(self, key: str, value: str, version: Optional[int] = None) -> int:
        """
        Set key to value.

        Args:
            key: The key to set
            value: The value to set
            version: Explicit version number (for followers).
                    If None (leader), increments current version.

        Returns:
            The version number stored.
        """
        with self._lock:
            current = self._store.get(key)
            current_ver = current[1] if current else 0

            if version is None:
                # Leader mode: Auto-increment version
                new_ver = current_ver + 1
                self._store[key] = (value, new_ver)
                return new_ver
            else:
                # Follower mode: Only update if new version is greater
                if version > current_ver:
                    self._store[key] = (value, version)
                    return version
                return current_ver

    def dump_all(self) -> Dict[str, str]:
        """Return copy of entire store (values only)."""
        with self._lock:
            return {k: v[0] for k, v in self._store.items()}

    def clear(self) -> None:
        """Clear all entries (for testing)."""
        with self._lock:
            self._store.clear()
