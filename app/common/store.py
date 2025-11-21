import threading
from typing import Optional, Dict

class KeyValueStore:
    """Thread-safe in-memory key-value store."""
    
    def __init__(self):
        self._store: Dict[str, str] = {}
        self._lock = threading.RLock()
    
    def get(self, key: str) -> Optional[str]:
        """Get value for key. Returns None if not found."""
        with self._lock:
            return self._store.get(key)
    
    def set(self, key: str, value: str) -> None:
        """Set key to value."""
        with self._lock:
            self._store[key] = value
    
    def dump_all(self) -> Dict[str, str]:
        """Return copy of entire store."""
        with self._lock:
            return self._store.copy()
    
    def clear(self) -> None:
        """Clear all entries (for testing)."""
        with self._lock:
            self._store.clear()
