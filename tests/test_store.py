import threading
import pytest
from app.common.store import KeyValueStore


class TestKeyValueStore:
    @pytest.fixture
    def store(self):
        return KeyValueStore()

    def test_initial_state(self, store):
        assert store.dump_all() == {}
        assert store.get("missing") is None

    def test_basic_operations(self, store):
        store.set("k", "v")
        assert store.get("k") == "v"

        # Overwrite
        store.set("k", "v2")
        assert store.get("k") == "v2"

    def test_dump_returns_copy(self, store):
        store.set("k", "v")
        dump = store.dump_all()
        dump["k"] = "modified"
        assert store.get("k") == "v"

    def test_clear(self, store):
        store.set("k", "v")
        store.clear()
        assert store.dump_all() == {}

    def test_concurrent_writes(self, store):
        """Test concurrent writes from multiple threads."""
        num_threads = 10
        ops_per_thread = 100

        def writer(tid):
            for i in range(ops_per_thread):
                store.set(f"k_{tid}_{i}", "v")

        threads = [
            threading.Thread(target=writer, args=(i,)) for i in range(num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(store.dump_all()) == num_threads * ops_per_thread

    def test_concurrent_reads_writes(self, store):
        """Test concurrent reads while writes are happening."""
        stop = threading.Event()
        store.set("k0", "v0")

        def writer():
            i = 0
            while not stop.is_set():
                store.set(f"k{i}", f"v{i}")
                i += 1

        def reader():
            count = 0
            while not stop.is_set():
                if store.get("k0") == "v0":
                    count += 1
            return count

        w = threading.Thread(target=writer)
        r = threading.Thread(target=reader)

        w.start()
        r.start()

        # Let them run briefly
        import time

        time.sleep(0.1)
        stop.set()

        w.join()
        r.join()

        # Just ensure no crashes/deadlocks occurred
        assert store.get("k0") == "v0"
