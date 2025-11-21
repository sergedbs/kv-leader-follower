import threading
import time
from app.common.store import KeyValueStore


def test_store_initially_empty():
    """Test that the store is empty on creation."""
    store = KeyValueStore()
    assert store.dump_all() == {}


def test_set_and_get_single_key():
    """Test setting and getting a single key."""
    store = KeyValueStore()
    store.set("key1", "value1")
    assert store.get("key1") == "value1"


def test_get_non_existent_key():
    """Test that getting a non-existent key returns None."""
    store = KeyValueStore()
    assert store.get("non_existent_key") is None


def test_overwrite_existing_key():
    """Test that setting an existing key overwrites its value."""
    store = KeyValueStore()
    store.set("key1", "value1")
    store.set("key1", "new_value")
    assert store.get("key1") == "new_value"


def test_dump_returns_copy():
    """Test that dump_all returns a copy, not a reference."""
    store = KeyValueStore()
    store.set("key1", "value1")
    dumped_store = store.dump_all()
    dumped_store["key2"] = "value2"
    assert "key2" not in store.dump_all()


def test_clear_store():
    """Test that clear removes all entries."""
    store = KeyValueStore()
    store.set("key1", "value1")
    store.set("key2", "value2")
    store.clear()
    assert store.dump_all() == {}


def test_concurrent_writes():
    """Test concurrent writes from multiple threads."""
    store = KeyValueStore()
    num_threads = 10
    ops_per_thread = 100
    threads = []

    def writer(thread_id):
        for i in range(ops_per_thread):
            key = f"key_{thread_id}_{i}"
            value = f"value_{thread_id}_{i}"
            store.set(key, value)

    for i in range(num_threads):
        thread = threading.Thread(target=writer, args=(i,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    assert len(store.dump_all()) == num_threads * ops_per_thread


def test_concurrent_reads_and_writes():
    """Test concurrent reads while writes are happening."""
    store = KeyValueStore()
    stop_event = threading.Event()

    # Writer thread
    def writer():
        i = 0
        while not stop_event.is_set():
            store.set(f"key{i}", f"value{i}")
            i += 1

    # Reader thread
    def reader():
        read_count = 0
        while not stop_event.is_set():
            value = store.get("key0")
            if value is not None:
                assert value == "value0"
            read_count += 1
        return read_count

    store.set("key0", "value0")
    writer_thread = threading.Thread(target=writer)
    reader_thread = threading.Thread(target=reader)

    writer_thread.start()
    reader_thread.start()

    time.sleep(0.1)
    stop_event.set()

    writer_thread.join()
    reader_thread.join()
