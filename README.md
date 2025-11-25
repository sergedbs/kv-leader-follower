# Key-Value Store With Single-Leader Replication

A distributed key-value store implementation with single-leader replication and configurable write quorum. Built with Python and Flask, this project demonstrates semi-synchronous replication, concurrent request handling, and consistency guarantees in distributed systems.

[![Python](https://img.shields.io/badge/Python-3.13+-blue.svg)](https://www.python.org/)
[![Flask](https://img.shields.io/badge/Flask-3.0-green.svg)](https://flask.palletsprojects.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Testing](#testing)
- [Benchmarking](#benchmarking)
- [API Reference](#api-reference)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## Features

- **Single-Leader Replication**: Write operations go through the leader, which replicates to followers
- **Configurable Write Quorum**: Set minimum number of successful replications (1-5)
- **Semi-Synchronous Replication**: Leader writes locally first, then replicates in parallel
- **Data Versioning**: Lamport timestamps ensure consistency despite message reordering
- **Thread-Safe Operations**: Concurrent request handling with proper locking
- **Network Delay Simulation**: Configurable network delays (~0.1-1000ms) for realistic testing
- **Consistency Verification**: Tools to verify data consistency across all replicas
- **Performance Benchmarking**: Comprehensive benchmarking suite with visualization
- **Docker Orchestration**: Easy deployment with Docker Compose
- **HTTP REST API**: Simple JSON-based API for all operations
- **Comprehensive Testing**: 55 tests covering unit, integration, and performance scenarios

## Architecture

Single-leader replication with configurable quorum-based consistency.

**Write Flow**: Client → Leader (local write + version increment + spawn parallel replication) → Wait for quorum acks → Respond  
**Read Flow**: Any node returns local value

**Consistency Levels**:

| Quorum | Survivable Failures | Strength |
|--------|---------------------|----------|
| 1      | 0                   | Weak     |
| 3      | 2                   | Strong (majority) |
| 5      | 4                   | Strongest (all) |

**Components**: Thread-safe `KeyValueStore` (RLock), `Replicator` (ThreadPoolExecutor), Flask HTTP API, environment-based config

## Project Structure

```txt
kv-leader-follower/
├── app/
│   ├── common/          # Shared: store, config, logging
│   ├── leader/          # Leader API and replication
│   └── follower/        # Follower API
├── tests/               # 55 tests (unit, integration, performance)
├── scripts/
│   ├── run_benchmark.py
│   ├── run_all.py
│   ├── plot_results.py
│   └── check_consistency.py
├── results/             # Benchmark outputs
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

## Quick Start

### Docker Compose (Recommended)

```bash
docker-compose up -d        # Start leader + 5 followers
docker-compose logs -f      # View logs
docker-compose down         # Stop services
```

### Local Development

**Start leader**:

```bash
ROLE=leader \
FOLLOWERS=localhost:8001,localhost:8002,localhost:8003,localhost:8004,localhost:8005 \
WRITE_QUORUM=3 \
python -m app.leader.app
```

**Start followers** (in separate terminals):

```bash
ROLE=follower PORT=8001 python -m app.follower.app
ROLE=follower PORT=8002 python -m app.follower.app
ROLE=follower PORT=8003 python -m app.follower.app
ROLE=follower PORT=8004 python -m app.follower.app
ROLE=follower PORT=8005 python -m app.follower.app
```

**Test the system**:

```bash
# Write
curl -X POST http://localhost:8000/set \
  -H "Content-Type: application/json" \
  -d '{"key":"username","value":"alice"}'

# Read
curl http://localhost:8000/get?key=username

# Health check
curl http://localhost:8000/health
```

## Installation

### Prerequisites

- Python 3.13+ (or 3.11+)
- Docker & Docker Compose (for containerized deployment)

### Setup

```bash
git clone <repository-url>
cd kv-leader-follower
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pytest tests/ -v  # Verify installation
```

### Running Locally

**Stop Docker services** (if running):

```bash
docker-compose down
```

**Start leader**:

```bash
ROLE=leader \
FOLLOWERS=localhost:8001,localhost:8002,localhost:8003,localhost:8004,localhost:8005 \
WRITE_QUORUM=3 \
python -m app.leader.app
```

**Start follower** (repeat for each follower):

```bash
ROLE=follower PORT=8001 python -m app.follower.app
ROLE=follower PORT=8002 python -m app.follower.app
# ... etc
```

### Basic Operations

**Write a key-value pair**:

```bash
curl -X POST http://localhost:8000/set \
  -H "Content-Type: application/json" \
  -d '{"key":"username","value":"alice"}'
```

**Read a value**:

```bash
curl "http://localhost:8000/get?key=username"
```

**Check health**:

```bash
curl http://localhost:8000/health
```

## Configuration

Configuration is managed through environment variables defined in `app/common/config.py`.

### Environment Variables

| Variable | Leader | Follower | Description |
|----------|--------|----------|-------------|
| `ROLE` | `leader` | `follower` | Service role |
| `PORT` | `8000` | `8001` | HTTP port |
| `FOLLOWERS` | Required | n/a | Comma-separated addresses |
| `WRITE_QUORUM` | `3` | n/a | Min successful replications |
| `REPL_SECRET` | `secret` | `secret` | Replication auth secret |
| `MIN_DELAY` | `0.0001` | n/a | Min network delay (seconds) |
| `MAX_DELAY` | `1.0` | n/a | Max network delay (seconds) |
| `LOG_LEVEL` | `INFO` | `INFO` | Logging verbosity |

**Example**:

```bash
ROLE=leader \
FOLLOWERS=f1:8001,f2:8002,f3:8003,f4:8004,f5:8005 \
WRITE_QUORUM=4 \
MIN_DELAY=0.0001 \
MAX_DELAY=1.0 \
python -m app.leader.app
```

## Testing

55 tests covering unit, integration, performance, and consistency scenarios.

```bash
pytest tests/ -v                        # All tests
pytest tests/test_replication.py -v    # Specific module
pytest tests/integration/ -v           # Integration tests
pytest tests/ --cov=app --cov-report=html  # With coverage
```

## Benchmarking

### Run Benchmarks

```bash
python scripts/run_all.py               # Full sweep (10K writes per quorum 1-5)
python scripts/run_all.py --quick       # Quick mode (1K writes)
python scripts/run_benchmark.py --writes 10000 --threads 15 --quorum 3 --keys 100
```

### Outputs

Results in `results/`:

- CSV files with latency data per quorum
- `quorum_vs_latency.png` visualization

```bash
python scripts/plot_results.py  # Regenerate plot
```

### Consistency Check

```bash
python scripts/check_consistency.py  # Verify all replicas match leader
```

### Expected Performance

With ~0.1-1000ms network delay, the impact of quorum size becomes dramatic:

```txt
=================================================================
Quorum   | Avg (ms)     | Min (ms)   | Max (ms)   | Samples 
-----------------------------------------------------------------
1        | 234.17       | 20.80      | 710.23     | 200     
2        | 338.61       | 27.02      | 842.09     | 200     
3        | 522.16       | 102.21     | 896.37     | 200     
4        | 686.95       | 157.79     | 987.30     | 200     
5        | 849.67       | 124.21     | 1014.41    | 200     
=================================================================
```

*Note: With 5 followers and uniform random delay (0-1s), Q=1 waits for the fastest, while Q=5 waits for the slowest. The base overhead (Docker networking + HTTP) adds ~150ms.*

## API Reference

All endpoints return JSON responses.

### Leader Endpoints

#### Leader Health Check

```http
GET /health
```

**Response**:

```json
{
  "status": "ok",
  "role": "leader",
  "followers": 5,
  "write_quorum": 3
}
```

#### Write Key-Value Pair

```http
POST /set
Content-Type: application/json

{
  "key": "username",
  "value": "alice"
}
```

**Response** (200 OK):

```json
{
  "status": "ok",
  "acks": 5,
  "latency_ms": 45.2,
  "replication": [
    {"follower": "follower1:8001", "status": "ok", "latency_ms": 12.3},
    {"follower": "follower2:8002", "status": "ok", "latency_ms": 14.1},
    {"follower": "follower3:8003", "status": "ok", "latency_ms": 11.8},
    {"follower": "follower4:8004", "status": "ok", "latency_ms": 13.5},
    {"follower": "follower5:8005", "status": "ok", "latency_ms": 12.9}
  ]
}
```

**Response** (500 Internal Server Error - quorum not reached):

```json
{
  "status": "error",
  "error": "Replication failed: only 2/3 acknowledgments received"
}
```

#### Read Key-Value Pair

```http
GET /get?key=username
```

**Response** (200 OK):

```json
{
  "status": "ok",
  "value": "alice"
}
```

**Response** (404 Not Found):

```json
{
  "status": "error",
  "error": "Key not found"
}
```

#### Dump All Data

```http
GET /dump
```

**Response**:

```json
{
  "status": "ok",
  "store": {
    "username": "alice",
    "email": "alice@example.com"
  }
}
```

### Follower Endpoints

#### Follower Health Check

```http
GET /health
```

**Response**:

```json
{
  "status": "ok",
  "role": "follower"
}
```

#### Replicate (Internal - Called by Leader)

```http
POST /replicate
Content-Type: application/json
X-Replication-Secret: secret

{
  "key": "username",
  "value": "alice",
  "version": 1
}
```

**Response**:

```json
{
  "status": "ok"
}
```

#### Read and Dump

Followers support the same `/get` and `/dump` endpoints as the leader.

## Troubleshooting

**Port already in use**:

```bash
lsof -i :8000        # Find process
kill -9 <PID>        # Kill process
PORT=9000 docker-compose up  # Use different port
```

**Docker services not starting**:

```bash
docker-compose logs           # View logs
docker-compose restart        # Restart
docker-compose up -d --build  # Rebuild images
```

**Replication failures**: Check that followers are running, `REPL_SECRET` matches, and network connectivity exists.

**Consistency check failures**: Wait for replication to complete, verify write quorum ≤ number of followers, check logs for errors.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Resources

### Distributed Systems Concepts

- [Replication in Distributed Systems](https://en.wikipedia.org/wiki/Replication_(computing))
- [Quorum-Based Replication](https://en.wikipedia.org/wiki/Quorum_(distributed_computing))
- [CAP Theorem](https://en.wikipedia.org/wiki/CAP_theorem)
- [Consistency Models](https://jepsen.io/consistency)

### Concurrency & Thread Safety

- [MIT 6.005: Software Construction (2016)](https://ocw.mit.edu/courses/6-005-software-construction-spring-2016/) - Thread safety, locks & synchronization
- [MIT 6.102: Software Construction (2025)](https://web.mit.edu/6.102/) - Promises, mutual exclusion, message passing
- [The Art of Multiprocessor Programming](https://www.elsevier.com/books/the-art-of-multiprocessor-programming/herlihy/978-0-12-397337-5) - Mutual exclusion, locks, monitors

### Networking & Protocols

- [Python Threading Documentation](https://docs.python.org/3/library/threading.html)
- [ThreadPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor)
