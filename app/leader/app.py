import time
from typing import Optional
from flask import Flask, request, jsonify
from app.common.store import KeyValueStore
from app.common.config import Config
from app.common.logging_setup import setup_logging
from app.leader.replication import Replicator

app = Flask(__name__)
store = KeyValueStore()
config = Config.from_env()
logger = setup_logging(config.log_level, f"leader-{config.port}")

# Initialize replicator only if we have followers
replicator: Optional[Replicator] = None
if config.followers:
    replicator = Replicator(
        followers=config.followers,
        min_delay=config.min_delay,
        max_delay=config.max_delay,
        repl_secret=config.repl_secret,
    )


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "role": "leader"}), 200


@app.route("/get", methods=["GET"])
def get():
    key = request.args.get("key")
    if not key:
        return jsonify({"status": "error", "error": "Missing key parameter"}), 400

    value = store.get(key)
    if value is None:
        return jsonify({"status": "error", "error": "Key not found"}), 404

    return jsonify({"status": "ok", "value": value}), 200


@app.route("/dump", methods=["GET"])
def dump():
    return jsonify({"status": "ok", "store": store.dump_all()}), 200


@app.route("/set", methods=["POST"])
def set_key():
    start_time = time.time()

    # Validate request
    if not request.is_json:
        return jsonify(
            {"status": "error", "error": "Content-Type must be application/json"}
        ), 400

    data = request.get_json()
    if "key" not in data or "value" not in data:
        return jsonify({"status": "error", "error": "Missing key or value"}), 400

    key = data["key"]
    value = data["value"]

    # Step 1: Write locally
    store.set(key, value)
    logger.info(f"Local write: {key}={value}")

    # Step 2: Replicate to followers
    if replicator:
        replication_results = replicator.replicate(key, value)
    else:
        replication_results = []

    # Step 3: Count acknowledgements
    acks = sum(1 for r in replication_results if r.status == "ok")

    # Step 4: Check quorum
    total_latency_ms = (time.time() - start_time) * 1000

    response = {
        "acks": acks,
        "required": config.write_quorum,
        "latency_ms": round(total_latency_ms, 3),
        "replication": [r.to_dict() for r in replication_results],
    }

    if acks >= config.write_quorum:
        logger.info(
            f"Write succeeded: {key}={value} (acks={acks}/{config.write_quorum})"
        )
        response["status"] = "ok"
        return jsonify(response), 200
    else:
        logger.warning(
            f"Write failed: {key}={value} (acks={acks}/{config.write_quorum})"
        )
        response["status"] = "error"
        response["error"] = "Quorum not reached"
        return jsonify(response), 500


if __name__ == "__main__":
    logger.info(f"Starting leader on port {config.port}")
    logger.info(f"Followers: {config.followers}")
    logger.info(f"Write quorum: {config.write_quorum}")
    app.run(host="0.0.0.0", port=config.port, threaded=True)
