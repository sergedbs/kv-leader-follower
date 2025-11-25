from flask import Flask, request, jsonify
from app.common.store import KeyValueStore
from app.common.config import Config
from app.common.logging_setup import setup_logging

app = Flask(__name__)
store = KeyValueStore()
config = Config.from_env()
logger = setup_logging(config.log_level, f"follower-{config.port}")


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "role": "follower"}), 200


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


@app.route("/replicate", methods=["POST"])
def replicate():
    # Validate request
    if not request.is_json:
        return jsonify(
            {"status": "error", "error": "Content-Type must be application/json"}
        ), 400

    try:
        data = request.get_json()
    except Exception as e:
        return jsonify({"status": "error", "error": f"Invalid JSON: {str(e)}"}), 400

    if "key" not in data or "value" not in data:
        return jsonify({"status": "error", "error": "Missing key or value"}), 400

    # Optional: check shared secret
    if config.repl_secret:
        auth = request.headers.get("X-Replication-Secret")
        if auth != config.repl_secret:
            return jsonify({"status": "error", "error": "Unauthorized"}), 401

    # Apply write
    key = data["key"]
    value = data["value"]
    version = data.get(
        "version"
    )  # Optional for backward compatibility, but recommended

    store.set(key, value, version=version)
    logger.info(f"Replicated: {key}={value} (v{version})")

    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    logger.info(f"Starting follower on port {config.port}")
    app.run(host="0.0.0.0", port=config.port, threaded=True)
