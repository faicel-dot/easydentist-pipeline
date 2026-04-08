"""
make_webhook.py — Serveur Flask exposant des webhooks pour Railway.
Endpoints:
  /trigger/enrich    — Lance l'enrichissement Est/Pas sur Doctolib (bulk)
  /trigger/orchestrator — Lance le pipeline complet (scrape + qualify)
  /status            — État des tâches en cours
  /health            — Health check Railway
"""

import os
import asyncio
import logging
import threading
from datetime import datetime
from flask import Flask, jsonify, request
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

# État global
tasks_status = {
    "enrich": {"running": False, "last_run": None, "last_result": None},
    "orchestrator": {"running": False, "last_run": None, "last_result": None},
}


def run_async_in_thread(coro_func, task_name, **kwargs):
    """Lance une coroutine dans un nouveau event loop dans un thread."""
    tasks_status[task_name]["running"] = True
    tasks_status[task_name]["last_run"] = datetime.now().isoformat()

    def target():
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(coro_func(**kwargs))
            tasks_status[task_name]["last_result"] = {"status": "success", "detail": str(result)}
            logger.info(f"[{task_name}] Terminé avec succès")
        except Exception as e:
            tasks_status[task_name]["last_result"] = {"status": "error", "detail": str(e)}
            logger.error(f"[{task_name}] Erreur: {e}")
        finally:
            tasks_status[task_name]["running"] = False

    thread = threading.Thread(target=target, daemon=True)
    thread.start()


@app.route("/trigger/enrich", methods=["POST", "GET"])
def trigger_enrich():
    if tasks_status["enrich"]["running"]:
        return jsonify({"status": "already_running", "started_at": tasks_status["enrich"]["last_run"]}), 409

    batch_size = int(request.args.get("batch_size", 200))
    start_row = int(request.args.get("start_row", 2))
    concurrency = int(request.args.get("concurrency", 10))
    force = request.args.get("force", "false").lower() == "true"
    use_browser = request.args.get("browser", "false").lower() == "true"

    from enrich_doctolib_status import run as enrich_run
    run_async_in_thread(enrich_run, "enrich",
        batch_size=batch_size, start_row=start_row,
        concurrency=concurrency, skip_filled=not force, use_browser=use_browser)

    return jsonify({"status": "started", "message": f"Enrichissement lancé (batch={batch_size}, concurrency={concurrency})"}), 200


@app.route("/trigger/orchestrator", methods=["POST", "GET"])
def trigger_orchestrator():
    if tasks_status["orchestrator"]["running"]:
        return jsonify({"status": "already_running", "started_at": tasks_status["orchestrator"]["last_run"]}), 409

    city = request.args.get("city", "Paris")
    max_pages = int(request.args.get("max_pages", 5))
    dry_run = request.args.get("dry_run", "false").lower() == "true"

    from orchestrator import run as orchestrator_run
    run_async_in_thread(orchestrator_run, "orchestrator",
        city=city, max_pages=max_pages, dry_run=dry_run, output_dir=".")

    return jsonify({"status": "started", "message": f"Orchestrateur lancé (city={city}, max_pages={max_pages})"}), 200


@app.route("/status", methods=["GET"])
def status():
    return jsonify(tasks_status), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "timestamp": datetime.now().isoformat()}), 200


@app.route("/", methods=["GET"])
def index():
    return jsonify({
        "service": "Easydentist Pipeline — Railway",
        "endpoints": {
            "/trigger/enrich": "POST/GET — Enrichissement Doctolib",
            "/trigger/orchestrator": "POST/GET — Pipeline complet",
            "/status": "GET — État des tâches",
            "/health": "GET — Health check",
        },
    }), 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=False)
