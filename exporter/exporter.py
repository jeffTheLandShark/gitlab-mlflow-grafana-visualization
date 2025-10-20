# exporter/exporter.py
import os
import time
import logging
from mlflow import MlflowClient
from prometheus_client import (
    Gauge,
    start_http_server,
    CollectorRegistry,
    generate_latest,
)
from prometheus_client.core import REGISTRY

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mlflow_exporter")


REFRESH_INTERVAL = int(os.environ.get("REFRESH_INTERVAL", "60"))
EXPORTER_PORT = int(os.environ.get("EXPORTER_PORT", "8000"))

# Metric registry and a general-purpose gauge
# We use a single gauge with labels so Prometheus can filter by metric name
METRIC = Gauge(
    "mlflow_metric",
    "Generic MLflow metric (value) with labels (experiment, run_id, metric).",
    ["experiment", "run_id", "metric"],
)


def safe_str(s):
    try:
        return str(s)
    except Exception:
        return ""


def collect_all_metrics(client: MlflowClient):
    """
    Fetch all experiments -> all runs -> all numeric metrics
    and update Prometheus gauges.
    """
    logger.info("Fetching experiments from MLflow at: %s", client.tracking_uri)
    try:
        # Newer/older MLflow client versions expose different helpers.
        # Prefer `list_experiments` when present, otherwise fall back to `search_experiments`.
        if hasattr(client, "list_experiments"):
            experiments = client.list_experiments()
        elif hasattr(client, "search_experiments"):
            # search_experiments accepts a filter_string; return all with empty filter
            experiments = client.search_experiments(filter_string="", max_results=10000)
        else:
            logger.error("MlflowClient has no method to list or search experiments")
            return
    except Exception as e:
        logger.exception("Failed to list experiments: %s", e)
        return

    # Clear previous values by creating a fresh registry? We'll simply set metrics we see.
    seen_labels = set()

    for exp in experiments:
        exp_id = exp.experiment_id
        exp_name = exp.name or f"experiment_{exp_id}"
        logger.info("Processing experiment: id=%s name=%s", exp_id, exp_name)

        # Fetch runs for this experiment. Increase max_results if you expect lots of runs.
        try:
            runs = client.search_runs(
                experiment_ids=[exp_id],
                filter_string="",
                run_view_type=1,
                max_results=10000,
            )
        except Exception as e:
            logger.exception("Failed to search runs for experiment %s: %s", exp_id, e)
            continue

        for run in runs:
            run_id = run.info.run_id
            metrics = run.data.metrics or {}
            # log number of metrics
            if not metrics:
                continue
            for mname, mval in metrics.items():
                # only numeric metrics
                try:
                    val = float(mval)
                except Exception:
                    logger.debug(
                        "Skipping non-numeric metric %s for run %s", mname, run_id
                    )
                    continue
                # Update gauge
                METRIC.labels(
                    experiment=exp_name, run_id=run_id, metric=safe_str(mname)
                ).set(val)
                seen_labels.add((exp_name, run_id, safe_str(mname)))

    logger.info("Metric collection complete. Total label tuples: %d", len(seen_labels))


def main():
    # Start HTTP server for Prometheus to scrape
    start_http_server(EXPORTER_PORT)
    logger.info("Exporter listening on :%d/metrics", EXPORTER_PORT)

    client = MlflowClient()

    # initial collect
    while True:
        try:
            collect_all_metrics(client)
        except Exception as e:
            logger.exception("Error collecting metrics: %s", e)
        time.sleep(REFRESH_INTERVAL)


if __name__ == "__main__":
    main()
