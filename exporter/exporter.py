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
import importlib
try:
    import pg8000
except Exception:
    pg8000 = None
from urllib.parse import urlparse


def get_db_params_from_url(db_url: str):
    """Parse a DATABASE_URL (postgresql://user:pass@host:port/dbname)
    and return keyword args suitable for pg8000.connect()."""
    if not db_url:
        return None
    p = urlparse(db_url)
    dbname = p.path.lstrip("/") if p.path else None
    return {
        "user": p.username,
        "password": p.password,
        "host": p.hostname,
        "port": int(p.port) if p.port else 5432,
        "database": dbname,
    }


def db_execute(db_url: str, sql: str, params=None, fetchone: bool = False):
    """Execute a SQL statement against DATABASE_URL using pg8000.
    Returns fetched row when fetchone=True, otherwise None.
    This helper opens and closes a connection per call to keep the code simple
    and avoid relying on connection context-manager semantics.
    """
    if pg8000 is None:
        raise ModuleNotFoundError("pg8000 is not installed. Add pg8000 to requirements.")
    conn_info = get_db_params_from_url(db_url)
    if conn_info is None:
        return None
    conn = pg8000.connect(**conn_info)
    try:
        cur = conn.cursor()
        cur.execute(sql, params or ())
        result = None
        if fetchone:
            result = cur.fetchone()
        conn.commit()
        cur.close()
        return result
    finally:
        try:
            conn.close()
        except Exception:
            pass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mlflow_exporter")


REFRESH_INTERVAL = int(os.environ.get("REFRESH_INTERVAL", "60"))
EXPORTER_PORT = int(os.environ.get("EXPORTER_PORT", "8000"))
DATABASE_URL = os.environ.get("DATABASE_URL")

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

        # Ensure experiment exists in DB
        if DATABASE_URL:
            try:
                res = db_execute(
                    DATABASE_URL,
                    "INSERT INTO experiments (mlflow_experiment_id, name) VALUES (%s, %s) ON CONFLICT (mlflow_experiment_id) DO UPDATE SET name = EXCLUDED.name RETURNING id",
                    (str(exp_id), exp_name),
                    fetchone=True,
                )
                exp_db_id = res[0] if res else None
            except Exception:
                logger.exception("Failed to upsert experiment into DB")
                exp_db_id = None
        else:
            exp_db_id = None

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
            params = run.data.params or {}
            # log number of metrics
            if not metrics:
                continue
            # Ensure run exists in DB
            if DATABASE_URL and exp_db_id is not None:
                try:
                    start_ts = (
                        int(run.info.start_time / 1000)
                        if getattr(run.info, "start_time", None)
                        else None
                    )
                    res = db_execute(
                        DATABASE_URL,
                        "INSERT INTO runs (mlflow_run_id, experiment_id, start_time) VALUES (%s, %s, to_timestamp(%s)) ON CONFLICT (mlflow_run_id) DO UPDATE SET experiment_id=EXCLUDED.experiment_id RETURNING id",
                        (str(run_id), exp_db_id, start_ts),
                        fetchone=True,
                    )
                    run_db_id = res[0] if res else None
                except Exception:
                    logger.exception("Failed to upsert run into DB")
                    run_db_id = None
            else:
                run_db_id = None
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
                # Insert metric into DB
                if DATABASE_URL and run_db_id is not None:
                    try:
                        db_execute(
                            DATABASE_URL,
                            "INSERT INTO metrics (run_id, name, value) VALUES (%s, %s, %s)",
                            (run_db_id, mname, val),
                        )
                    except Exception:
                        logger.exception(
                            "Failed to insert metric into DB for run %s", run_id
                        )

            # Insert params into DB (single upserts per param)
            if DATABASE_URL and run_db_id is not None:
                try:
                    for pname, pval in params.items():
                        try:
                            db_execute(
                                DATABASE_URL,
                                "INSERT INTO params (run_id, name, value) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                                (run_db_id, pname, str(pval)),
                            )
                        except Exception:
                            logger.exception(
                                "Failed to insert param %s for run %s", pname, run_id
                            )
                except Exception:
                    logger.exception(
                        "Failed to insert params into DB for run %s", run_id
                    )

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
