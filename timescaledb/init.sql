-- Initialize simple relational schema for MLflow exporter
CREATE TABLE IF NOT EXISTS experiments (
  id SERIAL PRIMARY KEY,
  mlflow_experiment_id TEXT UNIQUE,
  name TEXT
);

CREATE TABLE IF NOT EXISTS runs (
  id SERIAL PRIMARY KEY,
  mlflow_run_id TEXT UNIQUE,
  experiment_id INTEGER REFERENCES experiments(id),
  start_time TIMESTAMP,
  end_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS params (
  id SERIAL PRIMARY KEY,
  run_id INTEGER REFERENCES runs(id),
  name TEXT,
  value TEXT
);

CREATE TABLE IF NOT EXISTS metrics (
  id SERIAL PRIMARY KEY,
  run_id INTEGER REFERENCES runs(id),
  name TEXT,
  value DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_metrics_run_id ON metrics(run_id);
CREATE INDEX IF NOT EXISTS idx_metrics_name ON metrics(name);
CREATE INDEX IF NOT EXISTS idx_runs_experiment_id ON runs(experiment_id);
