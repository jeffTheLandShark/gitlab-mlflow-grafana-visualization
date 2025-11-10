# Experiment Dashboard 
<!-- grafana docerized dashboard for viewing mlflow experiemnts saved in gitlab -->
Lightweight monitoring stack for visualizing MLflow experiments and metrics using Grafana and Postgres. Exports metrics from an MLflow tracking server into a Postgres DB and runs a preconfigured Grafana dashboard to visualize them.

## Overview

This repository provides:
- An exporter script that polls an MLflow tracking server and writes metrics to Postgres.
- A Grafana container pre-provisioned for visualizing MLflow experiments and metrics.
- A pre-configured Postgres container for storing the exported metrics.
- A docker-compose setup to run everything locally.

## Repository Structure

```shell
├── exporter/               # exporter service
├── grafana/                # Grafana provisioning and experiment dashboard
├── timescaledb/init.sql    # DB initialization 
└── docker-compose.yml      # compose file for starting the stack
```

## Prerequisites

- Docker Desktop
- Python 3.8+ (if running exporter locally without Docker)

## Environment Variables

| Variable                | Description                                                       | Default                                     |
|-------------------------|-------------------------------------------------------------------|---------------------------------------------|
| `MLFLOW_TRACKING_URI`   | URL to your MLflow tracking server (e.g. GitLab MLflow API URL).  | NONE                                        |
| `MLFLOW_TRACKING_TOKEN` | Token for authenticating to the MLflow tracking server.           | NONE                                        |
| `DATABASE_URL`          | Postgres connection string used by the exporter to write metrics. | `postgresql://mlflow:mlflow@db:5432/mlflow` |
| `REFRESH_INTERVAL`      | Seconds between exporter polls.                                   | 60                                          |
| `EXPORTER_PORT`         | Port the exporter listens on inside the container.                | 8000                                        |

### Example `.env`

```powershell
# .env (example)
MLFLOW_TRACKING_URI=https://gitlab.com/api/v4/projects/<project-id>/ml/mlflow/
MLFLOW_TRACKING_TOKEN=<your-token>
DATABASE_URL=postgresql://mlflow:mlflow@db:5432/mlflow
REFRESH_INTERVAL=60
EXPORTER_PORT=8000
```

> **Note:** If you are not running this locally, ensure that you are using a more secure method of managing secrets than a `.env` file.

## Start the Stack (Local PowerShell)

Before starting make sure:
- You have created a `.env` file with the required variables above.
- Docker Desktop is running.

### Docker Compose Commands

Run the following commands from the repo root to start/stop the stack:
```powershell
docker compose up --build       # build and run containers
docker compose up --build -d    # build and run in detached mode
docker compose down             # stop and remove containers
docker compose down -v          # stop and remove containers + volumes (deletes DB data)
```

Once services start, you can go to http://localhost:3000 to access the Grafana interface.

## Grafana Dashboards

Grafana is pre-provisioned from the files in `grafana/provisioning` and `grafana/dashboards`.

Currently, I have one dashboard, `mlflow_sql_dashboard.json`, which is designed to work with exported MLflow metrics stored in SQL. 

![dasboard_screenshot](experiment_dashboard.png)

If you want to add or modify dashboards, add the JSON file to `grafana/dashboards`. Any dashboards placed in that folder will be automatically picked up by Grafana. You can make changes to the JSON directly and Grafana will update them while running.

You can also edit dashboards through the Grafana UI. Be aware that changes made through the UI will not persist if the container is removed and re-created, so consider exporting modified dashboards from the UI back to JSON and saving them in the `grafana/dashboards` folder.

## Exporter Details

The exporter polls the MLflow Tracking API (configured by `MLFLOW_TRACKING_URI` and `MLFLOW_TRACKING_TOKEN`) and writes experiment/run metrics into Postgres.

### Running the Exporter Locally

To run the exporter locally (without Docker), create a virtualenv, install deps and run the script.

#### Example:

Run the following commands from the `exporter/` folder:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:MLFLOW_TRACKING_URI = "https://gitlab.com/api/v4/projects/<id>/ml/mlflow/"
$env:MLFLOW_TRACKING_TOKEN = "<token>"
$env:DATABASE_URL = "postgresql://mlflow:mlflow@localhost:5432/mlflow"
python exporter.py
```

> **Note:** The exporter expects a reachable Postgres instance; if using Docker Compose, the host from inside the exporter container will be `db`.

## Database Details

The compose file mounts `timescaledb/init.sql` into Postgres' init directory so it runs when the DB container is created for the first time.

If you need to re-run `init.sql`, you can either recreate the DB volume or run the SQL against the DB manually using `psql`.



<!-- ## Acknowledgements
This project was possible thanks to the experience I gained working with Grafana through my work at NASA JPL under 347K - Robot Operations sponsored by the Wisconsin Space Grant Consortium.
It was inspired by the need to visualize MLflow experiments stored in GitLab for my senior design project at the Milwaukee School of Engineering (MSOE).  -->
