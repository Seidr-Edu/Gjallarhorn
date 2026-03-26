# Gjallarhorn

`Gjallarhorn` is the dedicated observability service package for the pipeline.

It indexes canonical Heimdall queue and run artifacts into Postgres and ships a
deployment bundle for:

- `Postgres`
- `Loki`
- `Vector`
- `Grafana`
- the `Gjallarhorn` run indexer

The package treats Heimdall and the service reports as external contracts. It
does not orchestrate runs and does not modify the pipeline filesystem.

## Install

From a checked-out repo:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install .
```

Directly from Git on the VPS:

```bash
python3 -m venv /opt/gjallarhorn/.venv
. /opt/gjallarhorn/.venv/bin/activate
python -m pip install --upgrade pip
python -m pip install 'git+ssh://git@<your-git-host>/<org>/Gjallarhorn.git@main'
```

If you want the bundled `docker-compose.yml` and Grafana dashboards on the VPS,
clone the repo as well:

```bash
git clone git@<your-git-host>:<org>/Gjallarhorn.git /opt/gjallarhorn
cd /opt/gjallarhorn/examples/observability
cp .env.example .env
```

## Run

Run one indexing pass:

```bash
export GJALLARHORN_DB_DSN='postgresql://gjallarhorn:secret@localhost:5432/pipeline_obs'

gjallarhorn indexer \
  --queue-root /srv/pipeline/queue \
  --runs-root /srv/pipeline/runs \
  --once
```

Run it as a long-lived service:

```bash
export GJALLARHORN_DB_DSN='postgresql://gjallarhorn:secret@localhost:5432/pipeline_obs'

gjallarhorn indexer \
  --queue-root /srv/pipeline/queue \
  --runs-root /srv/pipeline/runs \
  --poll-interval-sec 15 \
  --retention-days 30
```

## Deploy Bundle

The deployment bundle lives in `examples/observability/` and includes:

- `docker-compose.yml`
- Loki config
- Vector config
- Grafana datasource and dashboard provisioning
- the `run-indexer` Dockerfile

Bring it up with:

```bash
cd examples/observability
docker compose up -d --build
```
