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
python -m pip install wheel
python -m pip install 'git+ssh://git@github.com/Seidr-Edu/Gjallarhorn.git@main'
```

If you want the bundled `docker-compose.yml`, Loki/Vector config, and Grafana
dashboards on the VPS, clone the repo as well:

```bash
git clone git@github.com:Seidr-Edu/Gjallarhorn.git /opt/gjallarhorn
cd /opt/gjallarhorn/deploy/observability
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

The deployment bundle lives in `deploy/observability/` and includes:

- `docker-compose.yml`
- Loki config
- Vector config
- Grafana datasource and dashboard provisioning
- the `run-indexer` Dockerfile

Bring it up with:

```bash
cd deploy/observability
docker compose up -d --build
```

## Release

This repo now follows the same release-management pattern as the other service
repos:

- CI workflow in `.github/workflows/ci.yml`
- semantic-release workflow in `.github/workflows/release.yml`
- branch and plugin config in `.releaserc.json`

Release automation runs on `main` and `master`.

## VPS Setup

Full VPS setup for the bundled observability stack:

1. Install host prerequisites.

```bash
sudo apt-get update
sudo apt-get install -y git docker.io docker-compose-plugin python3 python3-venv
sudo systemctl enable --now docker
```

2. Ensure the pipeline filesystem already exists and is populated by Heimdall.

Expected roots:

- `/srv/pipeline/queue`
- `/srv/pipeline/runs`

3. Clone the repo for deployment assets.

```bash
sudo mkdir -p /opt
sudo chown "$USER":"$USER" /opt
git clone git@github.com:Seidr-Edu/Gjallarhorn.git /opt/gjallarhorn
cd /opt/gjallarhorn
```

4. Install the CLI in a dedicated virtualenv.

```bash
python3 -m venv /opt/gjallarhorn/.venv
. /opt/gjallarhorn/.venv/bin/activate
python -m pip install --upgrade pip wheel
python -m pip install 'git+ssh://git@github.com/Seidr-Edu/Gjallarhorn.git@main'
gjallarhorn --help
```

5. Prepare the observability stack config.

```bash
cd /opt/gjallarhorn/deploy/observability
cp .env.example .env
```

Edit `.env` and set at minimum:

- `POSTGRES_PASSWORD`
- `GRAFANA_ADMIN_PASSWORD`
- `GJALLARHORN_DB_DSN`

Recommended DSN:

```env
POSTGRES_DB=gjallarhorn_observability
POSTGRES_USER=gjallarhorn
POSTGRES_PASSWORD=<strong-password>
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=<strong-password>
GJALLARHORN_DB_DSN=postgresql://gjallarhorn:<strong-password>@postgres:5432/gjallarhorn_observability
```

6. Confirm the host exposes the log and pipeline mounts the stack expects.

Required host paths:

- `/srv/pipeline/queue`
- `/srv/pipeline/runs`
- `/var/log/journal`
- `/run/log/journal`
- `/etc/machine-id`

7. Start the stack.

```bash
cd /opt/gjallarhorn/deploy/observability
docker compose up -d --build
docker compose ps
```

8. Verify the services.

```bash
docker compose logs -f run-indexer
docker compose logs -f vector
docker compose logs -f grafana
```

What to expect:

- `postgres` becomes healthy
- `run-indexer` emits `observability_indexer_start`
- `run-indexer` emits `observability_indexed_run` for existing runs
- Grafana is reachable on `http://<vps-ip>:3000`

9. Open Grafana and verify data sources and dashboards.

Default URL:

- `http://<vps-ip>:3000`

Use the credentials from `.env`, then confirm:

- datasource `postgres-observability` is healthy
- datasource `loki-observability` is healthy
- dashboards appear under the `Gjallarhorn` folder

10. If you want only the indexer without the full Docker stack, run it directly.

```bash
. /opt/gjallarhorn/.venv/bin/activate
export GJALLARHORN_DB_DSN='postgresql://gjallarhorn:<strong-password>@localhost:5432/gjallarhorn_observability'
gjallarhorn indexer \
  --queue-root /srv/pipeline/queue \
  --runs-root /srv/pipeline/runs \
  --poll-interval-sec 15 \
  --retention-days 30
```

11. Keep it updated.

```bash
cd /opt/gjallarhorn
git pull --ff-only
. /opt/gjallarhorn/.venv/bin/activate
python -m pip install --upgrade pip wheel
python -m pip install 'git+ssh://git@github.com/Seidr-Edu/Gjallarhorn.git@main'
cd /opt/gjallarhorn/deploy/observability
docker compose up -d --build
```

## Troubleshooting

### `run-indexer`: `password authentication failed for user "gjallarhorn"`

This means the password inside `GJALLARHORN_DB_DSN` does not match the password
that Postgres is actually using.

Check these first:

- `POSTGRES_USER` in `deploy/observability/.env`
- `POSTGRES_PASSWORD` in `deploy/observability/.env`
- `GJALLARHORN_DB_DSN` in `deploy/observability/.env`

They must agree. Example:

```env
POSTGRES_USER=gjallarhorn
POSTGRES_PASSWORD=my-secret-password
GJALLARHORN_DB_DSN=postgresql://gjallarhorn:my-secret-password@postgres:5432/gjallarhorn_observability
```

Important: if you changed `.env` after Postgres had already initialized its data
directory, the existing `postgres_data` volume still keeps the old password.

If this is a fresh observability install and you do not need to keep the DB
contents yet, reset the stack volume:

```bash
cd /opt/gjallarhorn/deploy/observability
docker compose down -v
docker compose up -d --build
```

If you need to keep existing Postgres data, do not delete the volume. In that
case, either:

- change `GJALLARHORN_DB_DSN` back to the original password used when Postgres was first created
- or log into Postgres and rotate the user password to match the DSN

### `vector`: `unhandled fallible assignment` / `undefined variable`

That was caused by invalid VRL in the shipped `vector.yaml`. Update to the
latest `Gjallarhorn` commit and rebuild the stack:

```bash
cd /opt/gjallarhorn
git pull --ff-only
cd /opt/gjallarhorn/deploy/observability
docker compose up -d --build vector
```

If you want to restart the full stack:

```bash
docker compose up -d --build
```
