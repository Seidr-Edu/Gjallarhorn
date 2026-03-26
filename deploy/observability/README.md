# Gjallarhorn Observability Stack

This bundle deploys the first-phase observability stack for the pipeline:

- `Postgres` for structured pipeline data indexed from the filesystem
- `Loki` for raw logs
- `Vector` for journald/file collection and label enrichment
- `Grafana` for dashboards and log drill-down
- `run-indexer` as a standalone Gjallarhorn service

The stack assumes the pipeline worker already owns:

- `/srv/pipeline/queue`
- `/srv/pipeline/runs`

The observability services mount those paths read-only and do not mutate the
pipeline itself.

## Files

- `docker-compose.yml`
- `.env.example`
- `run-indexer.Dockerfile`
- `loki/config.yml`
- `vector/vector.yaml`
- `grafana/provisioning/...`
- `grafana/dashboards/...`

## Setup

1. Clone the repo on the VPS:

```bash
git clone git@github.com:Seidr-Edu/Gjallarhorn.git /opt/gjallarhorn
cd /opt/gjallarhorn/deploy/observability
```

2. Copy `.env.example` to `.env` and set the Grafana and Postgres passwords.
3. Ensure the host exposes:
   - `/srv/pipeline/queue`
   - `/srv/pipeline/runs`
   - `/var/log/journal`
   - `/run/log/journal`
   - `/etc/machine-id`
4. Start the stack:

```bash
docker compose up -d --build
```

Grafana will be available on `http://<host>:3000`.

## Indexer

The `run-indexer` container runs:

```bash
python -m gjallarhorn.cli indexer \
  --queue-root /srv/pipeline/queue \
  --runs-root /srv/pipeline/runs \
  --poll-interval-sec 15 \
  --retention-days 30
```

The Postgres schema defaults to `pipeline_obs`.

## Dashboards

Provisioned dashboards:

- `Pipeline Overview`
- `Run Detail`
- `Kvasir Analysis`
- `Lidskjalv / Sonar`
- `Logs Drill-down`

The dashboards assume the provisioned datasource UIDs:

- `postgres-observability`
- `loki-observability`

## Troubleshooting

### `run-indexer` cannot authenticate to Postgres

If you see `password authentication failed for user "gjallarhorn"`, check that
these values in `.env` all match:

- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `GJALLARHORN_DB_DSN`

If Postgres was already initialized with an older password, changing `.env`
alone is not enough because the `postgres_data` volume keeps the original DB
state. For a fresh install, reset the stack volumes:

```bash
docker compose down -v
docker compose up -d --build
```

### Vector VRL parse error

If Vector fails with `unhandled fallible assignment`, update to the latest repo
version and rebuild the `vector` service:

```bash
cd /opt/gjallarhorn
git pull --ff-only
cd /opt/gjallarhorn/deploy/observability
docker compose up -d --build vector
```
