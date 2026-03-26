from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from .indexer import ObservabilityIndexer, PostgresIndexerRepository


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "indexer":
            return _indexer_command(args)
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    raise AssertionError(f"Unhandled command: {args.command}")


def run_cli() -> None:
    raise SystemExit(main())


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gjallarhorn observability service for the pipeline"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    indexer_parser = subparsers.add_parser(
        "indexer",
        help="Index queue state, run reports, and service reports into Postgres",
    )
    indexer_parser.add_argument("--queue-root", type=Path, required=True)
    indexer_parser.add_argument("--runs-root", type=Path, required=True)
    indexer_parser.add_argument("--db-dsn")
    indexer_parser.add_argument("--schema", default="pipeline_obs")
    indexer_parser.add_argument("--poll-interval-sec", type=int, default=15)
    indexer_parser.add_argument("--retention-days", type=int, default=30)
    indexer_parser.add_argument(
        "--once",
        action="store_true",
        help="Run one indexing pass and then exit",
    )
    return parser


def _indexer_command(args: argparse.Namespace) -> int:
    dsn = args.db_dsn or os.environ.get("GJALLARHORN_DB_DSN")
    if not dsn:
        raise RuntimeError(
            "Provide --db-dsn or set GJALLARHORN_DB_DSN for the indexer connection"
        )
    repository = PostgresIndexerRepository(dsn, schema=args.schema)
    indexer = ObservabilityIndexer(
        queue_root=args.queue_root,
        runs_root=args.runs_root,
        repository=repository,
        poll_interval_sec=args.poll_interval_sec,
        retention_days=args.retention_days,
    )
    return indexer.run_loop(once=args.once)


if __name__ == "__main__":
    run_cli()
