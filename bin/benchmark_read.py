#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
import statistics
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Sequence, Tuple

from procplan.config import Config, load_config
from procplan.db import Database
from procplan.service import ProcPlanService

UTC = timezone.utc
GPU_KINDS = ("A100", "H100", "RTX6000", "L40S", "A10")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark ProcPlan availability reads with many nodes/GPUs."
    )
    parser.add_argument(
        "--config",
        default="data/benchmark-config.json",
        help="Path to benchmark config JSON (created if missing).",
    )
    parser.add_argument(
        "--database",
        default="data/benchmark.db",
        help="Path to benchmark sqlite DB (created if missing).",
    )
    parser.add_argument(
        "--nodes",
        type=int,
        default=10,
        help="Number of nodes to generate when creating a config.",
    )
    parser.add_argument(
        "--min-gpus",
        type=int,
        default=4,
        help="Minimum GPUs per node when generating a config.",
    )
    parser.add_argument(
        "--max-gpus",
        type=int,
        default=10,
        help="Maximum GPUs per node when generating a config.",
    )
    parser.add_argument(
        "--generate-config",
        action="store_true",
        help="Regenerate the benchmark config file.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing config when generating.",
    )
    parser.add_argument(
        "--reset-db",
        action="store_true",
        help="Delete the benchmark database before running.",
    )
    parser.add_argument(
        "--seed-per-gpu",
        type=int,
        default=0,
        help="Create this many bookings per GPU before benchmarking.",
    )
    parser.add_argument(
        "--booking-hours",
        type=int,
        default=4,
        help="Duration for seeded bookings (hours).",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=5,
        help="Number of benchmark iterations to measure.",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=1,
        help="Warmup iterations (not included in stats).",
    )
    parser.add_argument(
        "--date",
        help="Date (YYYY-MM-DD) used to pick the benchmark year window.",
    )
    parser.add_argument(
        "--mode",
        choices=("service", "http"),
        default="service",
        help="Benchmark the in-process service or the HTTP API.",
    )
    parser.add_argument(
        "--url",
        help="Base URL for http mode, e.g. http://localhost:8080",
    )
    parser.add_argument(
        "--http-timeout",
        type=float,
        default=20.0,
        help="Timeout (seconds) for HTTP requests in http mode.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Serialize availability payloads to JSON during the benchmark.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=1337,
        help="Random seed used for config generation and bookings.",
    )
    return parser.parse_args(argv)


def year_window(date_value: str | None) -> Tuple[datetime, datetime]:
    if date_value:
        try:
            day = datetime.strptime(date_value, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("Invalid --date value, expected YYYY-MM-DD") from exc
        year = day.year
    else:
        year = datetime.now(tz=UTC).year
    start = datetime(year, 1, 1, tzinfo=UTC)
    end = datetime(year + 1, 1, 1, tzinfo=UTC)
    return start, end


def generate_config(nodes: int, min_gpus: int, max_gpus: int, seed: int) -> dict:
    rng = random.Random(seed)
    if min_gpus <= 0 or max_gpus <= 0 or max_gpus < min_gpus:
        raise ValueError("GPU range must be positive and min <= max.")
    payload_nodes = []
    for idx in range(nodes):
        node_id = f"node-{idx + 1:02d}"
        gpu_count = rng.randint(min_gpus, max_gpus)
        gpus = []
        for gpu_idx in range(gpu_count):
            gpus.append(
                {
                    "id": f"{node_id}-gpu{gpu_idx:02d}",
                    "kind": rng.choice(GPU_KINDS),
                }
            )
        payload_nodes.append(
            {
                "id": node_id,
                "name": f"Node {idx + 1:02d}",
                "gpus": gpus,
            }
        )
    return {"nodes": payload_nodes}


def write_config(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
        fh.write("\n")


def summarize_config(config: Config) -> Tuple[int, int, List[int]]:
    gpu_counts = [node.gpu_count for node in config.nodes]
    total_gpus = sum(gpu_counts)
    return len(config.nodes), total_gpus, gpu_counts


def reset_db(db_path: Path) -> None:
    if db_path.exists():
        db_path.unlink()


def seed_bookings(
    db: Database,
    config: Config,
    *,
    start: datetime,
    end: datetime,
    per_gpu: int,
    duration_hours: int,
    seed: int,
) -> int:
    if per_gpu <= 0:
        return 0
    if duration_hours <= 0:
        raise ValueError("Booking duration must be positive.")

    rng = random.Random(seed)
    created = 0
    total_hours = int((end - start).total_seconds() // 3600)
    if total_hours <= duration_hours:
        return 0

    spacing = max(duration_hours, total_hours // max(1, per_gpu))
    for node in config.nodes:
        for gpu in node.gpus:
            for idx in range(per_gpu):
                offset_hours = idx * spacing
                booking_start = start + timedelta(hours=offset_hours)
                booking_end = booking_start + timedelta(hours=duration_hours)
                if booking_end > end or booking_start >= end:
                    break
                priority = rng.choice(("low", "medium", "high"))
                label = f"bench-{node.id}"
                try:
                    db.create_booking(
                        node_id=node.id,
                        gpu_ids=[gpu.id],
                        start_utc=booking_start,
                        end_utc=booking_end,
                        user_label=label,
                        priority=priority,
                    )
                except ValueError:
                    continue
                created += 1
    return created


def pct(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    idx = int(round((len(sorted_vals) - 1) * p))
    return sorted_vals[max(0, min(idx, len(sorted_vals) - 1))]


def fetch_json(url: str, timeout: float) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.load(response)


def benchmark_service(
    service: ProcPlanService,
    start: datetime,
    end: datetime,
    *,
    iterations: int,
    warmup: int,
    include_json: bool,
) -> Tuple[List[float], List[List[float]], List[str]]:
    totals: List[float] = []
    per_node_runs: List[List[float]] = []
    node_ids: List[str] = []

    for idx in range(iterations + warmup):
        iter_node_times: List[float] = []
        iter_start = time.perf_counter()

        nodes = service.list_nodes()
        if not node_ids:
            node_ids = [node["id"] for node in nodes]
        for node in nodes:
            node_start = time.perf_counter()
            availability = service.compute_availability(
                node_id=node["id"],
                start=start,
                end=end,
                granularity="day",
            )
            if include_json:
                json.dumps(availability)
            iter_node_times.append(time.perf_counter() - node_start)

        total = time.perf_counter() - iter_start
        if idx >= warmup:
            totals.append(total)
            per_node_runs.append(iter_node_times)

    return totals, per_node_runs, node_ids


def benchmark_http(
    base_url: str,
    start: datetime,
    end: datetime,
    *,
    iterations: int,
    warmup: int,
    timeout: float,
    include_json: bool,
) -> Tuple[List[float], List[List[float]], List[dict]]:
    totals: List[float] = []
    per_node_runs: List[List[float]] = []
    node_snapshot: List[dict] = []
    base_url = base_url.rstrip("/")

    for idx in range(iterations + warmup):
        iter_node_times: List[float] = []
        iter_start = time.perf_counter()

        nodes_payload = fetch_json(urllib.parse.urljoin(base_url, "/api/nodes"), timeout)
        nodes = nodes_payload.get("nodes", [])
        if not node_snapshot:
            node_snapshot = list(nodes)
        for node in nodes:
            query = urllib.parse.urlencode(
                {
                    "node_id": node["id"],
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "granularity": "day",
                }
            )
            node_start = time.perf_counter()
            availability = fetch_json(
                urllib.parse.urljoin(base_url, f"/api/availability?{query}"),
                timeout,
            )
            if include_json:
                json.dumps(availability)
            iter_node_times.append(time.perf_counter() - node_start)

        total = time.perf_counter() - iter_start
        if idx >= warmup:
            totals.append(total)
            per_node_runs.append(iter_node_times)

    return totals, per_node_runs, node_snapshot


def print_summary(
    totals: List[float],
    per_node_runs: List[List[float]],
    node_ids: List[str],
) -> None:
    if not totals:
        print("No benchmark iterations recorded.")
        return
    mean_total = statistics.mean(totals)
    median_total = statistics.median(totals)
    p95_total = pct(totals, 0.95)
    max_total = max(totals)
    min_total = min(totals)

    print("Totals (seconds): " + ", ".join(f"{t:.4f}" for t in totals))
    print(
        "Total summary: "
        f"mean={mean_total:.4f} median={median_total:.4f} p95={p95_total:.4f} "
        f"min={min_total:.4f} max={max_total:.4f}"
    )

    if not per_node_runs:
        return

    per_node_means = []
    for node_idx, node_id in enumerate(node_ids):
        samples = [run[node_idx] for run in per_node_runs if node_idx < len(run)]
        if samples:
            per_node_means.append((node_id, statistics.mean(samples)))

    if per_node_means:
        print("Per-node mean (seconds):")
        for node_id, mean_val in per_node_means:
            print(f"  {node_id}: {mean_val:.4f}")


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    config_path = Path(args.config)
    db_path = Path(args.database)

    start, end = year_window(args.date)

    if args.mode == "service":
        if args.reset_db:
            reset_db(db_path)

        if args.generate_config or not config_path.exists():
            if config_path.exists() and not args.force:
                sys.stderr.write(
                    f"Config {config_path} already exists. Use --force to overwrite.\n"
                )
                return 2
            payload = generate_config(args.nodes, args.min_gpus, args.max_gpus, args.seed)
            write_config(config_path, payload)

        config = load_config(config_path)
        node_count, total_gpus, gpu_counts = summarize_config(config)
        service = ProcPlanService(config_path, db_path)
        gpu_min = min(gpu_counts) if gpu_counts else 0
        gpu_max = max(gpu_counts) if gpu_counts else 0

        if args.seed_per_gpu > 0:
            created = seed_bookings(
                service.database,
                config,
                start=start,
                end=end,
                per_gpu=args.seed_per_gpu,
                duration_hours=args.booking_hours,
                seed=args.seed,
            )
            print(f"Seeded {created} bookings.")

        totals, per_node_runs, node_ids = benchmark_service(
            service,
            start,
            end,
            iterations=args.iterations,
            warmup=args.warmup,
            include_json=args.json,
        )

        print(f"Config: {config_path}")
        print(f"Database: {db_path}")
        print(
            f"Nodes: {node_count}, GPUs: {total_gpus} "
            f"(min {gpu_min}, max {gpu_max})"
        )
    else:
        if args.generate_config or args.reset_db:
            sys.stderr.write(
                "Note: --generate-config/--reset-db are ignored in http mode.\n"
            )
        if not args.url:
            sys.stderr.write("--url is required for http mode.\n")
            return 2
        totals, per_node_runs, nodes = benchmark_http(
            args.url,
            start,
            end,
            iterations=args.iterations,
            warmup=args.warmup,
            timeout=args.http_timeout,
            include_json=args.json,
        )
        node_ids = [node.get("id", "unknown") for node in nodes]
        gpu_counts = [len(node.get("gpus") or []) for node in nodes]
        node_count = len(node_ids)
        total_gpus = sum(gpu_counts)
        gpu_min = min(gpu_counts) if gpu_counts else 0
        gpu_max = max(gpu_counts) if gpu_counts else 0

        print(f"Base URL: {args.url}")
        print(f"Nodes: {node_count}, GPUs: {total_gpus} (min {gpu_min}, max {gpu_max})")

    print(f"Window: {start.isoformat()} -> {end.isoformat()} (UTC, granularity=day)")
    print(f"Mode: {args.mode}, iterations={args.iterations}, warmup={args.warmup}")
    if args.mode == "service" and args.seed_per_gpu > 0:
        print(f"Bookings: {args.seed_per_gpu} per GPU, duration {args.booking_hours}h")

    print_summary(totals, per_node_runs, node_ids)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
