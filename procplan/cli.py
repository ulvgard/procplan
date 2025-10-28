from __future__ import annotations

import argparse
import sys
import json
import os
from datetime import datetime, timedelta, timezone
from typing import List
import urllib.error
import urllib.parse
import urllib.request

UTC = timezone.utc


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect GPU availability via a running ProcPlan server."
    )
    parser.add_argument("--url", required=True, help="Base URL of the ProcPlan server, e.g. http://localhost:8080")
    parser.add_argument(
        "--node",
        action="append",
        dest="nodes",
        help="Filter availability to specific node id (can be used multiple times). Defaults to the current host name.",
    )
    parser.add_argument(
        "-a",
        "--all",
        action="store_true",
        help="Show availability for all nodes.",
    )
    parser.add_argument(
        "--date",
        help="Inspect a full UTC day (YYYY-MM-DD). If omitted, shows the current week (Monday 00:00 to next Monday 00:00).",
    )
    return parser.parse_args(argv)


def fetch_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.load(response)


def render_availability(base_url: str, node_id: str, start: datetime, end: datetime) -> str:
    query = urllib.parse.urlencode(
        {
            "node_id": node_id,
            "start": start.isoformat(),
            "end": end.isoformat(),
        }
    )
    availability_url = urllib.parse.urljoin(base_url, f"/api/availability?{query}")
    availability = fetch_json(availability_url)
    grid = availability.get("grid") or {}
    hours = grid.get("hours") or []
    rows = grid.get("rows") or []

    # Build day buckets from hourly data.
    day_map: dict[str, dict[str, List[int]]] = {}
    for idx, hour in enumerate(hours):
        start_iso = hour.get("start")
        if not start_iso:
            continue
        day_key = start_iso[:10]
        bucket = day_map.setdefault(day_key, {"indices": [], "start": start_iso})
        bucket["indices"].append(idx)
    day_items = sorted(day_map.items(), key=lambda item: item[1]["start"])
    day_keys = [key for key, _ in day_items]

    priority_rank = {"low": 0, "medium": 1, "high": 2}

    table_rows: List[List[str]] = []
    header = ["GPU"] + day_keys

    for row in rows:
        gpu = row.get("gpu") or {}
        label = f"{gpu.get('id', 'GPU')} ({gpu.get('kind', '-')})"
        cells: List[str] = [label]
        slots = row.get("hour_slots") or []
        for key, info in day_items:
            indices = info["indices"]
            best_rank = -1
            best_label = "-"
            for idx in indices:
                if idx >= len(slots):
                    continue
                slot = slots[idx] or {}
                if slot.get("status") != "booked":
                    continue
                booking = slot.get("booking") or {}
                priority = (booking.get("priority") or "medium").lower()
                rank = priority_rank.get(priority, 1)
                if rank >= best_rank:
                    best_rank = rank
                    user = booking.get("user_label") or ""
                    priority_text = priority.capitalize()
                    best_label = f"{user} ({priority_text})" if user else priority_text
            cells.append(best_label)
        table_rows.append(cells)

    # Determine column widths
    widths = [len(col) for col in header]
    for row in table_rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def fmt_row(values: List[str]) -> str:
        return " | ".join(value.ljust(widths[idx]) for idx, value in enumerate(values))

    lines = [f"Node {availability['node']['name']} ({node_id})"]
    lines.append(f"Window: {start.isoformat()} – {end.isoformat()}")
    lines.append(fmt_row(header))
    lines.append("-+-".join("-" * width for width in widths))
    for row in table_rows:
        lines.append(fmt_row(row))
    lines.append("")
    return "\n".join(lines)


def fetch_nodes(base_url: str) -> List[dict]:
    nodes_url = urllib.parse.urljoin(base_url, "/api/nodes")
    data = fetch_json(nodes_url)
    return data.get("nodes", [])


def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv)
    base_url = args.url.rstrip("/")

    try:
        nodes = fetch_nodes(base_url)
    except urllib.error.URLError as exc:
        sys.stderr.write(f"Failed to contact server: {exc}\n")
        return 1
    except json.JSONDecodeError:
        sys.stderr.write("Server returned invalid JSON while listing nodes.\n")
        return 1

    if args.date:
        try:
            day = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            sys.stderr.write("Invalid --date value, expected YYYY-MM-DD\n")
            return 1
        start = day.replace(tzinfo=UTC)
        end = start + timedelta(days=1)
    else:
        now = datetime.now(tz=UTC)
        weekday = now.weekday()  # Monday = 0
        start = now - timedelta(days=weekday)
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        start = start.astimezone(UTC)
        end = start + timedelta(days=7)

    start = start.astimezone(UTC)
    end = end.astimezone(UTC)

    if args.all:
        target_ids = None
    elif args.nodes:
        target_ids = set(args.nodes)
    else:
        try:
            default_node = os.uname().nodename
        except AttributeError:
            import socket

            default_node = socket.gethostname()
        target_ids = {default_node}

    filtered_nodes = [node for node in nodes if not target_ids or node["id"] in target_ids]
    if not filtered_nodes:
        if target_ids:
            sys.stderr.write(
                "No matching nodes found for filter(s): "
                + ", ".join(sorted(target_ids))
                + ". Use --all to list every node.\n"
            )
        else:
            sys.stderr.write("No nodes reported by the server.\n")
        return 1

    reports = []
    for node in filtered_nodes:
        try:
            reports.append(render_availability(base_url, node["id"], start, end))
        except urllib.error.URLError as exc:
            sys.stderr.write(f"Failed to fetch availability for {node['id']}: {exc}\n")
        except json.JSONDecodeError:
            sys.stderr.write(f"Invalid JSON for node {node['id']} availability.\n")

    sys.stdout.write("\n".join(reports))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
