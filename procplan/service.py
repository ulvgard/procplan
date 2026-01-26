from __future__ import annotations

import threading
import time
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .config import Config, Node, load_config
from .db import Database
from .timeutils import ensure_hour_alignment, hour_range, parse_iso_timestamp

UTC = timezone.utc
PRIORITY_LEVELS = ("low", "medium", "high")


class ProcPlanService:
    """Core application service that coordinates configuration and persistence."""

    def __init__(self, config_path: str | Path, database_path: str | Path):
        self.config_path = Path(config_path)
        self.database = Database(database_path)
        self._lock = threading.RLock()
        self._availability_cache: OrderedDict[tuple[str, str, str, str], tuple[float, Dict[str, Any]]] = OrderedDict()
        self._availability_cache_ttl = 30.0
        self._availability_cache_max = 64
        self._config = self._load_config()
        self.database.sync_from_config(self._config)

    def _load_config(self) -> Config:
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file '{self.config_path}' does not exist")
        return load_config(self.config_path)

    def reload_config(self) -> None:
        with self._lock:
            self._config = self._load_config()
            self.database.sync_from_config(self._config)
            self._availability_cache.clear()

    @property
    def config(self) -> Config:
        with self._lock:
            return self._config

    def list_nodes(self) -> List[Dict[str, Any]]:
        nodes = self.config.nodes
        return [self._node_to_dict(node) for node in nodes]

    def _node_to_dict(self, node: Node) -> Dict[str, Any]:
        return {
            "id": node.id,
            "name": node.name,
            "gpu_count": node.gpu_count,
            "gpus": [
                {"id": gpu.id, "kind": gpu.kind}
                for gpu in node.gpus
            ],
        }

    def get_node(self, node_id: str) -> Node:
        node = self.config.nodes_by_id.get(node_id)
        if not node:
            raise ValueError(f"Unknown node id '{node_id}'")
        return node

    def _cache_key(self, node_id: str, start: datetime, end: datetime, granularity: str) -> tuple[str, str, str, str]:
        return (node_id, start.isoformat(), end.isoformat(), granularity)

    def _get_cached_availability(self, key: tuple[str, str, str, str], now: float) -> Dict[str, Any] | None:
        cached = self._availability_cache.get(key)
        if not cached:
            return None
        created_at, payload = cached
        if now - created_at > self._availability_cache_ttl:
            self._availability_cache.pop(key, None)
            return None
        self._availability_cache.move_to_end(key)
        return payload

    def _store_cached_availability(self, key: tuple[str, str, str, str], now: float, payload: Dict[str, Any]) -> None:
        self._availability_cache[key] = (now, payload)
        self._availability_cache.move_to_end(key)
        while len(self._availability_cache) > self._availability_cache_max:
            self._availability_cache.popitem(last=False)

    def _invalidate_cache(self) -> None:
        with self._lock:
            self._availability_cache.clear()

    def compute_availability(
        self,
        *,
        node_id: str,
        start: datetime,
        end: datetime,
        granularity: str = "hour",
    ) -> Dict[str, Any]:
        ensure_hour_alignment(start)
        ensure_hour_alignment(end)
        if end <= start:
            raise ValueError("Availability end must be after start")

        granularity_key = granularity.lower()
        if granularity_key not in {"hour", "day"}:
            raise ValueError("Granularity must be either 'hour' or 'day'")

        cache_key = self._cache_key(node_id, start, end, granularity_key)
        now = time.monotonic()
        with self._lock:
            cached = self._get_cached_availability(cache_key, now)
            if cached:
                return cached
            node = self._config.nodes_by_id.get(node_id)
        if not node:
            raise ValueError(f"Unknown node id '{node_id}'")

        booking_rows = self.database.list_bookings_for_window(
            node_id=node_id,
            start_utc=start,
            end_utc=end,
        )

        parsed_bookings = self._prepare_bookings(booking_rows)

        if granularity_key == "day":
            availability = self._build_day_grid(node, start, end, parsed_bookings)
        else:
            availability = self._build_hour_grid(node, start, end, parsed_bookings)

        with self._lock:
            self._store_cached_availability(cache_key, now, availability)
        return availability

    def create_booking(
        self,
        *,
        node_id: str,
        start: datetime,
        end: datetime,
        user_label: str,
        gpu_ids: List[str] | None = None,
        gpu_count: int | None = None,
        priority: str | None = None,
    ) -> Dict[str, Any]:
        ensure_hour_alignment(start)
        ensure_hour_alignment(end)
        if end <= start:
            raise ValueError("Booking end must be after start")
        if not user_label.strip():
            raise ValueError("User label must not be empty")

        node = self.get_node(node_id)

        selected_priority = (priority or "medium").lower()
        if selected_priority not in PRIORITY_LEVELS:
            raise ValueError(f"Priority must be one of {', '.join(PRIORITY_LEVELS)}")

        if gpu_ids:
            candidates = gpu_ids
        else:
            if not gpu_count or gpu_count <= 0:
                raise ValueError("Either gpu_ids or a positive gpu_count must be provided")
            candidates = self.database.select_available_gpus(
                node_id=node_id,
                start_utc=start,
                end_utc=end,
                count=gpu_count,
            )
            if not candidates:
                raise ValueError("Not enough GPUs available for the requested window")

        booking_id = self.database.create_booking(
            node_id=node_id,
            gpu_ids=candidates,
            start_utc=start,
            end_utc=end,
            user_label=user_label.strip(),
            priority=selected_priority,
        )
        self._invalidate_cache()

        return {
            "booking_id": booking_id,
            "node_id": node_id,
            "gpu_ids": candidates,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "user_label": user_label.strip(),
            "priority": selected_priority,
        }

    def mark_booking_complete(self, booking_id: int) -> bool:
        success = self.database.mark_booking_done(booking_id=booking_id)
        if success:
            self._invalidate_cache()
        return success

    def cancel_booking(self, booking_id: int) -> bool:
        success = self.database.cancel_booking(booking_id=booking_id)
        if success:
            self._invalidate_cache()
        return success

    def default_availability_window(self) -> Tuple[datetime, datetime]:
        now = datetime.now(tz=UTC)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if now.hour >= 20:
            day_start = day_start + timedelta(days=1)
        day_end = day_start + timedelta(days=1)
        return day_start, day_end

    def _prepare_bookings(self, booking_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        parsed: List[Dict[str, Any]] = []
        for row in booking_rows:
            b_start = parse_iso_timestamp(row["start_utc"])
            b_end = parse_iso_timestamp(row["end_utc"])
            gpu_ids = [gpu.strip() for gpu in (row["gpu_ids"] or "").split(",") if gpu.strip()]
            public_payload = {
                "id": row["id"],
                "user_label": row["user_label"],
                "start": b_start.isoformat(),
                "end": b_end.isoformat(),
                "status": row["status"],
                "gpu_ids": gpu_ids,
                "priority": row["priority"],
            }
            parsed.append(
                {
                    "start": b_start,
                    "end": b_end,
                    "status": row["status"],
                    "gpu_ids": gpu_ids,
                    "public": public_payload,
                }
            )
        return parsed

    def _build_hour_grid(
        self,
        node: Node,
        start: datetime,
        end: datetime,
        parsed_bookings: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        hours_payload: List[Dict[str, Any]] = []
        gpu_cells: Dict[str, List[Dict[str, Any]]] = {gpu.id: [] for gpu in node.gpus}
        grid_hours: List[Dict[str, Any]] = []

        for hour_start in hour_range(start, end):
            hour_end = hour_start + timedelta(hours=1)
            hour_bookings: List[Dict[str, Any]] = []
            active_gpu_map: Dict[str, Dict[str, Any]] = {}

            for booking in parsed_bookings:
                if hour_start >= booking["end"] or hour_end <= booking["start"]:
                    continue
                payload = booking["public"]
                hour_bookings.append(payload)
                if booking["status"] == "active":
                    for gpu_id in booking["gpu_ids"]:
                        active_gpu_map[gpu_id] = payload

            used_gpu_ids = list(active_gpu_map.keys())
            used_set = set(used_gpu_ids)
            available_ids = [gpu.id for gpu in node.gpus if gpu.id not in used_set]

            hours_payload.append(
                {
                    "start": hour_start.isoformat(),
                    "end": hour_end.isoformat(),
                    "available_gpu_ids": available_ids,
                    "used_gpu_ids": used_gpu_ids,
                    "available_count": len(available_ids),
                    "used_count": len(used_gpu_ids),
                    "bookings": hour_bookings,
                }
            )

            grid_hours.append(
                {
                    "start": hour_start.isoformat(),
                    "end": hour_end.isoformat(),
                }
            )

            for gpu in node.gpus:
                booking_payload = active_gpu_map.get(gpu.id)
                cell: Dict[str, Any] = {
                    "start": hour_start.isoformat(),
                    "end": hour_end.isoformat(),
                    "status": "booked" if booking_payload else "free",
                }
                if booking_payload:
                    cell["booking"] = booking_payload
                gpu_cells[gpu.id].append(cell)

        grid_rows: List[Dict[str, Any]] = []
        for gpu in node.gpus:
            grid_rows.append(
                {
                    "gpu": {"id": gpu.id, "kind": gpu.kind},
                    "hour_slots": gpu_cells.get(gpu.id, []),
                }
            )

        return {
            "node": self._node_to_dict(node),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "hours": hours_payload,
            "grid": {
                "hours": grid_hours,
                "rows": grid_rows,
            },
        }

    def _build_day_grid(
        self,
        node: Node,
        start: datetime,
        end: datetime,
        parsed_bookings: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        day_cursor = start
        day_keys: List[str] = []
        day_starts: List[datetime] = []
        day_ends: List[datetime] = []
        day_total_hours: List[int] = []
        while day_cursor < end:
            next_day = min(day_cursor + timedelta(days=1), end)
            day_keys.append(day_cursor.date().isoformat())
            day_starts.append(day_cursor)
            day_ends.append(next_day)
            day_total_hours.append(int((next_day - day_cursor).total_seconds() // 3600))
            day_cursor = next_day

        bookings_by_gpu: Dict[str, List[Dict[str, Any]]] = {gpu.id: [] for gpu in node.gpus}
        for booking in parsed_bookings:
            for gpu_id in booking["gpu_ids"]:
                if gpu_id in bookings_by_gpu:
                    bookings_by_gpu[gpu_id].append(booking)

        priority_rank = {"low": 0, "medium": 1, "high": 2}
        grid_rows: List[Dict[str, Any]] = []
        day_count = len(day_keys)
        seconds_per_day = 24 * 3600
        total_seconds = int((end - start).total_seconds())

        for gpu in node.gpus:
            booked_hours = [0] * day_count
            slot_bookings: List[List[Dict[str, Any]]] = [[] for _ in range(day_count)]
            best_priority: List[str | None] = [None] * day_count
            best_rank = [-1] * day_count

            relevant_bookings = bookings_by_gpu.get(gpu.id, [])
            for booking in relevant_bookings:
                if booking["status"] != "active":
                    continue
                overlap_start = max(start, booking["start"])
                overlap_end = min(end, booking["end"])
                if overlap_start >= overlap_end:
                    continue
                start_offset = int((overlap_start - start).total_seconds())
                end_offset = int((overlap_end - start).total_seconds())
                if end_offset <= 0 or start_offset >= total_seconds:
                    continue

                start_idx = max(0, start_offset // seconds_per_day)
                end_idx = max(0, (end_offset + seconds_per_day - 1) // seconds_per_day)
                if start_idx >= day_count:
                    continue
                end_idx = min(end_idx, day_count)

                for day_idx in range(start_idx, end_idx):
                    day_start = day_starts[day_idx]
                    day_end = day_ends[day_idx]
                    overlap_start = max(day_start, booking["start"])
                    overlap_end = min(day_end, booking["end"])
                    if overlap_start >= overlap_end:
                        continue
                    overlap_hours = int((overlap_end - overlap_start).total_seconds() // 3600)
                    if overlap_hours <= 0:
                        continue

                    booked_hours[day_idx] += overlap_hours
                    summary = dict(booking["public"])
                    summary["hours"] = overlap_hours
                    summary["overlap_start"] = overlap_start.isoformat()
                    summary["overlap_end"] = overlap_end.isoformat()
                    slot_bookings[day_idx].append(summary)

                    priority = (summary.get("priority") or "medium").lower()
                    rank = priority_rank.get(priority, 1)
                    if rank > best_rank[day_idx]:
                        best_rank[day_idx] = rank
                        best_priority[day_idx] = priority

            gpu_day_slots: List[Dict[str, Any]] = []
            for idx in range(day_count):
                total_hours = day_total_hours[idx]
                booked = booked_hours[idx]
                if booked <= 0:
                    status = "free"
                elif booked >= total_hours:
                    status = "occupied"
                else:
                    status = "partial"

                gpu_day_slots.append(
                    {
                        "start": day_starts[idx].isoformat(),
                        "end": day_ends[idx].isoformat(),
                        "status": status,
                        "booked_hours": booked,
                        "total_hours": total_hours,
                        "priority": best_priority[idx] if booked > 0 else None,
                        "bookings": slot_bookings[idx],
                    }
                )

            grid_rows.append(
                {
                    "gpu": {"id": gpu.id, "kind": gpu.kind},
                    "day_slots": gpu_day_slots,
                }
            )

        grid_days = [
            {
                "key": day_keys[idx],
                "start": day_starts[idx].isoformat(),
                "end": day_ends[idx].isoformat(),
            }
            for idx in range(day_count)
        ]

        return {
            "node": self._node_to_dict(node),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "hours": [],
            "grid": {
                "hours": [],
                "days": grid_days,
                "rows": grid_rows,
            },
        }
