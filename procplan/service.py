from __future__ import annotations

import threading
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

    @property
    def config(self) -> Config:
        with self._lock:
            return self._config

    def list_nodes(self) -> List[Dict[str, Any]]:
        nodes = self.database.list_nodes()
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
        node = self.database.fetch_node(node_id)
        if not node:
            raise ValueError(f"Unknown node id '{node_id}'")
        return node

    def compute_availability(
        self,
        *,
        node_id: str,
        start: datetime,
        end: datetime,
    ) -> Dict[str, Any]:
        node = self.get_node(node_id)
        ensure_hour_alignment(start)
        ensure_hour_alignment(end)
        if end <= start:
            raise ValueError("Availability end must be after start")

        booking_rows = self.database.list_bookings_for_window(
            node_id=node_id,
            start_utc=start,
            end_utc=end,
        )

        parsed_bookings: List[Dict[str, Any]] = []
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
            parsed_bookings.append(
                {
                    "start": b_start,
                    "end": b_end,
                    "status": row["status"],
                    "gpu_ids": gpu_ids,
                    "public": public_payload,
                }
            )

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
        return self.database.mark_booking_done(booking_id=booking_id)

    def default_availability_window(self) -> Tuple[datetime, datetime]:
        now = datetime.now(tz=UTC)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if now.hour >= 20:
            day_start = day_start + timedelta(days=1)
        day_end = day_start + timedelta(days=1)
        return day_start, day_end
