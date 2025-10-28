from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Sequence

from .config import Config, Node

UTC = timezone.utc


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


@dataclass
class BookingAllocation:
    booking_id: int
    gpu_id: str


class Database:
    """Thin wrapper around sqlite3 providing schema management helpers."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._lock = threading.RLock()
        self._ensure_directory()
        self._ensure_schema()

    def _ensure_directory(self) -> None:
        if self.path.parent and not self.path.parent.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)

    def _ensure_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA foreign_keys = ON;
                CREATE TABLE IF NOT EXISTS nodes (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS gpus (
                    id TEXT PRIMARY KEY,
                    node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
                    kind TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS bookings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT NOT NULL REFERENCES nodes(id),
                    start_utc TEXT NOT NULL,
                    end_utc TEXT NOT NULL,
                    user_label TEXT NOT NULL,
                    priority TEXT NOT NULL DEFAULT 'medium',
                    status TEXT NOT NULL DEFAULT 'active',
                    created_utc TEXT NOT NULL,
                    updated_utc TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS booking_allocations (
                    booking_id INTEGER NOT NULL REFERENCES bookings(id) ON DELETE CASCADE,
                    gpu_id TEXT NOT NULL REFERENCES gpus(id),
                    PRIMARY KEY (booking_id, gpu_id)
                );
                """
            )
            columns = conn.execute("PRAGMA table_info(bookings)").fetchall()
            column_names = {row["name"] for row in columns}
            if "priority" not in column_names:
                conn.execute("ALTER TABLE bookings ADD COLUMN priority TEXT NOT NULL DEFAULT 'medium'")

    @contextmanager
    def connect(self) -> Iterable[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def sync_from_config(self, config: Config) -> None:
        with self._lock, self.connect() as conn:
            existing_node_ids = {row["id"] for row in conn.execute("SELECT id FROM nodes")}
            config_node_ids = {node.id for node in config.nodes}

            # Remove stale nodes
            stale_ids = existing_node_ids - config_node_ids
            if stale_ids:
                conn.executemany("DELETE FROM nodes WHERE id = ?", ((node_id,) for node_id in stale_ids))

            for node in config.nodes:
                conn.execute(
                    """
                    INSERT INTO nodes (id, name)
                    VALUES (?, ?)
                    ON CONFLICT(id) DO UPDATE SET name=excluded.name
                    """,
                    (node.id, node.name),
                )

                existing_gpu_ids = {
                    row["id"] for row in conn.execute("SELECT id FROM gpus WHERE node_id = ?", (node.id,))
                }
                config_gpu_ids = {gpu.id for gpu in node.gpus}

                stale_gpu_ids = existing_gpu_ids - config_gpu_ids
                if stale_gpu_ids:
                    conn.executemany(
                        "DELETE FROM gpus WHERE id = ?",
                        ((gpu_id,) for gpu_id in stale_gpu_ids),
                    )

                for gpu in node.gpus:
                    conn.execute(
                        """
                        INSERT INTO gpus (id, node_id, kind)
                        VALUES (?, ?, ?)
                        ON CONFLICT(id) DO UPDATE SET node_id=excluded.node_id, kind=excluded.kind
                        """,
                        (gpu.id, node.id, gpu.kind),
                    )

    def fetch_node(self, node_id: str) -> Node | None:
        from .config import Node, GPU

        with self.connect() as conn:
            node_row = conn.execute("SELECT id, name FROM nodes WHERE id = ?", (node_id,)).fetchone()
            if not node_row:
                return None
            gpu_rows = conn.execute(
                "SELECT id, kind FROM gpus WHERE node_id = ? ORDER BY id",
                (node_id,),
            ).fetchall()
        gpus = [GPU(id=row["id"], kind=row["kind"]) for row in gpu_rows]
        return Node(id=node_row["id"], name=node_row["name"], gpus=gpus)

    def list_nodes(self) -> List[Node]:
        from .config import Node, GPU

        with self.connect() as conn:
            node_rows = conn.execute("SELECT id, name FROM nodes ORDER BY id").fetchall()
            gpu_rows = conn.execute("SELECT id, node_id, kind FROM gpus ORDER BY node_id, id").fetchall()

        gpus_by_node: dict[str, List[GPU]] = {}
        for row in gpu_rows:
            gpus_by_node.setdefault(row["node_id"], []).append(
                GPU(id=row["id"], kind=row["kind"])
            )

        nodes: List[Node] = []
        for node_row in node_rows:
            nodes.append(
                Node(
                    id=node_row["id"],
                    name=node_row["name"],
                    gpus=gpus_by_node.get(node_row["id"], []),
                )
            )
        return nodes

    def create_booking(
        self,
        *,
        node_id: str,
        gpu_ids: Sequence[str],
        start_utc: datetime,
        end_utc: datetime,
        user_label: str,
        priority: str,
    ) -> int:
        if not gpu_ids:
            raise ValueError("At least one GPU id must be provided when creating a booking")
        if start_utc >= end_utc:
            raise ValueError("End time must be after start time")

        start_iso = start_utc.replace(tzinfo=UTC).isoformat()
        end_iso = end_utc.replace(tzinfo=UTC).isoformat()
        now_iso = utc_now().isoformat()

        with self._lock, self.connect() as conn:
            # Ensure GPUs belong to node
            placeholders = ",".join("?" for _ in gpu_ids)
            gpu_rows = conn.execute(
                f"SELECT id FROM gpus WHERE node_id = ? AND id IN ({placeholders})",
                (node_id, *gpu_ids),
            ).fetchall()
            if len(gpu_rows) != len(gpu_ids):
                raise ValueError("One or more GPUs do not belong to the specified node")

            # Check for overlapping bookings
            overlap_rows = conn.execute(
                f"""
                SELECT ba.gpu_id
                FROM booking_allocations ba
                JOIN bookings b ON b.id = ba.booking_id
                WHERE b.node_id = ?
                  AND ba.gpu_id IN ({placeholders})
                  AND b.status = 'active'
                  AND NOT (? >= b.end_utc OR ? <= b.start_utc)
                """,
                (node_id, *gpu_ids, start_iso, end_iso),
            ).fetchall()
            if overlap_rows:
                conflict_gpu = overlap_rows[0]["gpu_id"]
                raise ValueError(f"GPU '{conflict_gpu}' is already booked for the requested window")

            cur = conn.execute(
                """
                INSERT INTO bookings (node_id, start_utc, end_utc, user_label, priority, status, created_utc, updated_utc)
                VALUES (?, ?, ?, ?, ?, 'active', ?, ?)
                """,
                (node_id, start_iso, end_iso, user_label, priority, now_iso, now_iso),
            )
            booking_id = cur.lastrowid

            conn.executemany(
                "INSERT INTO booking_allocations (booking_id, gpu_id) VALUES (?, ?)",
                ((booking_id, gpu_id) for gpu_id in gpu_ids),
            )

            return int(booking_id)

    def list_allocated_gpu_ids(
        self,
        *,
        node_id: str,
        start_utc: datetime,
        end_utc: datetime,
    ) -> List[str]:
        start_iso = start_utc.replace(tzinfo=UTC).isoformat()
        end_iso = end_utc.replace(tzinfo=UTC).isoformat()

        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT ba.gpu_id
                FROM booking_allocations ba
                JOIN bookings b ON b.id = ba.booking_id
                WHERE b.node_id = ?
                  AND b.status = 'active'
                  AND NOT (? >= b.end_utc OR ? <= b.start_utc)
                """,
                (node_id, start_iso, end_iso),
            ).fetchall()
        return [row["gpu_id"] for row in rows]

    def select_available_gpus(
        self,
        *,
        node_id: str,
        start_utc: datetime,
        end_utc: datetime,
        count: int,
    ) -> List[str]:
        if count <= 0:
            return []

        start_iso = start_utc.replace(tzinfo=UTC).isoformat()
        end_iso = end_utc.replace(tzinfo=UTC).isoformat()

        with self.connect() as conn:
            allocated = conn.execute(
                """
                SELECT DISTINCT ba.gpu_id
                FROM booking_allocations ba
                JOIN bookings b ON b.id = ba.booking_id
                WHERE b.node_id = ?
                  AND b.status = 'active'
                  AND NOT (? >= b.end_utc OR ? <= b.start_utc)
                """,
                (node_id, start_iso, end_iso),
            ).fetchall()
            allocated_ids = {row["gpu_id"] for row in allocated}

            gpu_rows = conn.execute(
                "SELECT id FROM gpus WHERE node_id = ? ORDER BY id",
                (node_id,),
            ).fetchall()

        available = [row["id"] for row in gpu_rows if row["id"] not in allocated_ids]
        if len(available) < count:
            return []
        return available[:count]

    def mark_booking_done(self, booking_id: int) -> bool:
        now_iso = utc_now().isoformat()
        with self._lock, self.connect() as conn:
            cur = conn.execute(
                """
                UPDATE bookings
                SET status = 'completed',
                    updated_utc = ?
                WHERE id = ? AND status = 'active'
                """,
                (now_iso, booking_id),
            )
            return cur.rowcount > 0

    def list_bookings_for_window(
        self,
        *,
        node_id: str,
        start_utc: datetime,
        end_utc: datetime,
    ) -> List[sqlite3.Row]:
        start_iso = start_utc.replace(tzinfo=UTC).isoformat()
        end_iso = end_utc.replace(tzinfo=UTC).isoformat()

        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT b.*, group_concat(ba.gpu_id, ',') AS gpu_ids
                FROM bookings b
                JOIN booking_allocations ba ON ba.booking_id = b.id
                WHERE b.node_id = ?
                  AND NOT (? >= b.end_utc OR ? <= b.start_utc)
                GROUP BY b.id
                ORDER BY b.start_utc
                """,
                (node_id, start_iso, end_iso),
            ).fetchall()
        return list(rows)
