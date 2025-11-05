from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict
from urllib.parse import parse_qs, urlparse

from .service import ProcPlanService
from .timeutils import parse_iso_timestamp

LOGGER = logging.getLogger("procplan.server")
UTC = timezone.utc


class ProcPlanHTTPServer(ThreadingHTTPServer):
    def __init__(self, server_address, RequestHandlerClass, *, service: ProcPlanService, web_root: Path):
        self.service = service
        self.web_root = web_root
        super().__init__(server_address, RequestHandlerClass, bind_and_activate=True)


class ProcPlanHTTPRequestHandler(BaseHTTPRequestHandler):
    server_version = "ProcPlan/0.1"
    error_content_type = "application/json"

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        LOGGER.info("%s - %s", self.address_string(), format % args)

    @property
    def service(self) -> ProcPlanService:
        return self.server.service  # type: ignore[attr-defined]

    @property
    def web_root(self) -> Path:
        return self.server.web_root  # type: ignore[attr-defined]

    def _send_json(self, payload: Dict[str, Any], status: int = 200) -> None:
        data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_error(self, status: int, message: str) -> None:
        payload = {"error": message, "status": status}
        self._send_json(payload, status=status)

    def _serve_static(self, relative_path: str) -> None:
        if ".." in relative_path or relative_path.startswith("/"):
            self._send_error(HTTPStatus.FORBIDDEN, "Invalid path")
            return
        file_path = self.web_root / relative_path
        if file_path.is_dir():
            file_path = file_path / "index.html"
        if not file_path.exists():
            self._send_error(HTTPStatus.NOT_FOUND, "Not found")
            return
        mime = "text/plain"
        if file_path.suffix == ".html":
            mime = "text/html; charset=utf-8"
        elif file_path.suffix == ".js":
            mime = "application/javascript; charset=utf-8"
        elif file_path.suffix == ".css":
            mime = "text/css; charset=utf-8"

        data = file_path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/nodes":
            self._handle_list_nodes()
        elif parsed.path == "/api/availability":
            self._handle_availability(parsed.query)
        elif parsed.path in ("/", "/index.html"):
            self._serve_static("index.html")
        else:
            rel = parsed.path.lstrip("/")
            if not rel:
                rel = "index.html"
            self._serve_static(rel)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/book":
            self._handle_create_booking()
        elif parsed.path == "/api/mark_done":
            self._handle_mark_done()
        elif parsed.path == "/api/reload_config":
            self._handle_reload_config()
        else:
            self._send_error(HTTPStatus.NOT_FOUND, "Unknown endpoint")

    def do_DELETE(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path.startswith("/api/bookings/"):
            parts = parsed.path.rstrip("/").split("/")
            if len(parts) < 4 or not parts[3]:
                self._send_error(HTTPStatus.BAD_REQUEST, "Booking id is required")
                return
            try:
                booking_id = int(parts[3])
            except ValueError:
                self._send_error(HTTPStatus.BAD_REQUEST, "Booking id must be an integer")
                return
            self._handle_delete_booking(booking_id)
        else:
            self._send_error(HTTPStatus.NOT_FOUND, "Unknown endpoint")

    def _read_json_body(self) -> Dict[str, Any]:
        content_length = int(self.headers.get("Content-Length") or "0")
        if content_length <= 0:
            return {}
        raw = self.rfile.read(content_length)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON payload: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValueError("JSON payload must be an object")
        return payload

    def _handle_list_nodes(self) -> None:
        nodes = self.service.list_nodes()
        self._send_json({"nodes": nodes})

    def _handle_availability(self, query: str) -> None:
        params = parse_qs(query)
        node_id = params.get("node_id", [None])[0]
        if not node_id:
            self._send_error(HTTPStatus.BAD_REQUEST, "Query parameter 'node_id' is required")
            return
        start_raw = params.get("start", [None])[0]
        end_raw = params.get("end", [None])[0]
        try:
            if start_raw and end_raw:
                start = parse_iso_timestamp(start_raw)
                end = parse_iso_timestamp(end_raw)
            else:
                start, end = self.service.default_availability_window()
        except Exception as exc:
            self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
            return

        granularity = params.get("granularity", ["hour"])[0] or "hour"

        try:
            availability = self.service.compute_availability(
                node_id=node_id,
                start=start,
                end=end,
                granularity=granularity,
            )
        except ValueError as exc:
            self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
            return
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.exception("Failed to compute availability")
            self._send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(exc))
            return
        self._send_json(availability)

    def _handle_create_booking(self) -> None:
        try:
            payload = self._read_json_body()
        except ValueError as exc:
            self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
            return

        node_id = str(payload.get("node_id") or "")
        start_raw = payload.get("start")
        end_raw = payload.get("end")
        if not node_id or not start_raw or not end_raw:
            self._send_error(HTTPStatus.BAD_REQUEST, "Fields 'node_id', 'start', and 'end' are required")
            return

        try:
            start = parse_iso_timestamp(start_raw)
            end = parse_iso_timestamp(end_raw)
        except Exception as exc:
            self._send_error(HTTPStatus.BAD_REQUEST, f"Invalid timestamp: {exc}")
            return

        user_label = str(payload.get("user_label") or "").strip()
        gpu_ids = payload.get("gpu_ids")
        gpu_count = payload.get("gpu_count")
        priority = payload.get("priority")
        if gpu_ids is not None and not isinstance(gpu_ids, list):
            self._send_error(HTTPStatus.BAD_REQUEST, "'gpu_ids' must be an array when provided")
            return
        if gpu_ids is not None:
            gpu_ids = [str(item) for item in gpu_ids]
        if gpu_count is not None:
            try:
                gpu_count = int(gpu_count)
            except (TypeError, ValueError):
                self._send_error(HTTPStatus.BAD_REQUEST, "'gpu_count' must be an integer")
                return

        try:
            result = self.service.create_booking(
                node_id=node_id,
                start=start,
                end=end,
                user_label=user_label,
                gpu_ids=gpu_ids,
                gpu_count=gpu_count,
                priority=str(priority) if priority is not None else None,
            )
        except ValueError as exc:
            self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
            return
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.exception("Failed to create booking")
            self._send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(exc))
            return

        self._send_json(result, status=HTTPStatus.CREATED)

    def _handle_mark_done(self) -> None:
        try:
            payload = self._read_json_body()
        except ValueError as exc:
            self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
            return

        booking_id = payload.get("booking_id")
        if booking_id is None:
            self._send_error(HTTPStatus.BAD_REQUEST, "'booking_id' is required")
            return

        try:
            success = self.service.mark_booking_complete(int(booking_id))
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.exception("Failed to mark booking as complete")
            self._send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(exc))
            return

        if not success:
            self._send_error(HTTPStatus.NOT_FOUND, f"Booking '{booking_id}' is not active or does not exist")
            return

        self._send_json({"booking_id": int(booking_id), "status": "completed"})

    def _handle_delete_booking(self, booking_id: int) -> None:
        try:
            success = self.service.cancel_booking(booking_id)
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.exception("Failed to cancel booking")
            self._send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(exc))
            return

        if not success:
            self._send_error(HTTPStatus.NOT_FOUND, f"Booking '{booking_id}' is not active or does not exist")
            return

        self._send_json({"booking_id": booking_id, "status": "cancelled"})

    def _handle_reload_config(self) -> None:
        try:
            self.service.reload_config()
        except Exception as exc:
            self._send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(exc))
            return
        self._send_json({"status": "ok"})


def run_server(args: argparse.Namespace) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    service = ProcPlanService(args.config, args.database)
    web_root = Path(args.web_root).resolve()
    if not web_root.exists():
        raise FileNotFoundError(f"Web root '{web_root}' does not exist")

    with ProcPlanHTTPServer(
        (args.host, args.port),
        ProcPlanHTTPRequestHandler,
        service=service,
        web_root=web_root,
    ) as httpd:
        LOGGER.info("Serving on http://%s:%s", args.host, args.port)
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            LOGGER.info("Shutting down")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ProcPlan resource reservation server")
    parser.add_argument("--config", required=True, help="Path to the nodes/GPU configuration JSON")
    parser.add_argument("--database", required=True, help="Path to the sqlite database file")
    parser.add_argument("--web-root", default=str(Path(__file__).parent / "web"), help="Path to directory with web assets")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind the HTTP server to")
    parser.add_argument("--port", type=int, default=8080, help="Port to bind the HTTP server to")
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    run_server(args)


if __name__ == "__main__":  # pragma: no cover
    main()
