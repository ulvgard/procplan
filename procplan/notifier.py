from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
import argparse
import sys


@dataclass
class NotifyResult:
    ok: bool
    status: int
    message: str = ""


def signal_completion(
    base_url: str,
    booking_id: int,
    timeout: float = 5.0,
) -> NotifyResult:
    """Notify the ProcPlan server that a booking finished early.

    Args:
        base_url: Base URL for the ProcPlan server, e.g. ``http://localhost:8080``.
        booking_id: Identifier returned when the booking was created.
        timeout: Request timeout in seconds.

    Returns:
        NotifyResult describing whether the request was accepted.
    """
    url = base_url.rstrip("/") + "/api/mark_done"
    payload = {"booking_id": int(booking_id)}
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = response.status
            if 200 <= status < 300:
                return NotifyResult(ok=True, status=status, message="completed")
            body = response.read().decode("utf-8")
            return NotifyResult(ok=False, status=status, message=body)
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read().decode("utf-8")
        except Exception:  # pragma: no cover - best effort
            body = exc.reason  # type: ignore[attr-defined]
        return NotifyResult(ok=False, status=exc.code, message=body)
    except urllib.error.URLError as exc:
        return NotifyResult(ok=False, status=0, message=str(exc.reason))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Notify ProcPlan that a booking finished early.")
    parser.add_argument("--url", required=True, help="Base URL of the ProcPlan server, e.g. http://localhost:8080")
    parser.add_argument("--booking-id", required=True, type=int, help="Booking identifier to mark as complete")
    parser.add_argument("--timeout", type=float, default=5.0, help="Request timeout in seconds (default: 5)")
    args = parser.parse_args(argv)

    result = signal_completion(args.url, args.booking_id, timeout=args.timeout)
    if result.ok:
        print(f"Booking {args.booking_id} marked complete (status {result.status}).")
        return 0
    print(f"Failed to mark booking complete (status {result.status}): {result.message}", file=sys.stderr)
    return 1
