"""TTL-based singleton ownership leases for background node loops."""

from __future__ import annotations

import threading
import time
from typing import Any, Callable


class LeaseTable:
    """In-memory lease table ensuring only one instance owns a singleton role at a time.

    Why leases are intentionally non-persistent: if a host restarts, stale locks
    reset and active candidates race cleanly for ownership; if a holder crashes,
    its TTL expires and another peer takes over.
    """

    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._items: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._clock = clock

    def acquire(self, name: str, holder: str, ttl: float) -> bool:
        """Grant or renew a lease. Returns True if free, expired, or already held by `holder`."""
        if not name or not holder:
            return False
        # Floor only rejects zero/negative TTLs; never silently inflate a caller's short TTL.
        ttl_sec = max(float(ttl or 0.0), 0.01)
        now = float(self._clock())
        with self._lock:
            cur = self._items.get(name)
            if cur and cur["holder"] != holder and cur["expires"] > now:
                return False
            self._items[name] = {"holder": holder, "expires": now + ttl_sec}
            return True

    def release(self, name: str, holder: str) -> bool:
        """Release a lease; only the current holder may release it."""
        with self._lock:
            cur = self._items.get(name)
            if cur and cur["holder"] == holder:
                del self._items[name]
                return True
            return False

    def holder(self, name: str) -> str:
        """Return the current active holder or empty string if unheld/expired."""
        now = float(self._clock())
        with self._lock:
            cur = self._items.get(name)
            if not cur or cur["expires"] <= now:
                return ""
            return str(cur["holder"])

    def snapshot(self) -> dict[str, dict[str, Any]]:
        now = float(self._clock())
        with self._lock:
            return {
                name: {"holder": it["holder"], "expires_in": round(it["expires"] - now, 3)}
                for name, it in self._items.items()
                if it["expires"] > now
            }
