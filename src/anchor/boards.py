"""Registered blackboard store with pending-safe pruning and atomic persistence."""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any, Callable

from .storage import MemoryStorage, Storage

DEFAULT_PENDING_STATUSES = frozenset({
    "blocked",
    "assigned",
    "approved",
    "planned",
    "processing",
    "new",
})


class BoardError(ValueError):
    """Raised on invalid or unregistered board operations."""


def _prune(items: list[dict[str, Any]], limit: int, pending_statuses: frozenset[str]) -> list[dict[str, Any]]:
    """Evict oldest terminal entries above `limit` while never dropping pending entries.

    Why: a card waiting on an upstream dependency (`blocked`) or awaiting a reply can sit
    on a busy board while hundreds of newer entries arrive; naive tail-slicing silently
    drops the waiting card so downstream work never starts.
    """
    excess = len(items) - limit
    if excess <= 0:
        return items
    drop_ids: set[int] = set()
    for it in items:
        if len(drop_ids) >= excess:
            break
        if it.get("status") not in pending_statuses:
            drop_ids.add(id(it))
    return [it for it in items if id(it) not in drop_ids]


class BoardStore:
    """Thread-safe, registered blackboard store."""

    def __init__(
        self,
        storage: Storage | None = None,
        *,
        boards: list[str] | tuple[str, ...] | set[str] | None = None,
        max_entries_per_board: int = 200,
        pending_statuses: set[str] | frozenset[str] | tuple[str, ...] | None = None,
        strict: bool = True,
        clock: Callable[[], float] = time.time,
        logger: Callable[..., None] | None = None,
    ) -> None:
        self._storage: Storage = storage if storage is not None else MemoryStorage()
        self.max_entries_per_board = int(max_entries_per_board)
        self.pending_statuses = frozenset(
            pending_statuses if pending_statuses is not None else DEFAULT_PENDING_STATUSES
        )
        self.strict = bool(strict)
        self._clock = clock
        self._logger = logger
        self._lock = threading.RLock()
        self._boards: dict[str, list[dict[str, Any]]] = {}

        for name in boards or ():
            self._boards[str(name)] = []
        self._load()

    def register(self, *names: str) -> None:
        """Register one or more board names."""
        with self._lock:
            for name in names:
                key = str(name or "").strip()
                if not key:
                    raise BoardError("board name cannot be empty")
                self._boards.setdefault(key, [])
            self._save()

    def registered_boards(self) -> set[str]:
        with self._lock:
            return set(self._boards.keys())

    def _load(self) -> None:
        data = self._storage.load()
        if not isinstance(data, dict):
            return
        with self._lock:
            for name, items in data.items():
                if isinstance(items, list):
                    if self.strict and self._boards and name not in self._boards:
                        continue
                    self._boards[name] = list(items)

    def _save(self) -> None:
        self._storage.save(self._boards)

    def _ensure_board(self, board_name: str) -> str:
        if board_name in self._boards:
            return board_name
        if self.strict:
            raise BoardError(f"unregistered board '{board_name}'")
        self._boards[board_name] = []
        return board_name

    def post_entry(self, board_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Append a new entry to `board_name` and persist atomically."""
        with self._lock:
            board_name = self._ensure_board(board_name)
            author = str(payload.get("author") or "unknown")
            data_field = payload.get("data") if isinstance(payload.get("data"), dict) else payload

            topic = payload.get("topic")
            if not topic:
                topic = (
                    data_field.get("topic")
                    or data_field.get("goal")
                    or data_field.get("title")
                    or data_field.get("description")
                    or "untitled"
                )
            topic = str(topic)[:80]

            now = float(self._clock())
            entry: dict[str, Any] = {
                "id": str(payload.get("id") or uuid.uuid4().hex[:8]),
                "task_id": str(payload.get("task_id") or ""),
                "author": author,
                "status": str(payload.get("status") or "new"),
                "topic": topic,
                "board": board_name,
                "data": dict(data_field) if isinstance(data_field, dict) else data_field,
                "created": now,
                "updated": now,
            }

            for addr_key in ("to", "cc", "reply_to"):
                val = payload.get(addr_key)
                if val is None and isinstance(data_field, dict):
                    val = data_field.get(addr_key)
                if val is not None:
                    entry[addr_key] = val

            items = self._boards[board_name]
            items.append(entry)
            if len(items) > self.max_entries_per_board:
                self._boards[board_name] = _prune(items, self.max_entries_per_board, self.pending_statuses)
            self._save()
            return dict(entry)

    def update_entry(self, board_name: str, entry_id: str, **patch: Any) -> dict[str, Any] | None:
        with self._lock:
            if board_name not in self._boards:
                if self.strict:
                    raise BoardError(f"unregistered board '{board_name}'")
                return None
            for item in self._boards[board_name]:
                if item.get("id") == entry_id:
                    for k, v in patch.items():
                        if k in ("id", "created"):
                            continue
                        if v is not None:
                            item[k] = v
                    item["updated"] = float(self._clock())
                    self._save()
                    return dict(item)
            return None

    def get_entry(self, board_name: str, entry_id: str) -> dict[str, Any] | None:
        with self._lock:
            if board_name not in self._boards:
                if self.strict:
                    raise BoardError(f"unregistered board '{board_name}'")
                return None
            for item in self._boards[board_name]:
                if item.get("id") == entry_id:
                    return dict(item)
            return None

    def list_entries(
        self,
        board_name: str | None = None,
        status: str | list[str] | tuple[str, ...] | set[str] | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        with self._lock:
            if board_name and board_name not in ("all", "*"):
                if board_name not in self._boards:
                    if self.strict:
                        raise BoardError(f"unregistered board '{board_name}'")
                    return []
                items = [dict(it, board=it.get("board", board_name)) for it in self._boards[board_name]]
            else:
                items = []
                for b_name, b_items in self._boards.items():
                    for item in b_items:
                        it = dict(item)
                        it.setdefault("board", b_name)
                        items.append(it)

            if status:
                wanted = {status} if isinstance(status, str) else set(status)
                items = [t for t in items if t.get("status") in wanted]

            return sorted(items, key=lambda x: x.get("updated", 0), reverse=True)[:limit]

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {b: len(items) for b, items in self._boards.items()}
