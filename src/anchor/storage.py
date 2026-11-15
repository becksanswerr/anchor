"""Thread-safe persistence backends for Anchor (stdlib-only)."""

from __future__ import annotations

import copy
import json
import os
import tempfile
import threading
from typing import Any, Protocol


class Storage(Protocol):
    """Persistence contract for Anchor stores."""

    def load(self) -> Any:
        """Load persisted state, or return None if no state exists yet."""
        ...

    def save(self, state: Any) -> None:
        """Persist state atomically."""
        ...


class MemoryStorage:
    """In-memory storage for tests and ephemeral single-process runs."""

    def __init__(self, initial: Any = None) -> None:
        self._lock = threading.RLock()
        self._state = copy.deepcopy(initial)

    def load(self) -> Any:
        with self._lock:
            return copy.deepcopy(self._state)

    def save(self, state: Any) -> None:
        with self._lock:
            self._state = copy.deepcopy(state)


class JsonFileStorage:
    """Thread-safe atomic JSON file storage using write-temp + os.replace."""

    def __init__(self, path: str) -> None:
        self.path = os.fspath(path)
        self._lock = threading.RLock()

    def load(self) -> Any:
        with self._lock:
            if not os.path.exists(self.path):
                return None
            with open(self.path, encoding="utf-8-sig") as f:
                return json.load(f)

    def save(self, state: Any) -> None:
        with self._lock:
            parent = os.path.dirname(os.path.abspath(self.path))
            os.makedirs(parent, exist_ok=True)
            fd, tmp_path = tempfile.mkstemp(prefix=".anchor-", suffix=".tmp", dir=parent)
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(state, f, ensure_ascii=False, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, self.path)
            except Exception:
                try:
                    if os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                except OSError:
                    pass
                raise
