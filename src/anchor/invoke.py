"""Transport-independent node invokers for Anchor."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Callable, Protocol


class Invoker(Protocol):
    """Transport contract used by Dispatcher to wake a target node."""

    def invoke(self, node: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Wake `node` with `payload` and return a structured result dict."""
        ...


class LocalInvoker:
    """In-process invoker mapping node names to Python callables."""

    def __init__(self, handlers: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] | None = None) -> None:
        self._handlers: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = dict(handlers or {})

    def register(self, node: str, fn: Callable[[dict[str, Any]], dict[str, Any]]) -> None:
        if not node or not callable(fn):
            raise ValueError("node name and callable handler are required")
        self._handlers[node] = fn

    def invoke(self, node: str, payload: dict[str, Any]) -> dict[str, Any]:
        fn = self._handlers.get(node)
        if fn is None:
            raise LookupError(f"no handler registered for node '{node}'")
        res = fn(payload)
        return res if isinstance(res, dict) else {"result": res}


class HttpInvoker:
    """Stdlib urllib HTTP invoker mapping node names to base URLs."""

    def __init__(
        self,
        endpoints: dict[str, str] | None = None,
        *,
        path: str = "/anchor/invoke",
        timeout: float = 30.0,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._endpoints: dict[str, str] = {k: v.rstrip("/") for k, v in (endpoints or {}).items()}
        self.path = path if path.startswith("/") else f"/{path}"
        self.timeout = float(timeout)
        self.headers = dict(headers or {})

    def register(self, node: str, base_url: str) -> None:
        if not node or not base_url:
            raise ValueError("node name and base_url are required")
        self._endpoints[node] = base_url.rstrip("/")

    def invoke(self, node: str, payload: dict[str, Any]) -> dict[str, Any]:
        base = self._endpoints.get(node)
        if not base:
            raise LookupError(f"no HTTP endpoint registered for node '{node}'")
        url = f"{base}{self.path}"
        body = json.dumps({"node": node, "payload": payload}).encode("utf-8")
        req_headers = {"Content-Type": "application/json", **self.headers}
        req = urllib.request.Request(url, data=body, headers=req_headers, method="POST")
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            raw = resp.read().decode("utf-8")
            if not raw:
                return {"ok": True}
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {"result": parsed}
