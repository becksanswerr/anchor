"""Order dispatcher: route validation, delayed/instant rings, timeouts, and restart recovery."""

from __future__ import annotations

import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable

from .boards import BoardStore
from .invoke import Invoker
from .storage import MemoryStorage, Storage
from .tasks import TaskBoard


TERMINAL_ORDER_STATUSES = frozenset({"done", "failed", "cancelled", "timed_out"})


class RouteError(ValueError):
    """Raised when a ring violates the dispatcher's route table."""


@dataclass(frozen=True)
class RouteRule:
    """Routing rule for a board: allowed entry statuses, allowed target nodes, and complaint owner."""

    allowed_statuses: frozenset[str]
    allowed_nodes: tuple[str, ...]
    owner_board: str | None = None

    @classmethod
    def coerce(cls, value: RouteRule | tuple[Any, ...]) -> RouteRule:
        if isinstance(value, cls):
            return value
        if isinstance(value, tuple) and len(value) >= 2:
            statuses_raw, nodes_raw = value[0], value[1]
            owner = value[2] if len(value) > 2 else None
            statuses = (
                frozenset({statuses_raw})
                if isinstance(statuses_raw, str)
                else frozenset(str(s) for s in statuses_raw)
            )
            nodes = (nodes_raw,) if isinstance(nodes_raw, str) else tuple(str(n) for n in nodes_raw)
            return cls(allowed_statuses=statuses, allowed_nodes=nodes, owner_board=owner)
        raise TypeError(f"invalid route rule: {value!r}")


class Dispatcher:
    """Coordinates node wake-ups via persistent orders and a route allowlist."""

    def __init__(
        self,
        boards: BoardStore,
        invoker: Invoker,
        *,
        routes: dict[str, RouteRule | tuple[Any, ...]] | None = None,
        storage: Storage | None = None,
        tasks: TaskBoard | None = None,
        max_resumes: int = 3,
        max_finished_orders: int = 500,
        clock: Callable[[], float] = time.time,
        logger: Callable[..., None] | None = None,
    ) -> None:
        self.boards = boards
        self.invoker = invoker
        self.routes: dict[str, RouteRule] = {
            k: RouteRule.coerce(v) for k, v in (routes or {}).items()
        }
        self._storage: Storage = storage if storage is not None else MemoryStorage([])
        self.tasks = tasks
        self.max_resumes = int(max_resumes)
        self.max_finished_orders = int(max_finished_orders)
        self._clock = clock
        self._pool: ThreadPoolExecutor | None = None
        self._runner: threading.Thread | None = None
        self._stop = threading.Event()
        self._logger = logger
        self._lock = threading.RLock()
        self._orders: list[dict[str, Any]] = []
        self._load()
        self._recover_orders()

    def _log(self, msg: str, level: str = "info") -> None:
        if callable(self._logger):
            try:
                self._logger(msg, level=level, source="anchor.dispatcher")
            except TypeError:
                self._logger(msg)

    def _load(self) -> None:
        data = self._storage.load()
        if isinstance(data, list):
            with self._lock:
                self._orders = [dict(o) for o in data if isinstance(o, dict)]

    def _save(self) -> None:
        # Why prune here: every order used to stay in the file forever. Only FINISHED orders are
        # dropped (oldest first); queued/scheduled/running orders are what restart recovery needs.
        finished = [o for o in self._orders if o.get("status") in TERMINAL_ORDER_STATUSES]
        excess = len(finished) - self.max_finished_orders
        if excess > 0:
            drop = {id(o) for o in sorted(finished, key=lambda o: float(o.get("updated_at") or 0.0))[:excess]}
            self._orders = [o for o in self._orders if id(o) not in drop]
        self._storage.save(self._orders)

    def _write_complaint(self, order: dict[str, Any], reason: str, message: str) -> None:
        rule = self.routes.get(order.get("board", ""))
        owner_board = rule.owner_board if rule else None
        if not owner_board:
            return
        try:
            # Why status="complaint": complaints notify the owner board without matching any
            # dispatch route status, preventing infinite self-retrigger loops.
            self.boards.post_entry(
                owner_board,
                {
                    "task_id": order.get("task_id", ""),
                    "author": "anchor.dispatcher",
                    "status": "complaint",
                    "topic": f"{reason}: {order.get('target_node', '')}",
                    "data": {
                        "issue": reason,
                        "order_id": order.get("order_id"),
                        "board": order.get("board"),
                        "target_node": order.get("target_node"),
                        "message": message,
                    },
                },
            )
        except Exception as exc:
            self._log(f"failed to write complaint to '{owner_board}': {exc}", level="error")

    def _recover_orders(self) -> None:
        """Recover orders left queued or running across a process restart.

        Why expired running orders are NOT re-run: re-dispatching an order whose timeout
        already elapsed before restart would replay stale work as if it were fresh.
        Instead they transition to `timed_out` with an owner complaint.
        """
        now = float(self._clock())
        changed = False
        with self._lock:
            for order in self._orders:
                st = order.get("status")
                if st == "running":
                    dispatched_at = float(order.get("dispatched_at") or 0.0)
                    timeout_sec = float(order.get("timeout_seconds") or 0.0)
                    due_at = float(order.get("due_at") or 0.0)
                    expired = (due_at > 0 and now > due_at) or (
                        dispatched_at > 0 and timeout_sec > 0 and (now - dispatched_at) > timeout_sec
                    )
                    if expired:
                        order["status"] = "timed_out"
                        order["updated_at"] = now
                        changed = True
                        self._write_complaint(
                            order,
                            "timeout",
                            f"order {order.get('order_id')} expired before restart",
                        )
                        continue
                    resumes = int(order.get("resume_count") or 0) + 1
                    order["resume_count"] = resumes
                    if resumes > self.max_resumes:
                        order["status"] = "failed"
                        order["error"] = "max_resumes exceeded"
                        order["updated_at"] = now
                        changed = True
                        self._write_complaint(
                            order,
                            "max_resumes",
                            f"order {order.get('order_id')} exceeded max_resumes ({self.max_resumes})",
                        )
                    else:
                        order["status"] = "queued"
                        order["dispatched_at"] = 0.0
                        order["due_at"] = 0.0
                        order["updated_at"] = now
                        changed = True
            if changed:
                self._save()

    def ring(
        self,
        *,
        author: str,
        board: str,
        entry_id: str = "",
        target_node: str = "",
        task_id: str = "",
        delay_seconds: float = 0.0,
        timeout_seconds: float = 1800.0,
        topic: str | None = None,
        to: Any = None,
        cc: Any = None,
    ) -> dict[str, Any]:
        """Validate against `routes` and enqueue an order; `tick()` performs the actual invoke."""
        rule = self.routes.get(board)
        if rule is None:
            raise RouteError(f"no route configured for board '{board}'")

        entry = self.boards.get_entry(board, entry_id) if entry_id else None
        if entry_id and entry is None:
            raise RouteError(f"entry '{entry_id}' not found on board '{board}'")
        if entry is not None and entry.get("status") not in rule.allowed_statuses:
            raise RouteError(
                f"entry status '{entry.get('status')}' is not dispatchable on board '{board}'"
            )

        resolved_node = target_node or (rule.allowed_nodes[0] if rule.allowed_nodes else "")
        if not resolved_node or resolved_node not in rule.allowed_nodes:
            raise RouteError(
                f"target_node '{resolved_node}' is not allowed for board '{board}' "
                f"(allowed: {list(rule.allowed_nodes)})"
            )

        now = float(self._clock())
        delay = max(0.0, float(delay_seconds or 0.0))
        order: dict[str, Any] = {
            "order_id": uuid.uuid4().hex[:8],
            "task_id": str(task_id or (entry.get("task_id") if entry else "") or ""),
            "author": str(author or "unknown"),
            "target_node": resolved_node,
            "board": board,
            "entry_id": str(entry_id or ""),
            "topic": topic or (entry.get("topic") if entry else ""),
            "to": to,
            "cc": cc,
            "status": "scheduled" if delay > 0 else "queued",
            "created_at": now,
            "run_at": now + delay,
            "timeout_seconds": float(timeout_seconds or 0.0),
            "dispatched_at": 0.0,
            "due_at": 0.0,
            "resume_count": 0,
            "updated_at": now,
        }
        with self._lock:
            self._orders.append(order)
            self._save()
            return dict(order)

    def _is_task_suspended(self, task_id: str) -> bool:
        if not task_id or self.tasks is None:
            return False
        task = self.tasks.get(task_id)
        return bool(task and task.get("status") == "needs_input")

    def tick(self, now: float | None = None) -> dict[str, list[str]]:
        """Promote due scheduled orders, flag timed-out running orders, and invoke queued orders."""
        cur_now = float(self._clock() if now is None else now)
        promoted: list[str] = []
        timed_out_orders: list[dict[str, Any]] = []
        to_dispatch: list[dict[str, Any]] = []
        dispatched: list[str] = []

        with self._lock:
            changed = False
            for order in self._orders:
                st = order.get("status")
                if st == "scheduled" and float(order.get("run_at") or 0.0) <= cur_now:
                    order["status"] = "queued"
                    order["updated_at"] = cur_now
                    promoted.append(order["order_id"])
                    changed = True
                elif st == "running":
                    due_at = float(order.get("due_at") or 0.0)
                    if due_at > 0 and cur_now > due_at and not self._is_task_suspended(order.get("task_id", "")):
                        order["status"] = "timed_out"
                        order["updated_at"] = cur_now
                        timed_out_orders.append(dict(order))
                        changed = True

            for order in self._orders:
                if order.get("status") == "queued":
                    timeout_sec = float(order.get("timeout_seconds") or 0.0)
                    order["status"] = "running"
                    order["dispatched_at"] = cur_now
                    order["due_at"] = (cur_now + timeout_sec) if timeout_sec > 0 else 0.0
                    order["updated_at"] = cur_now
                    to_dispatch.append(dict(order))
                    dispatched.append(order["order_id"])
                    changed = True

            if changed:
                self._save()

        for order in timed_out_orders:
            self._write_complaint(
                order,
                "timeout",
                f"order {order['order_id']} ({order['target_node']}) timed out",
            )

        for order in to_dispatch:
            if self._pool is not None:
                # Why a pool when running in the background: one slow node must not hold up the
                # wake-up of every other node queued in the same tick.
                self._pool.submit(self._invoke_order, order)
            else:
                self._invoke_order(order)

        return {
            "promoted": promoted,
            "timed_out": [o["order_id"] for o in timed_out_orders],
            "dispatched": dispatched,
        }

    def _invoke_order(self, order: dict[str, Any]) -> None:
        entry = (
            self.boards.get_entry(order["board"], order["entry_id"])
            if order.get("entry_id")
            else None
        )
        payload = {
            "order_id": order["order_id"],
            "task_id": order["task_id"],
            "board": order["board"],
            "entry_id": order["entry_id"],
            "entry": entry,
            "topic": order.get("topic", ""),
        }
        try:
            res = self.invoker.invoke(order["target_node"], payload)
            deferred = isinstance(res, dict) and (res.get("async") or res.get("status") == "running")
            if not deferred:
                self.complete_order(order["order_id"], status="done")
        except Exception as exc:
            self._log(f"invoke failed for {order['order_id']}: {exc}", level="error")
            self.complete_order(order["order_id"], status="failed")

    def start(self, interval: float = 1.0, max_workers: int = 4) -> "Dispatcher":
        """Run `tick()` in a background thread; node invocations run in a worker pool.

        Why this exists: ringing only enqueues an order. Without something calling `tick()`
        nothing is ever woken — work sits on the board and every order eventually times out.
        Tests and single-threaded apps can keep calling `tick()` themselves instead.
        """
        if self._runner is not None and self._runner.is_alive():
            return self
        self._stop.clear()
        self._pool = ThreadPoolExecutor(max_workers=max(1, int(max_workers)), thread_name_prefix="anchor-invoke")

        def _loop() -> None:
            while not self._stop.is_set():
                try:
                    self.tick()
                except Exception as exc:  # the loop must survive a bad tick
                    self._log(f"tick failed: {exc}", level="error")
                self._stop.wait(interval)

        self._runner = threading.Thread(target=_loop, name="anchor-dispatcher", daemon=True)
        self._runner.start()
        return self

    def stop(self, wait: bool = True) -> None:
        self._stop.set()
        if self._runner is not None:
            self._runner.join(timeout=5.0 if wait else 0.0)
            self._runner = None
        if self._pool is not None:
            self._pool.shutdown(wait=wait)
            self._pool = None

    def complete_order(self, order_id: str, status: str = "done") -> dict[str, Any] | None:
        now = float(self._clock())
        with self._lock:
            for order in self._orders:
                if order.get("order_id") == order_id:
                    if order.get("status") in ("done", "failed", "cancelled", "timed_out"):
                        return dict(order)
                    order["status"] = status
                    order["updated_at"] = now
                    self._save()
                    return dict(order)
        return None

    def cancel_order(self, order_id: str) -> dict[str, Any] | None:
        return self.complete_order(order_id, status="cancelled")

    def get_order(self, order_id: str) -> dict[str, Any] | None:
        with self._lock:
            for order in self._orders:
                if order.get("order_id") == order_id:
                    return dict(order)
        return None

    def list_orders(self, status: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            items = [dict(o) for o in self._orders]
            if status:
                items = [o for o in items if o.get("status") == status]
            return items
