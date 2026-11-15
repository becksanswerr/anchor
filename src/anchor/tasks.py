"""Task board with DAG step dependencies, suspend/resume, and mid-flight instructions."""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any, Callable

from .storage import MemoryStorage, Storage


class TaskBoard:
    """Thread-safe task store managing execution plans, dependencies, and suspension."""

    def __init__(
        self,
        storage: Storage | None = None,
        *,
        clock: Callable[[], float] = time.time,
        max_log: int = 100,
    ) -> None:
        self._storage: Storage = storage if storage is not None else MemoryStorage()
        self._clock = clock
        self._max_log = int(max_log)
        self._lock = threading.RLock()
        self._tasks: dict[str, dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        data = self._storage.load()
        if isinstance(data, dict):
            with self._lock:
                self._tasks = {str(k): dict(v) for k, v in data.items() if isinstance(v, dict)}

    def _save(self) -> None:
        self._storage.save(self._tasks)

    def create(
        self,
        task_id: str | None = None,
        *,
        title: str = "",
        description: str = "",
        origin: str = "system",
        status: str = "queued",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            tid = str(task_id or uuid.uuid4().hex[:8])
            now = float(self._clock())
            task: dict[str, Any] = {
                "id": tid,
                "title": str(title or ""),
                "description": str(description or ""),
                "origin": str(origin or "system"),
                "status": str(status or "queued"),
                "steps": [],
                "plan_state": "none",
                "instructions": [],
                "log": [],
                "waiting": {},
                "cancel_requested": False,
                "metadata": dict(metadata or {}),
                "created": now,
                "updated": now,
                "finished": None,
            }
            self._tasks[tid] = task
            self._save()
            return dict(task)

    def get(self, task_id: str) -> dict[str, Any] | None:
        with self._lock:
            t = self._tasks.get(task_id)
            return dict(t) if t is not None else None

    def update(self, task_id: str, **patch: Any) -> dict[str, Any] | None:
        with self._lock:
            t = self._tasks.get(task_id)
            if not t:
                return None
            for k, v in patch.items():
                if k in ("id", "created"):
                    continue
                if v is not None:
                    t[k] = v
            now = float(self._clock())
            t["updated"] = now
            if t.get("status") in ("done", "failed", "cancelled") and not t.get("finished"):
                t["finished"] = now
            self._save()
            return dict(t)

    def list_tasks(self, status: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            items = [dict(t) for t in self._tasks.values()]
            if status:
                items = [t for t in items if t.get("status") == status]
            return sorted(items, key=lambda x: x.get("updated", 0), reverse=True)[:limit]

    def add_log(self, task_id: str, text: str) -> dict[str, Any] | None:
        with self._lock:
            t = self._tasks.get(task_id)
            if not t:
                return None
            now = float(self._clock())
            t.setdefault("log", []).append({"ts": now, "text": str(text)[:400]})
            t["log"] = t["log"][-self._max_log :]
            t["updated"] = now
            self._save()
            return dict(t)

    def set_plan(self, task_id: str, steps: list[Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """Attach an ordered DAG plan. `waits_for` may only reference earlier steps.

        Why: restricting `waits_for` to previously seen step IDs makes cycles impossible by
        construction; any forward/unknown reference is dropped and reported in `adjustments`
        so callers can log it rather than silently deadlocking.
        """
        with self._lock:
            t = self._tasks.get(task_id)
            if not t:
                return None, []
            normalized: list[dict[str, Any]] = []
            seen: list[str] = []
            adjustments: list[str] = []
            for idx, raw in enumerate(steps or [], start=1):
                s = raw if isinstance(raw, dict) else {"action": str(raw)}
                sid = str(s.get("step_id") or idx)
                if sid in seen:
                    sid = f"{sid}_{idx}"
                    adjustments.append(f"duplicate step_id renamed to '{sid}'")
                waits_for: list[str] = []
                for dep in s.get("waits_for") or ():
                    dep_id = str(dep)
                    if dep_id in seen and dep_id not in waits_for:
                        waits_for.append(dep_id)
                    else:
                        adjustments.append(
                            f"step '{sid}': cannot wait for '{dep_id}' (not an earlier step), dropped"
                        )
                normalized.append({
                    "step_id": sid,
                    "action": str(s.get("action") or "")[:600],
                    "waits_for": waits_for,
                    "status": "waiting" if waits_for else "assigned",
                    "result": "",
                    "entry_id": str(s.get("entry_id") or ""),
                    "role": str(s.get("role") or ""),
                })
                seen.append(sid)
            t["steps"] = normalized
            t["plan_state"] = "running"
            t["status"] = "running"
            t["updated"] = float(self._clock())
            self._save()
            return dict(t), adjustments

    def bind_card(self, task_id: str, step_id: str, entry_id: str, role: str = "") -> dict[str, Any] | None:
        """Bind a blackboard entry_id (and optional role) to a plan step."""
        with self._lock:
            t = self._tasks.get(task_id)
            if not t:
                return None
            for s in t.get("steps", []):
                if str(s.get("step_id")) == str(step_id):
                    s["entry_id"] = str(entry_id)
                    if role:
                        s["role"] = str(role)
                    break
            t["updated"] = float(self._clock())
            self._save()
            return dict(t)

    def step_done(
        self,
        task_id: str,
        step_id: str,
        status: str,
        result: str = "",
        *,
        boards: Any = None,
        board_name: str | None = None,
    ) -> dict[str, Any] | None:
        """Mark a step finished (`done` or `failed`) and release newly unblocked steps.

        Why atomic under lock: two parallel steps completing simultaneously must never both
        release the same downstream step or both emit the final plan transition.
        Also, if a newly ready step has a missing bound card on `boards`, it is marked
        `failed` immediately so the plan does not think it is running forever.
        """
        with self._lock:
            t = self._tasks.get(task_id)
            if not t:
                return None
            steps: list[dict[str, Any]] = t.get("steps") or []
            if not steps:
                steps = [{
                    "step_id": str(step_id or "1"),
                    "action": t.get("title", ""),
                    "waits_for": [],
                    "status": "assigned",
                    "result": "",
                    "entry_id": "",
                    "role": "",
                }]
                t["steps"] = steps

            target = next((s for s in steps if str(s.get("step_id")) == str(step_id)), None)
            if target is None and len(steps) == 1:
                target = steps[0]
            if target is not None and target.get("status") not in ("done", "failed", "cancelled"):
                target["status"] = "done" if status == "done" else "failed"
                target["result"] = str(result or "")[:4000]

            # Cascade failure to waiting steps whose upstream failed or was blocked_by_failure.
            changed = True
            while changed:
                changed = False
                statuses = {str(s.get("step_id")): s.get("status") for s in steps}
                for s in steps:
                    if s.get("status") != "waiting":
                        continue
                    if any(statuses.get(str(d)) in ("failed", "blocked_by_failure") for d in s.get("waits_for", [])):
                        s["status"] = "blocked_by_failure"
                        changed = True

            ready: list[dict[str, Any]] = []
            statuses = {str(s.get("step_id")): s.get("status") for s in steps}
            for s in steps:
                if s.get("status") != "waiting":
                    continue
                if all(statuses.get(str(d)) == "done" for d in s.get("waits_for", [])):
                    eid = str(s.get("entry_id") or "")
                    if boards is not None and (
                        not eid or (board_name and boards.get_entry(board_name, eid) is None)
                    ):
                        s["status"] = "failed"
                        s["result"] = "bound card missing"
                        statuses[str(s.get("step_id"))] = "failed"
                        continue
                    s["status"] = "assigned"
                    statuses[str(s.get("step_id"))] = "assigned"
                    ready.append({
                        "step_id": s.get("step_id"),
                        "entry_id": eid,
                        "inputs": {
                            str(d): next(
                                (x.get("result", "") for x in steps if str(x.get("step_id")) == str(d)),
                                "",
                            )
                            for d in s.get("waits_for", [])
                        },
                    })

            # Re-cascade in case a newly ready step failed due to a missing card.
            changed = True
            while changed:
                changed = False
                statuses = {str(s.get("step_id")): s.get("status") for s in steps}
                for s in steps:
                    if s.get("status") != "waiting":
                        continue
                    if any(statuses.get(str(d)) in ("failed", "blocked_by_failure") for d in s.get("waits_for", [])):
                        s["status"] = "blocked_by_failure"
                        changed = True

            statuses = {str(s.get("step_id")): s.get("status") for s in steps}
            if all(v == "done" for v in statuses.values()):
                plan_state = "done"
            elif any(v in ("assigned", "running") for v in statuses.values()):
                plan_state = "running"
            else:
                plan_state = "failed"

            prev_state = t.get("plan_state") or "running"
            transitioned = plan_state != "running" and prev_state == "running"
            now = float(self._clock())
            t["plan_state"] = plan_state
            if plan_state in ("done", "failed"):
                t["status"] = plan_state
                if not t.get("finished"):
                    t["finished"] = now
            t["updated"] = now
            self._save()
            return {
                "ready": ready,
                "state": plan_state,
                "transitioned": transitioned,
                "steps": [dict(s) for s in steps],
            }

    def suspend(self, task_id: str, question: str, wait_key: str = "") -> dict[str, Any] | None:
        """Pause a task in `needs_input`; suspended tasks are exempt from execution timeouts."""
        with self._lock:
            t = self._tasks.get(task_id)
            if not t:
                return None
            now = float(self._clock())
            t["status"] = "needs_input"
            t["waiting"] = {
                "question": str(question or ""),
                "key": str(wait_key or ""),
                "since": now,
            }
            t["updated"] = now
            self._save()
            return dict(t)

    def resume(self, task_id: str, answer: str = "", key: str = "") -> tuple[bool, dict[str, Any] | None]:
        """Resume a suspended (`needs_input`) task atomically for at most one caller."""
        with self._lock:
            t = self._tasks.get(task_id)
            if not t or t.get("status") != "needs_input":
                return False, (dict(t) if t else None)
            waiting = t.get("waiting") or {}
            if key and waiting.get("key") and waiting.get("key") != key:
                return False, dict(t)
            now = float(self._clock())
            t["status"] = "running"
            t["waiting"] = {}
            if answer:
                t["last_answer"] = str(answer)
            t["updated"] = now
            self._save()
            return True, dict(t)

    def add_instruction(self, task_id: str, text: str = "", cancel: bool = False) -> dict[str, Any] | None:
        """Append a mid-flight instruction or cancel the task."""
        with self._lock:
            t = self._tasks.get(task_id)
            if not t:
                return None
            now = float(self._clock())
            if text:
                t.setdefault("instructions", []).append({"ts": now, "text": str(text)[:1000]})
            if cancel:
                t["cancel_requested"] = True
                if t.get("status") not in ("done", "failed", "cancelled"):
                    t["status"] = "cancelled"
                    t["finished"] = now
            t["updated"] = now
            self._save()
            return dict(t)
