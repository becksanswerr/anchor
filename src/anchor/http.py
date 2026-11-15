"""Optional FastAPI adapter for Anchor (requires `vorlabs-anchor[http]`)."""

from __future__ import annotations

from typing import Any

from .boards import BoardError, BoardStore
from .dispatcher import Dispatcher
from .leases import LeaseTable
from .tasks import TaskBoard


def create_app(
    boards: BoardStore,
    tasks: TaskBoard,
    dispatcher: Dispatcher,
    leases: LeaseTable | None = None,
):
    """Build a FastAPI app exposing Anchor boards, tasks, orders, and leases."""
    from fastapi import Body, FastAPI, HTTPException

    app = FastAPI(title="Anchor Coordination API")

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"ok": True, "boards": boards.snapshot()}

    @app.post("/boards/{board_name}/entries")
    def post_entry(board_name: str, payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
        try:
            entry = boards.post_entry(board_name, payload)
        except BoardError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"entry": entry}

    @app.get("/boards/{board_name}/entries")
    def list_entries(board_name: str, status: str | None = None, limit: int = 50) -> dict[str, Any]:
        try:
            items = boards.list_entries(board_name, status=status, limit=limit)
        except BoardError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"entries": items}

    @app.post("/tasks")
    def create_task(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
        task = tasks.create(
            task_id=payload.get("id"),
            title=str(payload.get("title") or ""),
            description=str(payload.get("description") or ""),
            origin=str(payload.get("origin") or "http"),
        )
        return {"task": task}

    @app.get("/tasks/{task_id}")
    def get_task(task_id: str) -> dict[str, Any]:
        task = tasks.get(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="task not found")
        return {"task": task}

    @app.post("/orders/tick")
    def tick_orders() -> dict[str, Any]:
        return dispatcher.tick()

    if leases is not None:

        @app.get("/leases")
        def get_leases() -> dict[str, Any]:
            return {"leases": leases.snapshot()}

    return app
