"""Atomic board-write + ring hand-off primitive."""

from __future__ import annotations

from typing import Any

from .boards import BoardError, BoardStore
from .dispatcher import Dispatcher


class HandoffError(RuntimeError):
    """Raised when writing to a board or ringing the dispatcher fails."""


def hand_off(
    boards: BoardStore,
    dispatcher: Dispatcher,
    board: str,
    author: str,
    status: str,
    data: Any,
    target_node: str = "",
    *,
    task_id: str = "",
    delay_seconds: float = 0.0,
    timeout_seconds: float = 1800.0,
    topic: str | None = None,
    to: Any = None,
    cc: Any = None,
    reply_to: str | None = None,
) -> str:
    """Write a work item to `board` and ring `dispatcher` as one logical hand-off.

    Why one indivisible helper: writing without ringing leaves work silently stranded
    on the board; ringing without writing leaves the woken node with nothing to read.
    If the ring fails after the write succeeded, the entry is marked `undelivered`
    before raising `HandoffError`.
    """
    payload: dict[str, Any] = {
        "task_id": task_id,
        "author": author,
        "status": status,
        "data": data,
    }
    if topic is not None:
        payload["topic"] = topic
    if to is not None:
        payload["to"] = to
    if cc is not None:
        payload["cc"] = cc
    if reply_to is not None:
        payload["reply_to"] = reply_to

    try:
        entry = boards.post_entry(board, payload)
    except BoardError as exc:
        raise HandoffError(f"cannot write hand-off to board '{board}': {exc}") from exc
    except Exception as exc:
        raise HandoffError(f"failed to write hand-off to board '{board}': {exc}") from exc

    entry_id = str(entry.get("id") or "")
    if not entry_id:
        raise HandoffError(f"board '{board}' returned no entry id")

    try:
        dispatcher.ring(
            author=author,
            board=board,
            entry_id=entry_id,
            target_node=target_node,
            task_id=task_id,
            delay_seconds=delay_seconds,
            timeout_seconds=timeout_seconds,
            topic=topic,
            to=to,
            cc=cc,
        )
    except Exception as exc:
        try:
            boards.update_entry(board, entry_id, status="undelivered")
        except Exception:
            pass
        raise HandoffError(f"ring failed after writing '{entry_id}' on '{board}': {exc}") from exc

    return entry_id
