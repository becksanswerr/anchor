"""Dispatcher runtime behaviours added after review: order pruning and the background runner."""

import threading
import time

from anchor import BoardStore, Dispatcher, LocalInvoker, RouteRule, hand_off


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = float(start)

    def __call__(self) -> float:
        return self.now


def test_finished_orders_are_pruned_but_live_ones_are_kept():
    clock = FakeClock()
    boards = BoardStore(boards=["work"], max_entries_per_board=1000, clock=clock)
    disp = Dispatcher(
        boards,
        LocalInvoker({"worker": lambda p: {"ok": True}, "slow": lambda p: {"async": True}}),
        routes={"work": RouteRule({"assigned"}, ("worker", "slow"))},
        max_finished_orders=10,
        clock=clock,
    )
    live = hand_off(boards, disp, "work", "m", "assigned", {"k": "live"}, "slow")
    disp.tick()  # 'slow' answers async -> stays running
    for i in range(30):
        clock.now += 1
        hand_off(boards, disp, "work", "m", "assigned", {"i": i}, "worker")
        disp.tick()

    orders = disp.list_orders()
    finished = [o for o in orders if o["status"] == "done"]
    assert len(finished) == 10
    assert any(o["entry_id"] == live and o["status"] == "running" for o in orders)


def test_background_runner_does_not_let_a_slow_node_block_others():
    boards = BoardStore(boards=["work"])
    release = threading.Event()
    fast_done = threading.Event()

    def slow(_p):
        release.wait(5)
        return {"ok": True}

    def fast(_p):
        fast_done.set()
        return {"ok": True}

    disp = Dispatcher(
        boards,
        LocalInvoker({"slow": slow, "fast": fast}),
        routes={"work": RouteRule({"assigned"}, ("slow", "fast"))},
    )
    hand_off(boards, disp, "work", "m", "assigned", {}, "slow")
    hand_off(boards, disp, "work", "m", "assigned", {}, "fast")
    disp.start(interval=0.05, max_workers=4)
    try:
        assert fast_done.wait(2), "fast node was blocked behind the slow one"
    finally:
        release.set()
        disp.stop()
    deadline = time.time() + 2
    while time.time() < deadline and any(o["status"] != "done" for o in disp.list_orders()):
        time.sleep(0.02)
    assert all(o["status"] == "done" for o in disp.list_orders())
