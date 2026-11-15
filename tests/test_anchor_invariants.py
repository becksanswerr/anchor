"""Tests for all 8 mandatory Anchor invariants (real Dispatcher, BoardStore, LocalInvoker, JsonFileStorage)."""

import pytest

from anchor import (
    BoardError,
    BoardStore,
    Dispatcher,
    HandoffError,
    JsonFileStorage,
    LeaseTable,
    LocalInvoker,
    RouteError,
    RouteRule,
    TaskBoard,
    hand_off,
)


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = float(start)

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> float:
        self.now += seconds
        return self.now


# 1. Pending entries are never pruned when terminal entries exceed the board limit.
def test_1_pending_entries_are_never_pruned():
    clock = FakeClock()
    boards = BoardStore(boards=["work"], max_entries_per_board=200, clock=clock)
    blocked = boards.post_entry("work", {"author": "planner", "status": "blocked", "topic": "waiting-card"})
    for i in range(250):
        boards.post_entry("work", {"author": "worker", "status": "done", "topic": f"done-{i}"})

    assert boards.get_entry("work", blocked["id"]) is not None
    assert len(boards.list_entries("work", limit=500)) <= 201


# 2. hand_off is indivisible and noisy: board write failure never rings; ring failure marks undelivered.
def test_2_handoff_is_indivisible_and_noisy():
    clock = FakeClock()
    boards = BoardStore(boards=["work"], clock=clock)
    called = []
    invoker = LocalInvoker({"worker": lambda p: called.append(p) or {"ok": True}})
    dispatcher = Dispatcher(boards, invoker, routes={"work": RouteRule({"assigned"}, ("worker",))}, clock=clock)

    # Unregistered board fails before any ring is attempted
    with pytest.raises(HandoffError):
        hand_off(boards, dispatcher, "unregistered_board", "planner", "assigned", {"x": 1}, "worker")
    assert dispatcher.list_orders() == []
    assert called == []

    # Valid board write + invalid route target -> entry is marked `undelivered` and HandoffError is raised
    with pytest.raises(HandoffError):
        hand_off(boards, dispatcher, "work", "planner", "assigned", {"x": 2}, "forbidden_node")
    entries = boards.list_entries("work")
    assert len(entries) == 1
    assert entries[0]["status"] == "undelivered"
    assert isinstance(BoardError("x"), ValueError)


# 3. Ring alone only enqueues; dispatcher.tick() performs the invoke.
def test_3_ring_enqueues_and_tick_dispatches():
    clock = FakeClock()
    boards = BoardStore(boards=["work"], clock=clock)
    seen = []
    invoker = LocalInvoker({"worker": lambda p: seen.append(p["entry_id"]) or {"ok": True}})
    dispatcher = Dispatcher(boards, invoker, routes={"work": RouteRule({"assigned"}, ("worker",))}, clock=clock)

    entry_id = hand_off(boards, dispatcher, "work", "manager", "assigned", {"step": 1}, "worker")
    orders = dispatcher.list_orders()
    assert len(orders) == 1
    assert orders[0]["status"] == "queued"
    assert seen == []

    res = dispatcher.tick()
    assert orders[0]["order_id"] in res["dispatched"]
    assert seen == [entry_id]
    assert dispatcher.get_order(orders[0]["order_id"])["status"] == "done"


# 4. Timeout writes a `complaint` entry to the owner board and never self-triggers any route.
def test_4_timeout_writes_complaint_without_retriggering():
    clock = FakeClock(1000.0)
    boards = BoardStore(boards=["work", "owner"], clock=clock)
    invoker = LocalInvoker({
        "worker": lambda p: {"async": True},
        "owner_node": lambda p: {"ok": True},
    })
    dispatcher = Dispatcher(
        boards,
        invoker,
        routes={
            "work": RouteRule({"assigned"}, ("worker",), owner_board="owner"),
            "owner": RouteRule({"approved"}, ("owner_node",)),
        },
        clock=clock,
    )

    entry_id = hand_off(boards, dispatcher, "work", "manager", "assigned", {"x": 1}, "worker", timeout_seconds=60)
    dispatcher.tick()
    order = dispatcher.list_orders()[0]
    assert order["status"] == "running"

    clock.advance(65.0)
    res = dispatcher.tick()
    assert order["order_id"] in res["timed_out"]
    assert dispatcher.get_order(order["order_id"])["status"] == "timed_out"

    complaints = boards.list_entries("owner", status="complaint")
    assert len(complaints) == 1
    assert complaints[0]["data"]["issue"] == "timeout"
    # Trying to ring on a complaint entry must be rejected by the route table
    with pytest.raises(RouteError):
        dispatcher.ring(author="dispatcher", board="owner", entry_id=complaints[0]["id"], target_node="owner_node")
    assert entry_id


# 5. Restart recovery: persisted queued/running orders recover; expired running orders become timed_out (never re-run);
#    running orders exceeding max_resumes become failed.
def test_5_restart_recovery_from_disk_never_reruns_expired(tmp_path):
    clock = FakeClock(2000.0)
    boards_file = tmp_path / "boards.json"
    orders_file = tmp_path / "orders.json"

    boards1 = BoardStore(JsonFileStorage(boards_file), boards=["work", "owner"], clock=clock)
    invoked = []
    invoker1 = LocalInvoker({"worker": lambda p: invoked.append(p["order_id"]) or {"async": True}})
    disp1 = Dispatcher(
        boards1,
        invoker1,
        storage=JsonFileStorage(orders_file),
        routes={"work": RouteRule({"assigned"}, ("worker",), owner_board="owner")},
        max_resumes=2,
        clock=clock,
    )

    hand_off(boards1, disp1, "work", "m", "assigned", {"k": "expired"}, "worker", timeout_seconds=30)
    disp1.tick()
    assert len(invoked) == 1

    # Advance past timeout before "restart"
    clock.advance(45.0)
    boards2 = BoardStore(JsonFileStorage(boards_file), boards=["work", "owner"], clock=clock)
    invoked2 = []
    invoker2 = LocalInvoker({"worker": lambda p: invoked2.append(p["order_id"]) or {"async": True}})
    disp2 = Dispatcher(
        boards2,
        invoker2,
        storage=JsonFileStorage(orders_file),
        routes={"work": RouteRule({"assigned"}, ("worker",), owner_board="owner")},
        max_resumes=2,
        clock=clock,
    )
    disp2.tick()
    assert invoked2 == []
    assert disp2.list_orders()[0]["status"] == "timed_out"
    assert len(boards2.list_entries("owner", status="complaint")) == 1

    # Now test non-expired running order exceeding max_resumes across restarts
    orders_file2 = tmp_path / "orders_max_resumes.json"
    boards3 = BoardStore(boards=["work", "owner"], clock=clock)
    disp3 = Dispatcher(
        boards3,
        LocalInvoker({"worker": lambda p: {"async": True}}),
        storage=JsonFileStorage(orders_file2),
        routes={"work": RouteRule({"assigned"}, ("worker",), owner_board="owner")},
        max_resumes=1,
        clock=clock,
    )
    hand_off(boards3, disp3, "work", "m", "assigned", {"k": "fresh"}, "worker", timeout_seconds=300)
    disp3.tick()

    # Restart 1: resume_count=1 <= max_resumes(1) -> re-queued
    disp4 = Dispatcher(
        boards3,
        LocalInvoker({"worker": lambda p: {"async": True}}),
        storage=JsonFileStorage(orders_file2),
        routes={"work": RouteRule({"assigned"}, ("worker",), owner_board="owner")},
        max_resumes=1,
        clock=clock,
    )
    assert disp4.list_orders()[0]["status"] == "queued"
    disp4.tick()

    # Restart 2: resume_count=2 > max_resumes(1) -> failed
    disp5 = Dispatcher(
        boards3,
        LocalInvoker({"worker": lambda p: {"async": True}}),
        storage=JsonFileStorage(orders_file2),
        routes={"work": RouteRule({"assigned"}, ("worker",), owner_board="owner")},
        max_resumes=1,
        clock=clock,
    )
    assert disp5.list_orders()[0]["status"] == "failed"


# 6. DAG step dependencies: waits_for unblocking, failure -> blocked_by_failure, missing bound card -> failed.
def test_6_step_dependencies_and_missing_card():
    clock = FakeClock()
    boards = BoardStore(boards=["work"], clock=clock)
    tasks = TaskBoard(clock=clock)
    t = tasks.create(task_id="dag-1", title="Pipeline")
    tasks.set_plan(
        t["id"],
        [
            {"step_id": "1", "action": "fetch"},
            {"step_id": "2", "action": "summarize", "waits_for": ["1"]},
            {"step_id": "3", "action": "publish", "waits_for": ["2"]},
        ],
    )
    e2 = boards.post_entry("work", {"author": "manager", "status": "assigned", "topic": "step-2"})
    tasks.bind_card("dag-1", "2", e2["id"])
    # Note: step 3 has NO bound card -> when step 2 finishes, step 3 must fail immediately (not hang)

    r1 = tasks.step_done("dag-1", "1", "done", result="raw-data", boards=boards, board_name="work")
    assert [s["step_id"] for s in r1["ready"]] == ["2"]
    assert r1["ready"][0]["inputs"] == {"1": "raw-data"}
    assert r1["state"] == "running"

    r2 = tasks.step_done("dag-1", "2", "done", result="summary", boards=boards, board_name="work")
    assert r2["state"] == "failed"
    step3 = next(s for s in r2["steps"] if s["step_id"] == "3")
    assert step3["status"] == "failed"

    # Failure cascades to blocked_by_failure on downstream steps
    t2 = tasks.create(task_id="dag-2", title="Cascade")
    tasks.set_plan(
        t2["id"],
        [
            {"step_id": "1", "action": "fetch"},
            {"step_id": "2", "action": "transform", "waits_for": ["1"]},
        ],
    )
    r_fail = tasks.step_done("dag-2", "1", "failed", result="network error")
    assert r_fail["state"] == "failed"
    assert r_fail["steps"][1]["status"] == "blocked_by_failure"


# 7. Suspend / resume: suspended (`needs_input`) tasks never time out.
def test_7_suspend_exempts_from_timeout_and_resumes_once():
    clock = FakeClock(1000.0)
    boards = BoardStore(boards=["work", "owner"], clock=clock)
    tasks = TaskBoard(clock=clock)
    t = tasks.create(task_id="susp-1", title="Need confirmation")

    dispatcher = Dispatcher(
        boards,
        LocalInvoker({"worker": lambda p: {"async": True}}),
        tasks=tasks,
        routes={"work": RouteRule({"assigned"}, ("worker",), owner_board="owner")},
        clock=clock,
    )
    hand_off(boards, dispatcher, "work", "manager", "assigned", {"x": 1}, "worker", task_id=t["id"], timeout_seconds=30)
    dispatcher.tick()

    tasks.suspend(t["id"], "Proceed with action?", wait_key="req-42")
    clock.advance(500.0)
    res = dispatcher.tick()
    assert res["timed_out"] == []
    assert dispatcher.list_orders()[0]["status"] == "running"

    # Wrong key does not resume; right key resumes once
    ok_bad, _ = tasks.resume(t["id"], answer="yes", key="wrong")
    assert ok_bad is False
    ok_good, resumed = tasks.resume(t["id"], answer="yes", key="req-42")
    assert ok_good is True
    assert resumed["status"] == "running"
    ok_dup, _ = tasks.resume(t["id"], answer="yes", key="req-42")
    assert ok_dup is False


# 8. LeaseTable: exclusive acquisition, TTL expiry, and owner-only release.
def test_8_lease_acquire_ttl_and_owner_release():
    clock = FakeClock(100.0)
    leases = LeaseTable(clock=clock)

    assert leases.acquire("curator", "node-a", ttl=10.0) is True
    assert leases.acquire("curator", "node-b", ttl=10.0) is False
    assert leases.holder("curator") == "node-a"

    # Non-owner cannot release
    assert leases.release("curator", "node-b") is False
    assert leases.holder("curator") == "node-a"

    # After TTL expires, another holder can acquire
    clock.advance(11.0)
    assert leases.holder("curator") == ""
    assert leases.acquire("curator", "node-b", ttl=10.0) is True
    assert leases.release("curator", "node-b") is True
    assert leases.holder("curator") == ""
