"""Single-process pipeline: planner -> worker -> reviewer via registered boards and hand_off."""

from anchor import BoardStore, Dispatcher, LocalInvoker, RouteRule, TaskBoard, hand_off


def main() -> None:
    boards = BoardStore(boards=["planning", "work", "review"])
    tasks = TaskBoard()
    task = tasks.create(task_id="job-1", title="Draft release notes")
    tasks.set_plan(
        task["id"],
        [
            {"step_id": "1", "action": "Gather changelog"},
            {"step_id": "2", "action": "Review summary", "waits_for": ["1"]},
        ],
    )

    invoker = LocalInvoker()
    dispatcher = Dispatcher(
        boards,
        invoker,
        tasks=tasks,
        routes={
            "planning": RouteRule({"planned"}, ("planner",)),
            "work": RouteRule({"assigned"}, ("worker",)),
            "review": RouteRule({"ready"}, ("reviewer",)),
        },
    )

    def on_planner(payload: dict) -> dict:
        print(f"[planner] received {payload['entry_id']} -> handing off to worker")
        e1 = hand_off(boards, dispatcher, "work", "planner", "assigned", {"step": "1"}, "worker", task_id="job-1")
        tasks.bind_card("job-1", "1", e1, role="worker")
        return {"ok": True}

    def on_worker(payload: dict) -> dict:
        print(f"[worker] executing {payload['entry_id']} -> marking step 1 done")
        res = tasks.step_done("job-1", "1", "done", result="Changelog collected")
        print(f"[worker] unblocked steps: {[s['step_id'] for s in res['ready']]}")
        hand_off(
            boards,
            dispatcher,
            "review",
            "worker",
            "ready",
            {"draft": res["steps"][0]["result"]},
            "reviewer",
            task_id="job-1",
        )
        return {"ok": True}

    def on_reviewer(payload: dict) -> dict:
        print(f"[reviewer] reviewing {payload['entry_id']} -> marking step 2 done")
        res = tasks.step_done("job-1", "2", "done", result="Approved")
        print(f"[reviewer] plan state: {res['state']}")
        return {"ok": True}

    invoker.register("planner", on_planner)
    invoker.register("worker", on_worker)
    invoker.register("reviewer", on_reviewer)

    hand_off(boards, dispatcher, "planning", "api", "planned", {"goal": task["title"]}, "planner", task_id="job-1")
    while dispatcher.list_orders(status="queued"):
        dispatcher.tick()


if __name__ == "__main__":
    main()
