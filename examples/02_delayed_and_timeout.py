"""Delayed order scheduling and timeout complaint routing using a fake clock."""

from anchor import BoardStore, Dispatcher, LocalInvoker, RouteRule, hand_off


def main() -> None:
    now = [1000.0]
    clock = lambda: now[0]

    boards = BoardStore(boards=["jobs", "supervisor"], clock=clock)
    invoker = LocalInvoker({
        "worker": lambda payload: {"async": True, "order_id": payload["order_id"]},
    })
    dispatcher = Dispatcher(
        boards,
        invoker,
        routes={
            "jobs": RouteRule({"assigned"}, ("worker",), owner_board="supervisor"),
        },
        clock=clock,
    )

    hand_off(
        boards,
        dispatcher,
        "jobs",
        "scheduler",
        "assigned",
        {"task": "nightly-index"},
        "worker",
        delay_seconds=30,
        timeout_seconds=60,
    )

    print("t=1000 tick:", dispatcher.tick())
    now[0] = 1030.0
    print("t=1030 tick (promoted + dispatched):", dispatcher.tick())
    now[0] = 1095.0
    print("t=1095 tick (timed out):", dispatcher.tick())
    complaints = boards.list_entries("supervisor", status="complaint")
    print("supervisor complaints:", len(complaints), complaints[0]["topic"])


if __name__ == "__main__":
    main()
