"""Anchor + drift: Anchor keeps the state and decides WHEN a node wakes; drift decides WHERE it runs.

Anchor's dispatcher wakes a logical node ("summariser"). The invoker below hands that wake-up to a
drift Node, which runs a reverse auction across every machine in the mesh (this one bids too) and
executes the job on the winner. Start this script on a second machine on the same LAN/Tailscale
network and the auction will choose between them.

    pip install "vorlabs-anchor[drift] @ git+https://github.com/becksanswerr/anchor.git"
    python examples/03_with_drift.py
"""

import sys


def main() -> None:
    try:
        from drift import Node
    except ImportError:
        print("vorlabs-drift is not installed. Install with: pip install "vorlabs-anchor[drift] @ git+https://github.com/becksanswerr/anchor.git"")
        sys.exit(0)

    from anchor import BoardStore, Dispatcher, RouteRule, hand_off

    hive = Node(name="anchor-example", http_port=8741)  # any free port; must match on every machine

    @hive.capability("summariser")
    def summarise(payload):
        entry = payload.get("entry") or {}
        text = str((entry.get("data") or {}).get("text", ""))
        return {"summary": text[:60], "ran_on": hive.name}

    class DriftInvoker:
        """Anchor Invoker that routes every wake-up through a drift auction."""

        def __init__(self, node):
            self.node = node

        def invoke(self, target_node: str, payload: dict) -> dict:
            # prefer_remote=True opens the auction even though the capability exists locally;
            # the local node bids as well, so it still wins when it is the best choice.
            return self.node.call(target_node, payload, prefer_remote=True)

    hive.start()
    try:
        boards = BoardStore(boards=["work"])
        dispatcher = Dispatcher(boards, DriftInvoker(hive),
                                routes={"work": RouteRule({"assigned"}, ("summariser",))})
        hand_off(boards, dispatcher, "work", "orchestrator", "assigned",
                 {"text": "Anchor tracks the work; drift picks the machine that runs it."}, "summariser")
        print("tick:", dispatcher.tick())
        print("orders:", [(o["target_node"], o["status"]) for o in dispatcher.list_orders()])
    finally:
        hive.stop()


if __name__ == "__main__":
    main()
