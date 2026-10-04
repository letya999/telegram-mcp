"""Interactive walkthrough of the fake Telegram world — run it, read the
transcript, hack on the scenario. No network, no real session.

    uv run python scripts/fake_playground.py
"""

import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tests"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

os.environ.setdefault("TELEGRAM_API_ID", "12345")
os.environ.setdefault("TELEGRAM_API_HASH", "playground")
os.environ.setdefault("TELEGRAM_SESSION_NAME", "playground")

_tmp = tempfile.mkdtemp(prefix="tg-fake-")
os.environ["TELEGRAM_OPS_JOURNAL_FILE"] = os.path.join(_tmp, "ops.jsonl")
os.environ["TELEGRAM_PENDING_FILE"] = os.path.join(_tmp, "pending.json")
os.environ["TELEGRAM_INCOMING_GROUPS"] = "1"

import fake_telegram  # noqa: E402
from telegram_mcp import runtime  # noqa: E402
from telegram_mcp.floodgate import FloodWaitState  # noqa: E402
from telegram_mcp.tools import events, messages  # noqa: E402


async def main():
    world = fake_telegram.FakeTelegram()
    me = world.add_account("owner")
    alice = world.add_user("alice")
    group = world.group_chat("team", me, alice)
    dm = world.private_chat(me, alice)
    from pathlib import Path

    client = world.client_for(me, flood_state=FloodWaitState(Path(_tmp) / "flood.json"))
    runtime.clients["default"] = client
    events.register_incoming_handlers()

    print("== alice writes to the DM ==")
    await alice.say(dm, "hey @owner, deploy done?")
    print("pending bursts:", dict(events._pending_msgs))

    print("\n== we answer with an idempotent send ==")
    r = await messages.send_message(chat_id=dm.id, message="yes, shipped", operation_id="demo-1")
    print(r)
    print(
        "retry:",
        await messages.send_message(chat_id=dm.id, message="yes, shipped", operation_id="demo-1"),
    )

    print("\n== alice mentions us in the group ==")
    await alice.say(group, "@owner check this thread")
    print("pending:", {k: v["kind"] for k, v in events._pending_msgs.items()})

    print("\n== flood! ==")
    client.flood(120)
    print(await messages.send_message(chat_id=dm.id, message="oops too fast"))

    print("\n== the world's actual message log ==")
    for chat in (dm, group):
        for m in chat.messages:
            who = m.sender.username if m.sender else "?"
            print(f"  [{chat.title}] {who}: {m.message}")

    print("\nstate files:", _tmp)


if __name__ == "__main__":
    asyncio.run(main())
