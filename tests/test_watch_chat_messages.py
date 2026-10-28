import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from telegram_mcp.tools import messages


class Client:
    def __init__(self, ids):
        self.ids = ids
        self.calls = []

    async def get_messages(self, entity, **kwargs):
        return [self.msg(self.ids[-1])] if self.ids else []

    def iter_messages(self, entity, **kwargs):
        self.calls.append(kwargs)

        async def rows():
            for id in [x for x in self.ids if x > kwargs["min_id"]][: kwargs["limit"]]:
                yield self.msg(id)

        return rows()

    @staticmethod
    def msg(id):
        return SimpleNamespace(
            id=id,
            sender=None,
            sender_id=1,
            date=datetime(2026, 10, 28, tzinfo=timezone.utc),
            message=f"GitHub issue {id}",
            reply_to=None,
        )


@pytest.mark.asyncio
async def test_watch_chat_messages_bootstraps_and_pages_without_gaps(monkeypatch):
    client = Client([1, 2, 3])
    monkeypatch.setattr(messages, "get_client", lambda account=None: client)
    monkeypatch.setattr(messages, "resolve_entity", AsyncMock(return_value=SimpleNamespace()))
    monkeypatch.setattr(messages, "get_marked_id", lambda entity: -1001)

    baseline = json.loads(await messages.watch_chat_messages(chat_id=-1001))
    assert baseline == {"events": [], "next_cursor": "3"}
    client.ids.extend([4, 5, 6])
    first = json.loads(await messages.watch_chat_messages(chat_id=-1001, cursor="3", limit=2))
    assert [e["id"] for e in first["events"]] == ["4", "5"]
    assert first["next_cursor"] == "5"
    second = json.loads(await messages.watch_chat_messages(chat_id=-1001, cursor="5", limit=2))
    assert [e["id"] for e in second["events"]] == ["6"]
    assert second["next_cursor"] == "6"
    assert client.calls == [
        {"min_id": 3, "reverse": True, "limit": 2},
        {"min_id": 5, "reverse": True, "limit": 2},
    ]


@pytest.mark.asyncio
async def test_watch_chat_messages_rejects_bad_cursor_before_read(monkeypatch):
    monkeypatch.setattr(messages, "get_client", lambda account=None: pytest.fail("read occurred"))
    with pytest.raises(ValueError, match="invalid cursor"):
        await messages.watch_chat_messages(chat_id=-1001, cursor="-1")
