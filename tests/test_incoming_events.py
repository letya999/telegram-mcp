"""Incoming-event recording: group wake rules, dedup, restart persistence."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from telegram_mcp.tools import events


def _sender(id=42, username="alice"):
    return SimpleNamespace(
        id=id,
        username=username,
        bot=False,
        is_self=False,
        first_name="Alice",
        last_name=None,
        title=None,
    )


def _client(me_id=999):
    return SimpleNamespace(get_me=AsyncMock(return_value=SimpleNamespace(id=me_id)))


def _event(
    chat_id,
    msg_id,
    *,
    is_private=False,
    mentioned=False,
    is_reply=False,
    reply_to=None,
    sender=None,
    client=None,
    replied=None,
):
    return SimpleNamespace(
        is_private=is_private,
        chat_id=chat_id,
        client=client or _client(),
        message=SimpleNamespace(
            id=msg_id, mentioned=mentioned, is_reply=is_reply, reply_to=reply_to
        ),
        get_sender=AsyncMock(return_value=sender or _sender()),
        get_reply_message=AsyncMock(return_value=replied),
    )


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch, tmp_path):
    events._pending_msgs.clear()
    events._seen_ids.clear()
    events._self_ids.clear()
    monkeypatch.setattr(events, "_pending_loaded", False)
    monkeypatch.setenv("TELEGRAM_PENDING_FILE", str(tmp_path / "pending.json"))
    monkeypatch.delenv(events._INCOMING_GROUPS_ENV, raising=False)
    monkeypatch.delenv("TELEGRAM_EVENT_FEED", raising=False)
    monkeypatch.delenv("TELEGRAM_ALLOWED_CHAT_IDS", raising=False)
    monkeypatch.setattr(events, "is_chat_allowlist_enabled", lambda: False)
    yield
    events._pending_msgs.clear()
    events._seen_ids.clear()
    events._self_ids.clear()


@pytest.mark.asyncio
async def test_private_message_recorded_as_dm():
    await events._on_new_incoming(_event(111, 7, is_private=True))
    rec = events._pending_msgs[111]
    assert rec["kind"] == "dm" and rec["count"] == 1


@pytest.mark.asyncio
async def test_group_message_ignored_without_flag(monkeypatch):
    await events._on_new_incoming(_event(-100, 7, mentioned=True))
    assert -100 not in events._pending_msgs


@pytest.mark.asyncio
async def test_group_mention_recorded(monkeypatch):
    monkeypatch.setenv("TELEGRAM_INCOMING_GROUPS", "1")
    await events._on_new_incoming(_event(-100, 7, mentioned=True))
    assert events._pending_msgs[-100]["kind"] == "mention"


@pytest.mark.asyncio
async def test_group_plain_message_ignored(monkeypatch):
    monkeypatch.setenv("TELEGRAM_INCOMING_GROUPS", "1")
    await events._on_new_incoming(_event(-100, 7))
    assert -100 not in events._pending_msgs


@pytest.mark.asyncio
async def test_group_reply_to_own_message_recorded(monkeypatch):
    monkeypatch.setenv("TELEGRAM_INCOMING_GROUPS", "1")
    reply_to = SimpleNamespace(reply_to_msg_id=50, reply_to_top_id=None, forum_topic=False)
    replied = SimpleNamespace(sender_id=999)  # == client's own id
    await events._on_new_incoming(
        _event(-100, 7, is_reply=True, reply_to=reply_to, replied=replied)
    )
    assert events._pending_msgs[-100]["kind"] == "reply"


@pytest.mark.asyncio
async def test_group_reply_to_someone_else_ignored(monkeypatch):
    monkeypatch.setenv("TELEGRAM_INCOMING_GROUPS", "1")
    reply_to = SimpleNamespace(reply_to_msg_id=50, reply_to_top_id=None, forum_topic=False)
    replied = SimpleNamespace(sender_id=555)  # another member's message
    await events._on_new_incoming(
        _event(-100, 7, is_reply=True, reply_to=reply_to, replied=replied)
    )
    assert -100 not in events._pending_msgs


@pytest.mark.asyncio
async def test_forum_topic_extracted(monkeypatch):
    monkeypatch.setenv("TELEGRAM_INCOMING_GROUPS", "1")
    reply_to = SimpleNamespace(reply_to_msg_id=30, reply_to_top_id=31337, forum_topic=True)
    await events._on_new_incoming(_event(-100, 7, mentioned=True, reply_to=reply_to))
    assert events._pending_msgs[-100]["topic_id"] == 31337


@pytest.mark.asyncio
async def test_replayed_message_is_deduplicated():
    ev = _event(111, 7, is_private=True)
    await events._on_new_incoming(ev)
    await events._on_new_incoming(ev)  # same chat+id replay after reconnect
    assert events._pending_msgs[111]["count"] == 1


@pytest.mark.asyncio
async def test_state_survives_restart(tmp_path):
    await events._on_new_incoming(_event(111, 7, is_private=True))

    # Simulate a restart: in-memory state gone, file remains.
    events._pending_msgs.clear()
    events._seen_ids.clear()
    events._pending_loaded = False

    events._load_pending_state()
    assert 111 in events._pending_msgs  # undelivered burst restored
    assert events._pending_msgs[111]["count"] == 1

    # ...and the replayed message still dedupes against the restored seen-set.
    await events._on_new_incoming(_event(111, 7, is_private=True))
    assert events._pending_msgs[111]["count"] == 1
