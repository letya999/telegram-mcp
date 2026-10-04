"""Scenario playground: real tools over a simulated Telegram world.

The world (tests/fake_telegram.py) owns chats, users and fault injection;
runtime.clients is mutated in place so get_client / ensure_connected /
resolve_entity / ops journal / pending events all run for real — only the
wire is fake. Scenarios are written in user terms: alice.say(), flood(),
drop_response(), a second client_for() = process restart.
"""

import json
import time
from types import SimpleNamespace

import pytest

import fake_telegram
from telegram_mcp import runtime
from telegram_mcp.floodgate import FloodWaitState
from telegram_mcp.runner import _connect_authorized_client, _session_locks
from telegram_mcp.tools import events, media, messages


@pytest.fixture
def world_harness(monkeypatch, tmp_path):
    world = fake_telegram.FakeTelegram()
    me = world.add_account("test_account")
    alice = world.add_user("alice")
    bob = world.add_user("bob")
    dm = world.private_chat(me, alice)
    group = world.group_chat("team", me, alice, bob)
    world.add_sticker_set("test_pack", "👍", "🔥")

    flood_file = tmp_path / "flood.json"
    client = world.client_for(me, flood_state=FloodWaitState(flood_file))

    monkeypatch.setenv("TELEGRAM_OPS_JOURNAL_FILE", str(tmp_path / "ops.jsonl"))
    monkeypatch.setenv("TELEGRAM_PENDING_FILE", str(tmp_path / "pending.json"))
    monkeypatch.delenv("TELEGRAM_ALLOWED_CHAT_IDS", raising=False)
    monkeypatch.delenv(events._INCOMING_GROUPS_ENV, raising=False)
    monkeypatch.setattr(runtime, "SERVER_ALLOWED_ROOTS", [tmp_path])
    monkeypatch.setattr(events, "is_chat_allowlist_enabled", lambda: False)
    # runtime.clients holds the import-time real client — restore it after,
    # don't just clear: other tests assume the import-time default exists.
    saved_clients = dict(runtime.clients)
    runtime.clients.clear()
    runtime.clients["default"] = client
    events._pending_msgs.clear()
    events._seen_ids.clear()
    events._self_ids.clear()
    monkeypatch.setattr(events, "_pending_loaded", False)
    yield SimpleNamespace(
        world=world,
        me=me,
        alice=alice,
        bob=bob,
        dm=dm,
        group=group,
        client=client,
        tmp=tmp_path,
        flood_file=flood_file,
    )
    runtime.clients.clear()
    runtime.clients.update(saved_clients)
    events._pending_msgs.clear()
    events._seen_ids.clear()
    events._self_ids.clear()
    for lock in _session_locks.values():
        lock.release()
    _session_locks.clear()


def _restarted(h):
    """Same account, same session, same on-disk state = a restarted process."""
    return h.world.client_for(h.me, flood_state=FloodWaitState(h.flood_file))


# --- connect / auth -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_connect_authorized_client_full_path(world_harness):
    await _connect_authorized_client("default", world_harness.client)
    assert world_harness.client.is_connected()


@pytest.mark.asyncio
async def test_connect_refuses_wrong_account_id(world_harness, monkeypatch):
    monkeypatch.setenv("TELEGRAM_EXPECTED_USER_ID", "1")
    with pytest.raises(RuntimeError, match="different account"):
        await _connect_authorized_client("default", world_harness.client)
    assert not world_harness.client.is_connected()


@pytest.mark.asyncio
async def test_connect_refuses_unauthorized(world_harness):
    world_harness.client.set_authorized(False)
    with pytest.raises(RuntimeError, match="not authorized"):
        await _connect_authorized_client("default", world_harness.client)


# --- outgoing sends -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_lands_in_world_and_dedups(world_harness):
    h = world_harness
    first = await messages.send_message(chat_id=h.dm.id, message="hi", operation_id="e2e-1")
    receipt = json.loads(first)
    assert receipt["status"] == "confirmed"

    # The world actually holds the message a real recipient would see.
    assert h.dm.messages[-1].message == "hi"
    assert h.dm.messages[-1].sender_id == h.me.id

    second = await messages.send_message(chat_id=h.dm.id, message="hi", operation_id="e2e-1")
    assert json.loads(second)["deduplicated"] is True
    assert len(h.dm.messages) == 1  # Telegram never saw a second request


@pytest.mark.asyncio
async def test_operation_id_dedup_survives_restart(world_harness):
    h = world_harness
    await messages.send_message(chat_id=h.dm.id, message="hi", operation_id="e2e-r")
    runtime.clients["default"] = _restarted(h)
    retry = await messages.send_message(chat_id=h.dm.id, message="hi", operation_id="e2e-r")
    assert json.loads(retry)["deduplicated"] is True
    assert runtime.clients["default"].sent == []


@pytest.mark.asyncio
async def test_media_sends_journaled_end_to_end(world_harness):
    h = world_harness
    (h.tmp / "note.ogg").write_bytes(b"OggS" * 4)

    r1 = json.loads(
        await media.send_voice(
            chat_id=h.dm.id, file_path=str(h.tmp / "note.ogg"), operation_id="v1"
        )
    )
    r2 = json.loads(await media.send_gif(chat_id=h.dm.id, gif_id=42, operation_id="g1"))
    r3 = json.loads(
        await media.send_sticker(chat_id=h.dm.id, sticker="11:22:dead", operation_id="s1")
    )
    for receipt in (r1, r2, r3):
        assert receipt["status"] == "confirmed"
        assert receipt["message_ids"]

    assert len(h.client.sent) == 3
    dup = await media.send_voice(
        chat_id=h.dm.id, file_path=str(h.tmp / "note.ogg"), operation_id="v1"
    )
    assert json.loads(dup)["deduplicated"] is True


@pytest.mark.asyncio
async def test_sticker_set_lookup_then_send(world_harness):
    h = world_harness
    listing = json.loads(await media.get_sticker_set("test_pack"))
    assert listing["set"] == "test_pack Set"
    assert len(listing["stickers"]) == 2

    ref = listing["stickers"][0]["sticker"]
    result = await media.send_sticker(chat_id=h.dm.id, sticker=ref)
    assert "sent" in result.lower()
    assert h.dm.messages[-1].media is not None


# --- ambiguous failure: delivered but response lost ------------------------------------


@pytest.mark.asyncio
async def test_dropped_response_is_unknown_not_resent(world_harness):
    h = world_harness
    h.client.drop_response()

    result = await messages.send_message(
        chat_id=h.dm.id, message="did it land?", operation_id="amb-1"
    )
    assert "sent" not in result.lower()

    journal = runtime.ops_journal_for(h.client)
    assert journal.lookup("amb-1")["status"] == "unknown"
    # The message really did reach the world — a blind resend would duplicate.
    assert h.dm.messages[-1].message == "did it land?"

    retry = await messages.send_message(
        chat_id=h.dm.id, message="did it land?", operation_id="amb-1"
    )
    assert json.loads(retry)["status"] == "unknown"
    assert h.dm.messages[-1].message == "did it land?"
    assert len(h.dm.messages) == 1  # no duplicate


@pytest.mark.asyncio
async def test_partition_failures_do_not_fake_success(world_harness):
    h = world_harness
    h.world.partition()
    result = await messages.send_message(chat_id=h.dm.id, message="hello?", operation_id="off-1")
    assert "sent" not in result.lower()
    assert runtime.ops_journal_for(h.client).lookup("off-1")["status"] == "unknown"

    h.world.go_online()
    ok = await messages.send_message(chat_id=h.dm.id, message="back online")
    assert "sent" in ok.lower() or "successfully" in ok.lower()


# --- FloodWait ------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_floodwait_deferred_and_survives_restart(world_harness):
    h = world_harness
    h.client.flood(3600)

    result = await messages.send_message(chat_id=h.dm.id, message="hi", operation_id="fl-1")
    assert "sent" not in result.lower()
    assert runtime.ops_journal_for(h.client).lookup("fl-1")["status"] == "deferred"

    restarted = _restarted(h)
    runtime.clients["default"] = restarted
    result = await messages.send_message(chat_id=h.dm.id, message="again")
    assert "sent" not in result.lower()
    retry = await messages.send_message(chat_id=h.dm.id, message="hi", operation_id="fl-1")
    assert json.loads(retry)["status"] == "deferred"
    assert restarted.sent == []  # no wire calls post-restart


# --- incoming events: simulated users drive the real handler ----------------------------


@pytest.mark.asyncio
async def test_users_talking_drive_incoming_events(world_harness, monkeypatch):
    h = world_harness
    events.register_incoming_handlers()

    await h.alice.say(h.dm, "hey there")
    assert events._pending_msgs[h.dm.id]["kind"] == "dm"

    # Groups stay silent until TELEGRAM_INCOMING_GROUPS.
    await h.alice.say(h.group, "hello @test_account")
    assert h.group.id not in events._pending_msgs

    monkeypatch.setenv("TELEGRAM_INCOMING_GROUPS", "1")
    await h.alice.say(h.group, "hello @test_account")
    assert events._pending_msgs[h.group.id]["kind"] == "mention"

    # Reply to our own group message wakes us even without a mention.
    ours = h.group.messages[-1]  # placeholder — send one of ours first
    await messages.send_message(chat_id=h.group.id, message="deploy is live")
    ours = h.group.messages[-1]
    await h.bob.say(h.group, "nice work", reply_to=ours)
    assert events._pending_msgs[h.group.id]["kind"] == "mention"  # keeps stronger kind


@pytest.mark.asyncio
async def test_reply_to_own_message_wakes(world_harness, monkeypatch):
    h = world_harness
    monkeypatch.setenv("TELEGRAM_INCOMING_GROUPS", "1")
    events.register_incoming_handlers()

    await messages.send_message(chat_id=h.group.id, message="deploy is live")
    ours = h.group.messages[-1]
    await h.bob.say(h.group, "nice work", reply_to=ours)
    rec = events._pending_msgs[h.group.id]
    assert rec["kind"] == "reply"
    assert rec["sender_id"] == h.bob.id


@pytest.mark.asyncio
async def test_replayed_update_deduplicates(world_harness):
    h = world_harness
    events.register_incoming_handlers()
    msg = await h.alice.say(h.dm, "ping")
    await h.world.broadcast(msg)  # reconnect replays the same update
    assert events._pending_msgs[h.dm.id]["count"] == 1


@pytest.mark.asyncio
async def test_own_messages_never_wake(world_harness):
    h = world_harness
    events.register_incoming_handlers()
    await messages.send_message(chat_id=h.dm.id, message="me talking to myself")
    assert h.dm.id not in events._pending_msgs


# --- read-side tools against the world --------------------------------------------------


@pytest.mark.asyncio
async def test_get_mentions_finds_world_messages(world_harness, monkeypatch):
    h = world_harness
    monkeypatch.setenv("TELEGRAM_INCOMING_GROUPS", "1")
    events.register_incoming_handlers()

    await h.alice.say(h.group, "ping @test_account are you around?")
    result = await messages.get_mentions(chat_id=h.group.id)
    assert "ping" in result
    assert "alice" in result.lower()


@pytest.mark.asyncio
async def test_scheduled_message_journaled(world_harness):
    h = world_harness
    when = int(time.time()) + 3600
    result = await messages.send_scheduled_message(
        chat_id=h.dm.id, message="later", schedule_date=when, operation_id="sch-1"
    )
    assert json.loads(result)["status"] == "confirmed"
    assert h.dm.messages[-1].scheduled is True

    retry = await messages.send_scheduled_message(
        chat_id=h.dm.id, message="later", schedule_date=when, operation_id="sch-1"
    )
    assert json.loads(retry)["deduplicated"] is True
    assert len(h.dm.messages) == 1


@pytest.mark.asyncio
async def test_pending_events_survive_restart(world_harness):
    """A message arrives, the process dies before the feed drains it — the
    restored pending burst is still there."""
    h = world_harness
    events.register_incoming_handlers()
    await h.alice.say(h.dm, "don't lose me")

    events._pending_msgs.clear()
    events._seen_ids.clear()
    events._pending_loaded = False
    events._load_pending_state()
    assert h.dm.id in events._pending_msgs
    assert events._pending_msgs[h.dm.id]["count"] == 1


@pytest.mark.asyncio
async def test_two_accounts_share_one_world(world_harness, monkeypatch):
    """Second client = a colleague's account in the same simulated Telegram:
    sends land in the same chats the first account sees."""
    h = world_harness
    other = h.world.client_for(h.alice, session_string="fake-session-alice")
    runtime.clients["work"] = other

    # Two clients → account is required on every call.
    await messages.send_message(chat_id=h.dm.id, message="from default", account="default")
    via_alice = await messages.send_message(chat_id=h.dm.id, message="from work", account="work")
    assert "successfully" in via_alice.lower()
    senders = [m.sender_id for m in h.dm.messages]
    assert h.me.id in senders and h.alice.id in senders


@pytest.mark.asyncio
async def test_edit_and_delete_round_trip(world_harness):
    h = world_harness
    await messages.send_message(chat_id=h.dm.id, message="typo")
    msg = h.dm.messages[-1]
    assert msg.message == "typo"

    edited = await messages.edit_message(chat_id=h.dm.id, message_id=msg.id, new_text="fixed")
    assert "error" not in edited.lower() or "edited" in edited.lower()
    assert h.dm.messages[-1].message == "fixed"
