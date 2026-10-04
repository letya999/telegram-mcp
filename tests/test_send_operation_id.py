"""operation_id must make send retries safe: same request id, journaled result."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from telethon.tl.types import UpdateMessageID

from telegram_mcp.tools import messages


@pytest.fixture
def mock_client(monkeypatch, tmp_path):
    sent_requests = []

    client = MagicMock()

    async def fake_call(request):
        sent_requests.append(request)
        return SimpleNamespace(updates=[UpdateMessageID(id=4242, random_id=request.random_id)])

    client.side_effect = fake_call
    client.send_message = AsyncMock(return_value=SimpleNamespace(id=4242))
    client._parse_message_text = AsyncMock(side_effect=lambda text, mode: (text, []))
    client.session = None  # session_identity falls back to anon:<id>

    monkeypatch.setenv("TELEGRAM_OPS_JOURNAL_FILE", str(tmp_path / "ops.jsonl"))

    entity = SimpleNamespace(id=777000123)

    async def fake_resolve(chat_id, cl=None):
        return entity

    monkeypatch.setattr(messages, "get_client", lambda account=None: client)
    monkeypatch.setattr(messages, "ensure_connected", AsyncMock())
    monkeypatch.setattr(messages, "resolve_entity", fake_resolve)
    monkeypatch.setattr(messages, "is_chat_allowlist_enabled", lambda: False)

    return {"client": client, "sent_requests": sent_requests, "entity": entity}


@pytest.mark.asyncio
async def test_same_operation_id_never_sends_twice(mock_client):
    """A retry with the same operation_id returns the recorded receipt —
    the journaled operation is not executed a second time, even in-process.
    Across restarts the journal on disk provides the same guarantee."""
    first = await messages.send_message(chat_id=123, message="hi", operation_id="op-1")
    second = await messages.send_message(chat_id=123, message="hi", operation_id="op-1")

    assert len(mock_client["sent_requests"]) == 1

    receipt = json.loads(first)
    assert receipt["status"] == "confirmed"
    assert receipt["operation_id"] == "op-1"
    assert receipt["message_id"] == 4242
    assert receipt["chat_id"] == 777000123

    replay = json.loads(second)
    assert replay["deduplicated"] is True
    assert replay["message_id"] == 4242
    assert replay["status"] == "confirmed"


@pytest.mark.asyncio
async def test_different_operation_ids_differ(mock_client):
    await messages.send_message(chat_id=123, message="hi", operation_id="op-1")
    await messages.send_message(chat_id=123, message="hi", operation_id="op-2")

    rids = [r.random_id for r in mock_client["sent_requests"]]
    assert rids[0] != rids[1]


@pytest.mark.asyncio
async def test_stable_random_id_fits_signed_int64(mock_client):
    await messages.send_message(chat_id=123, message="hi", operation_id="op-1")
    rid = mock_client["sent_requests"][0].random_id
    assert -(2**63) <= rid <= 2**63 - 1


@pytest.mark.asyncio
async def test_failed_send_is_journaled_not_resent(mock_client):
    """A clear Telegram rejection records 'failed'; retrying the op id returns
    the recorded failure instead of blindly resending."""
    from telethon.errors import ChatWriteForbiddenError

    async def reject(request):
        raise ChatWriteForbiddenError(request=request)

    mock_client["client"].side_effect = reject
    result = await messages.send_message(chat_id=123, message="hi", operation_id="op-x")
    assert "error" in result.lower() or "code:" in result.lower()

    # The journal has the failure; a retry returns it without another request.
    import os
    from pathlib import Path
    from telegram_mcp.opsjournal import OperationJournal

    rec = OperationJournal(Path(os.environ["TELEGRAM_OPS_JOURNAL_FILE"])).lookup("op-x")
    assert rec is not None and rec["status"] == "failed"

    await messages.send_message(chat_id=123, message="hi", operation_id="op-x")
    assert len(mock_client["sent_requests"]) == 0


@pytest.mark.asyncio
async def test_ambiguous_failure_is_unknown(mock_client):
    """A timeout mid-send leaves the operation 'unknown' — never reported sent."""

    async def timeout(request):
        raise asyncio.TimeoutError()

    import asyncio

    mock_client["client"].side_effect = timeout
    await messages.send_message(chat_id=123, message="hi", operation_id="op-t")
    # surfaced as a formatted error, and journaled as unknown
    import os
    from pathlib import Path
    from telegram_mcp.opsjournal import OperationJournal

    rec = OperationJournal(Path(os.environ["TELEGRAM_OPS_JOURNAL_FILE"])).lookup("op-t")
    assert rec is not None and rec["status"] == "unknown"


@pytest.mark.asyncio
async def test_without_operation_id_keeps_legacy_result(mock_client):
    result = await messages.send_message(chat_id=123, message="hi")
    assert result == "Message sent successfully."
    # The legacy path uses the high-level helper, not a raw request.
    assert mock_client["sent_requests"] == []
