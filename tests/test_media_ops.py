"""Media sends under the operation journal + stickers by document reference."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from telethon.tl.types import DocumentAttributeSticker, InputDocument

from telegram_mcp.tools import media


@pytest.fixture
def mock_media(monkeypatch, tmp_path):
    sent_files = []

    client = MagicMock()
    client.session = None

    async def fake_send_file(entity, file, **kwargs):
        sent_files.append({"file": file, **kwargs})
        return SimpleNamespace(id=9001)

    client.send_file = AsyncMock(side_effect=fake_send_file)
    client._parse_message_text = AsyncMock(side_effect=lambda t, m: (t, []))

    rpc_calls = []

    async def fake_call(request):
        rpc_calls.append(request)
        return SimpleNamespace(
            set=SimpleNamespace(title="My Set"),
            documents=[
                SimpleNamespace(
                    id=11,
                    access_hash=22,
                    file_reference=b"\xde\xad",
                    attributes=[DocumentAttributeSticker(alt="👍", stickerset=None)],
                )
            ],
        )

    client.side_effect = fake_call

    monkeypatch.setenv("TELEGRAM_OPS_JOURNAL_FILE", str(tmp_path / "ops.jsonl"))

    entity = SimpleNamespace(id=777000123)

    async def fake_resolve(chat_id, cl=None):
        return entity

    async def fake_resolve_readable(raw_path, ctx=None, tool_name=None):
        return raw_path, None

    monkeypatch.setattr(media, "get_client", lambda account=None: client)
    monkeypatch.setattr(media, "ensure_connected", AsyncMock())
    monkeypatch.setattr(media, "resolve_entity", fake_resolve)
    monkeypatch.setattr(media, "_resolve_readable_file_path", fake_resolve_readable)
    monkeypatch.setattr(media, "is_chat_allowlist_enabled", lambda: False)

    return {"client": client, "sent_files": sent_files, "rpc": rpc_calls}


@pytest.mark.asyncio
async def test_send_voice_journaled_and_deduplicated(mock_media):
    first = await media.send_voice(chat_id=1, file_path="note.ogg", operation_id="v-1")
    second = await media.send_voice(chat_id=1, file_path="note.ogg", operation_id="v-1")

    assert len(mock_media["sent_files"]) == 1
    assert mock_media["sent_files"][0]["voice_note"] is True

    receipt = json.loads(first)
    assert receipt["status"] == "confirmed"
    assert receipt["message_ids"] == [9001]
    assert json.loads(second)["deduplicated"] is True


@pytest.mark.asyncio
async def test_send_file_journaled(mock_media):
    result = await media.send_file(chat_id=1, file_path="doc.pdf", operation_id="f-1")
    receipt = json.loads(result)
    assert receipt["kind"] == "send_file"
    assert receipt["status"] == "confirmed"
    assert receipt["message_ids"] == [9001]


@pytest.mark.asyncio
async def test_send_sticker_by_reference(mock_media):
    result = await media.send_sticker(chat_id=1, sticker="11:22:dead")

    sent = mock_media["sent_files"][0]["file"]
    assert isinstance(sent, InputDocument)
    assert sent.id == 11 and sent.access_hash == 22
    assert sent.file_reference == b"\xde\xad"
    assert "sent" in result.lower()


@pytest.mark.asyncio
async def test_send_sticker_requires_exactly_one_source(mock_media):
    assert "exactly one" in await media.send_sticker(chat_id=1)
    assert "exactly one" in await media.send_sticker(chat_id=1, file_path="a.webp", sticker="1:2")
    assert "must be" in await media.send_sticker(chat_id=1, sticker="garbage")


@pytest.mark.asyncio
async def test_get_sticker_set_returns_sendable_refs(mock_media):
    result = json.loads(await media.get_sticker_set("my_set"))
    assert result["set"] == "My Set"
    assert result["stickers"] == [{"sticker": "11:22:dead", "emoji": "👍"}]
