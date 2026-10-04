"""Proactive per-class request pacing in GatedTelegramClient.

Pacing spaces requests BEFORE Telegram complains -- the reactive FloodWait
gate stays as the backstop. Off by default; TELEGRAM_PACE_*_MS opts in.
"""

import asyncio
import time

import pytest

from telethon.tl import functions, types

from telegram_mcp.floodgate import PacingGate


class _FakeClient:
    """Minimal stand-in for GatedTelegramClient's __call__ pacing path."""

    def __init__(self, pacing):
        self._pacing = pacing
        self.calls = []

    async def __call__(self, request):
        pacing = self._pacing
        if pacing is not None:
            cls = pacing.request_class(request)
            if cls is not None:
                await pacing.wait(cls)
        self.calls.append(request)
        return "ok"


def _send_request():
    return functions.messages.SendMessageRequest(
        peer=types.InputPeerEmpty(), message="hi", random_id=1
    )


def _search_request():
    return functions.messages.SearchRequest(
        peer=types.InputPeerEmpty(),
        q="x",
        filter=types.InputMessagesFilterEmpty(),
        min_date=0,
        max_date=0,
        offset_id=0,
        add_offset=0,
        limit=1,
        max_id=0,
        min_id=0,
        hash=0,
    )


class TestRequestClassification:
    def test_send_requests_are_message_class(self):
        gate = PacingGate({})
        assert gate.request_class(_send_request()) == "message"

    def test_search_request_is_search_class(self):
        gate = PacingGate({})
        assert gate.request_class(_search_request()) == "search"

    def test_unlisted_request_is_unpaced(self):
        gate = PacingGate({})
        assert gate.request_class(types.InputPeerEmpty()) is None


class TestPacingGate:
    @pytest.mark.asyncio
    async def test_off_by_default(self):
        gate = PacingGate({"message": 0, "search": 0})
        assert not gate.configured
        t = time.monotonic()
        await gate.wait("message")
        assert time.monotonic() - t < 0.05

    @pytest.mark.asyncio
    async def test_spaces_requests_of_same_class(self):
        gate = PacingGate({"message": 80})
        client = _FakeClient(gate)
        t = time.monotonic()
        await client(_send_request())
        await client(_send_request())
        assert time.monotonic() - t >= 0.075

    @pytest.mark.asyncio
    async def test_different_classes_run_independently(self):
        gate = PacingGate({"message": 80, "search": 0})
        client = _FakeClient(gate)
        t = time.monotonic()
        await client(_send_request())
        await client(_search_request())  # search unpaced — no delay
        assert time.monotonic() - t < 0.05

    @pytest.mark.asyncio
    async def test_concurrent_callers_serialize_back_to_back(self):
        gate = PacingGate({"message": 60})
        client = _FakeClient(gate)
        t = time.monotonic()
        await asyncio.gather(client(_send_request()), client(_send_request()))
        elapsed = time.monotonic() - t
        assert elapsed >= 0.055
        assert len(client.calls) == 2

    def test_from_env_global_and_label_override(self, monkeypatch):
        monkeypatch.setenv("TELEGRAM_PACE_MESSAGES_MS", "50")
        monkeypatch.setenv("TELEGRAM_PACE_MESSAGES_MS_WORK", "200")
        assert PacingGate.from_env("default")._intervals["message"] == 0.05
        assert PacingGate.from_env("work")._intervals["message"] == 0.2

    def test_from_env_bad_value_falls_back_to_zero(self, monkeypatch):
        monkeypatch.setenv("TELEGRAM_PACE_SEARCHES_MS", "abc")
        assert not PacingGate.from_env("default").configured
