"""Persisted FloodWait deadlines must survive restarts and gate every request."""

from unittest.mock import AsyncMock

import pytest
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

from telegram_mcp.floodgate import FloodWaitState, GatedTelegramClient, flood_state_path


def test_state_without_file_allows_requests(tmp_path):
    state = FloodWaitState(tmp_path / "flood.json")
    assert state.retry_after() == 0.0


def test_recorded_deadline_survives_new_instance(tmp_path):
    """A restarted process (new state object, same file) still honours the wait."""
    path = tmp_path / "flood.json"
    FloodWaitState(path).record(3600)

    restarted = FloodWaitState(path)
    remaining = restarted.retry_after()
    assert 3590 < remaining <= 3600


def test_expired_deadline_allows_requests(tmp_path):
    path = tmp_path / "flood.json"
    state = FloodWaitState(path)
    state.record(-1)  # already in the past
    assert state.retry_after() == 0.0


def test_record_failure_stops_instead_of_continuing(tmp_path):
    # A path whose parent cannot be created: a file exists where a directory
    # is needed, so mkdir/replace raises OSError.
    blocker = tmp_path / "blocker"
    blocker.write_text("x")
    state = FloodWaitState(blocker / "flood.json")

    with pytest.raises(RuntimeError, match="could not be persisted"):
        state.record(60)


def test_corrupt_state_file_is_ignored(tmp_path):
    path = tmp_path / "flood.json"
    path.write_text("{not json")
    assert FloodWaitState(path).retry_after() == 0.0


def test_flood_state_path_env_override(tmp_path, monkeypatch):
    override = tmp_path / "custom.json"
    monkeypatch.setenv("TELEGRAM_FLOOD_STATE_FILE", str(override))
    assert flood_state_path("session-id", "default") == override

    labelled = tmp_path / "work.json"
    monkeypatch.setenv("TELEGRAM_FLOOD_STATE_FILE_WORK", str(labelled))
    assert flood_state_path("session-id", "work") == labelled


def _gated_client(state) -> GatedTelegramClient:
    client = GatedTelegramClient(StringSession(), 12345, "fake_hash")
    client._flood_gate = state
    return client


@pytest.mark.asyncio
async def test_gate_blocks_requests_before_deadline(tmp_path, monkeypatch):
    """No network is attempted while a recorded wait is still running."""
    state = FloodWaitState(tmp_path / "flood.json")
    state.record(3600)
    client = _gated_client(state)

    underlying = AsyncMock(side_effect=AssertionError("network call attempted"))
    monkeypatch.setattr(TelegramClient, "__call__", underlying)

    with pytest.raises(FloodWaitError) as excinfo:
        await client("any-request")

    underlying.assert_not_awaited()
    assert excinfo.value.seconds > 3500


@pytest.mark.asyncio
async def test_gate_persists_reported_flood_wait(tmp_path, monkeypatch):
    """A FloodWaitError from Telegram is recorded before it propagates."""
    state = FloodWaitState(tmp_path / "flood.json")
    client = _gated_client(state)

    monkeypatch.setattr(
        TelegramClient,
        "__call__",
        AsyncMock(side_effect=FloodWaitError(request="req", capture=3600)),
    )

    with pytest.raises(FloodWaitError):
        await client("any-request")

    # The deadline is on disk — a process restarting right now would see it.
    assert FloodWaitState(tmp_path / "flood.json").retry_after() > 3500


@pytest.mark.asyncio
async def test_gate_passes_requests_when_unblocked(tmp_path, monkeypatch):
    state = FloodWaitState(tmp_path / "flood.json")
    client = _gated_client(state)

    underlying = AsyncMock(return_value="ok")
    monkeypatch.setattr(TelegramClient, "__call__", underlying)

    assert await client("any-request") == "ok"
    underlying.assert_awaited_once()
