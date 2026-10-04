"""State files carry a schema version and survive crashes intact.

The durability rules live in telegram_mcp.state: every JSON state file is
written via tmp+fsync+replace and stamped with a version; every journal
line is fsynced. These tests pin those properties against the three
callers (flood deadline, ops journal, pending events).
"""

import json

import pytest

from telegram_mcp.floodgate import FloodWaitState
from telegram_mcp.opsjournal import OperationJournal
from telegram_mcp.state import STATE_VERSION, append_jsonl, atomic_write_json


def test_atomic_write_stamps_version(tmp_path):
    path = tmp_path / "state.json"
    atomic_write_json(path, {"until": 123.0})
    data = json.loads(path.read_text())
    assert data["v"] == STATE_VERSION
    assert data["until"] == 123.0
    assert not (tmp_path / "state.json.tmp").exists()


def test_atomic_write_replaces_not_appends(tmp_path):
    path = tmp_path / "state.json"
    atomic_write_json(path, {"n": 1})
    atomic_write_json(path, {"n": 2})
    assert json.loads(path.read_text())["n"] == 2


def test_append_jsonl_durable_lines(tmp_path):
    path = tmp_path / "ops.jsonl"
    append_jsonl(path, {"operation_id": "a", "status": "executing"})
    append_jsonl(path, {"operation_id": "a", "status": "confirmed"})
    lines = path.read_text().strip().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[1])["status"] == "confirmed"


def test_append_jsonl_creates_parent_dirs(tmp_path):
    path = tmp_path / "nested" / "deep" / "ops.jsonl"
    append_jsonl(path, {"ok": True})
    assert path.exists()


def test_floodwait_state_file_is_versioned(tmp_path):
    path = tmp_path / "flood.json"
    FloodWaitState(path).record(30)
    data = json.loads(path.read_text())
    assert data["v"] == STATE_VERSION
    assert data["until"] > 0


def test_operation_journal_reads_own_durable_lines(tmp_path):
    path = tmp_path / "ops.jsonl"
    journal = OperationJournal(path)
    journal.record("op-1", "executing", kind="message")
    assert OperationJournal(path).lookup("op-1")["status"] == "executing"


def test_events_pending_file_is_versioned(tmp_path, monkeypatch):
    import telegram_mcp.tools.events as ev

    monkeypatch.setenv("TELEGRAM_PENDING_FILE", str(tmp_path / "pending.json"))
    ev._pending_msgs[42] = {
        "first_ts": 1.0,
        "last_ts": 1.0,
        "count": 1,
        "first_id": 7,
        "last_id": 7,
        "name": "n",
        "username": None,
        "kind": "dm",
        "topic_id": None,
        "sender_id": 9,
    }
    try:
        ev._save_pending_state()
        data = json.loads((tmp_path / "pending.json").read_text())
        assert data["v"] == STATE_VERSION
        assert "42" in data["pending"]
    finally:
        ev._pending_msgs.clear()
