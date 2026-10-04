"""Append-only ledger for outgoing Telegram operations.

Every send that carries an ``operation_id`` is journaled before the network
call with its target, and its outcome is appended after the call:

    executing -> confirmed | deferred | unknown | failed

The semantics that matter:

- a retry with the same ``operation_id`` returns the recorded operation
  instead of issuing a second send -- even after a restart, because the
  journal lives on disk, keyed by session identity;
- a clear Telegram rejection (RPCError) is ``failed``;
- a FloodWait raised before the request hit the wire is ``deferred``;
- a timeout or connection break in an ambiguous moment is ``unknown`` --
  never reported as sent and never blindly resent;
- a successful result records the message id(s) Telegram confirmed.

Storage is one JSON line per state transition; the last line for an
``operation_id`` wins. Small and boring on purpose: outgoing volume for a
personal account is low, and a flat file is auditable and survives crashes
better than an index nobody asked for.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Callable, Optional

from telethon.errors import FloodWaitError, RPCError
from telethon.tl import types

from telegram_mcp.floodgate import state_dir

STATUS_CONFIRMED = "confirmed"
STATUS_DEFERRED = "deferred"
STATUS_UNKNOWN = "unknown"
STATUS_FAILED = "failed"

_TRANSIENT = (TimeoutError, ConnectionError, OSError)


def journal_path(session_identity: str, label: str) -> Path:
    """Journal location; ``TELEGRAM_OPS_JOURNAL_FILE[_<LABEL>]`` overrides."""
    override = os.getenv(f"TELEGRAM_OPS_JOURNAL_FILE_{label.upper()}") or os.getenv(
        "TELEGRAM_OPS_JOURNAL_FILE"
    )
    if override:
        return Path(override)
    digest = hashlib.sha256(session_identity.encode("utf-8")).hexdigest()[:16]
    return state_dir() / f"operations-{digest}.jsonl"


class OperationJournal:
    """JSONL operation log: lookup() returns the newest record per op id."""

    def __init__(self, path: Path):
        self.path = path
        self._records: dict[str, dict[str, Any]] | None = None

    def _load(self) -> dict[str, dict[str, Any]]:
        if self._records is None:
            records: dict[str, dict[str, Any]] = {}
            try:
                for line in self.path.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except ValueError:
                        continue  # a crash may leave a torn final line
                    op = rec.get("operation_id")
                    if op:
                        records[op] = rec
            except OSError:
                pass
            self._records = records
        return self._records

    def lookup(self, operation_id: str) -> Optional[dict]:
        return self._load().get(operation_id)

    def record(self, operation_id: str, status: str, **fields: Any) -> dict:
        rec = {
            "operation_id": operation_id,
            "status": status,
            "ts": round(time.time(), 2),
            **fields,
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        self._load()[operation_id] = rec
        return rec

    def begin(self, operation_id: str, kind: str, **fields: Any) -> Optional[dict]:
        """Register an operation about to run; return the existing record if known."""
        existing = self.lookup(operation_id)
        if existing is not None:
            return existing
        self.record(operation_id, "executing", kind=kind, **fields)
        return None


def stable_random_id(operation_id: str, chat_id: int) -> int:
    """Derive a deterministic Telegram ``random_id`` from an operation id.

    Telegram deduplicates sends that reuse the same ``random_id``, so a retry
    carrying the same ``operation_id`` reissues the same request instead of
    producing a duplicate message.
    """
    digest = hashlib.sha256(f"telegram-mcp:{chat_id}:{operation_id}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


def receipt_message_id(result, random_id: int) -> Optional[int]:
    """Extract the confirmed message id from an Updates result via UpdateMessageID."""
    for update in getattr(result, "updates", None) or []:
        if isinstance(update, types.UpdateMessageID) and update.random_id == random_id:
            return update.id
    return None


def message_ids(result) -> list[int]:
    """Message ids from a send result that is a Message or a list of Messages."""
    items = result if isinstance(result, list) else [result]
    return [m.id for m in items if getattr(m, "id", None) is not None]


def classify_error(exc: Exception) -> str:
    """Map a send failure onto the journal status vocabulary."""
    if isinstance(exc, FloodWaitError):
        return STATUS_DEFERRED
    if isinstance(exc, RPCError):
        return STATUS_FAILED
    if isinstance(exc, _TRANSIENT):
        return STATUS_UNKNOWN
    return STATUS_FAILED


async def run_operation(
    journal: OperationJournal,
    operation_id: Optional[str],
    kind: str,
    fields: dict,
    fn: Callable[[], Any],
    receipt: Callable[[Any], dict],
) -> str:
    """Run ``fn`` under the journal; return a JSON receipt string.

    ``fn`` performs the send and returns the Telegram result; ``receipt``
    extracts ids from that result. Without ``operation_id`` this just calls
    ``fn`` and returns ``json.dumps(receipt(result))`` -- callers that want
    the legacy text response don't go through here at all.
    """
    prior = journal.begin(operation_id, kind, **fields)
    if prior is not None:
        return json.dumps({"deduplicated": True, **prior}, ensure_ascii=False)
    try:
        result = await fn()
    except Exception as exc:
        journal.record(
            operation_id, classify_error(exc), kind=kind, error=type(exc).__name__, **fields
        )
        raise
    return json.dumps(
        journal.record(operation_id, STATUS_CONFIRMED, kind=kind, **receipt(result), **fields),
        ensure_ascii=False,
    )
