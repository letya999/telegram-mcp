"""Single decision point for Telegram rate limiting.

Telethon already sleeps through FloodWaits shorter than
``flood_sleep_threshold`` inside ``client(...)``, but when a wait exceeds that
threshold the error surfaces to the caller -- and the process forgets it ever
happened. After a crash or restart the next tool call would hit Telegram
early, which is exactly the blind retry that escalates penalties.

:class:`FloodWaitState` persists the "next allowed attempt" deadline to disk,
keyed by session identity, so a restarted worker still honours the wait
Telegram imposed. :class:`GatedTelegramClient` enforces it on every request
going through ``client(...)`` -- service calls (peer resolution, connection
checks, updates, media) are covered too, not just MCP tool calls.

Per-chat slow mode (``SlowModeWaitError``) is deliberately not recorded here:
it is a sibling of ``FloodWaitError``, not a subclass, and it restricts one
chat, not the account. Recording it under the account key would throttle
unrelated chats.

If the deadline cannot be persisted, the operation fails instead of
continuing with a warning -- an unpersisted wait is a wait the next restart
will violate.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Optional

from telethon import TelegramClient
from telethon.errors import FloodWaitError

from telegram_mcp.state import atomic_write_json


def state_dir() -> Path:
    """State directory, following the aliases-file convention (XDG-aware)."""
    base = os.getenv("XDG_STATE_HOME") or Path.home() / ".local" / "state"
    return Path(base) / "telegram-mcp"


def flood_state_path(session_identity: str, label: str) -> Path:
    """Where this session's FloodWait deadline is recorded.

    ``TELEGRAM_FLOOD_STATE_FILE_<LABEL>`` / ``TELEGRAM_FLOOD_STATE_FILE``
    override the location (per-account first, then the shared fallback,
    mirroring the ``TELEGRAM_PROXY_*`` convention).
    """
    override = os.getenv(f"TELEGRAM_FLOOD_STATE_FILE_{label.upper()}") or os.getenv(
        "TELEGRAM_FLOOD_STATE_FILE"
    )
    if override:
        return Path(override)
    digest = hashlib.sha256(session_identity.encode("utf-8")).hexdigest()[:16]
    return state_dir() / f"floodwait-{digest}.json"


class FloodWaitState:
    """Durable record of the earliest moment Telegram will accept a new request."""

    def __init__(self, path: Path):
        self.path = path
        self._until: float = 0.0
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            self._until = float(data["until"])
        except (OSError, ValueError, KeyError, TypeError):
            # Missing or corrupt file = no recorded wait. A file left half-
            # written by a crash must not block the account forever.
            pass

    def retry_after(self) -> float:
        """Seconds until requests are allowed again; 0 when none is recorded."""
        return max(0.0, self._until - time.time())

    def record(self, seconds: float) -> None:
        """Persist ``now + seconds`` as the next allowed attempt.

        Raises ``RuntimeError`` when the write fails: continuing without a
        durable record would let the next restart retry early, so the
        operation stops instead.
        """
        until = time.time() + max(0.0, float(seconds))
        try:
            atomic_write_json(self.path, {"until": until})
        except OSError as exc:
            raise RuntimeError(
                f"FloodWait deadline could not be persisted to {self.path}; "
                "stopping rather than risking an early retry after restart."
            ) from exc
        self._until = max(self._until, until)


class GatedTelegramClient(TelegramClient):
    """TelegramClient that honours a persisted FloodWait deadline.

    Attach a :class:`FloodWaitState` to ``_flood_gate`` after construction.
    Every ``client(request)`` checks the recorded deadline first and raises
    ``FloodWaitError`` locally -- with the remaining seconds -- instead of
    asking Telegram. When Telegram itself reports a FloodWait, the deadline
    is persisted before the error propagates, so a crash or restart cannot
    lose it.
    """

    _flood_gate: Optional[FloodWaitState] = None

    async def __call__(self, request, ordered=False, flood_sleep_threshold=None):
        gate = self._flood_gate
        if gate is not None:
            wait = gate.retry_after()
            if wait > 0:
                raise FloodWaitError(request=request, capture=max(1, int(wait)))
        try:
            return await super().__call__(
                request, ordered=ordered, flood_sleep_threshold=flood_sleep_threshold
            )
        except FloodWaitError as exc:
            if gate is not None:
                gate.record(exc.seconds)
            raise
