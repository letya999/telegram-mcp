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

The same gate also paces requests *proactively* per operation class
(messages / forwards / searches) when ``TELEGRAM_PACE_*`` is configured:
reactive FloodWait handling limits the damage after Telegram complains,
spacing limits how often it needs to. The two mechanisms are deliberately
separate -- pacing state lives only in memory (a restarted process pacing
from zero is safe), while FloodWait deadlines must outlive the process.

Per-chat slow mode (``SlowModeWaitError``) is deliberately not recorded here:
it is a sibling of ``FloodWaitError``, not a subclass, and it restricts one
chat, not the account. Recording it under the account key would throttle
unrelated chats.

If the deadline cannot be persisted, the operation fails instead of
continuing with a warning -- an unpersisted wait is a wait the next restart
will violate.
"""

from __future__ import annotations

import asyncio
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


# Requests grouped by how Telegram's anti-spam heuristics treat them: sends
# and forwards are the actions that trigger penalties; searches are paced
# separately because a fan-out (one call per chat) burns quota faster than
# its own rate suggests. Everything unlisted is unpaced -- reads, edits,
# deletes and service calls don't need proactive spacing to stay safe.
_PACING_REQUEST_CLASSES: dict[str, str] = {
    "SendMessageRequest": "message",
    "SendMediaRequest": "message",
    "SendMultiMediaRequest": "message",
    "ForwardMessagesRequest": "forward",
    "SearchRequest": "search",
    "SearchGlobalRequest": "search",
    "SearchGifsRequest": "search",
}

_PACING_ENV = {
    "message": "TELEGRAM_PACE_MESSAGES_MS",
    "forward": "TELEGRAM_PACE_FORWARDS_MS",
    "search": "TELEGRAM_PACE_SEARCHES_MS",
}


class PacingGate:
    """Minimum spacing between requests of the same class, per client.

    In-memory only: a restarted process pacing from zero is safe (the worst
    case is a burst it should have spaced), unlike a lost FloodWait deadline
    which provokes an escalating penalty.
    """

    def __init__(self, intervals_ms: dict[str, int]):
        self._intervals = {cls: ms / 1000.0 for cls, ms in intervals_ms.items() if ms > 0}
        self._next_at: dict[str, float] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    @classmethod
    def from_env(cls, label: str) -> "PacingGate":
        """Read ``TELEGRAM_PACE_*`` (milliseconds), per-label override first."""

        def _ms(name: str) -> int:
            raw = os.getenv(f"{name}_{label.upper()}") or os.getenv(name) or "0"
            try:
                return max(0, int(raw))
            except ValueError:
                return 0

        return cls({cls_: _ms(env) for cls_, env in _PACING_ENV.items()})

    @property
    def configured(self) -> bool:
        return bool(self._intervals)

    def request_class(self, request) -> Optional[str]:
        """Pacing class of a TL request, or ``None`` when it is unpaced."""
        return _PACING_REQUEST_CLASSES.get(type(request).__name__)

    async def wait(self, cls: str) -> None:
        """Sleep until this class's slot opens; reserve the next slot first.

        The slot is claimed before sleeping so concurrent callers queue up
        back-to-back rather than waking together into a burst -- which is
        precisely the pattern pacing exists to prevent.
        """
        interval = self._intervals.get(cls)
        if not interval:
            return
        lock = self._locks.setdefault(cls, asyncio.Lock())
        async with lock:
            now = time.monotonic()
            next_at = self._next_at.get(cls, 0.0)
            self._next_at[cls] = max(now, next_at) + interval
            delay = next_at - now
            if delay > 0:
                await asyncio.sleep(delay)


class GatedTelegramClient(TelegramClient):
    """TelegramClient that honours a persisted FloodWait deadline.

    Attach a :class:`FloodWaitState` to ``_flood_gate`` after construction.
    Every ``client(request)`` checks the recorded deadline first and raises
    ``FloodWaitError`` locally -- with the remaining seconds -- instead of
    asking Telegram. When Telegram itself reports a FloodWait, the deadline
    is persisted before the error propagates, so a crash or restart cannot
    lose it.

    Attach a :class:`PacingGate` to ``_pacing`` for proactive per-class
    spacing; without one, behaviour is unchanged.
    """

    _flood_gate: Optional[FloodWaitState] = None
    _pacing: Optional[PacingGate] = None

    async def __call__(self, request, ordered=False, flood_sleep_threshold=None):
        gate = self._flood_gate
        if gate is not None:
            wait = gate.retry_after()
            if wait > 0:
                raise FloodWaitError(request=request, capture=max(1, int(wait)))
        pacing = self._pacing
        if pacing is not None:
            cls = pacing.request_class(request)
            if cls is not None:
                await pacing.wait(cls)
        try:
            return await super().__call__(
                request, ordered=ordered, flood_sleep_threshold=flood_sleep_threshold
            )
        except FloodWaitError as exc:
            if gate is not None:
                gate.record(exc.seconds)
            raise
