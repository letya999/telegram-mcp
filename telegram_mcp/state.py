"""Shared durability helpers for the small on-disk state files.

``floodgate``, ``opsjournal`` and the incoming-event tracker each keep a
flat state file under :func:`floodgate.state_dir`. Those files only matter
if they survive a crash intact: a torn write must never look like a valid
record, and a write the OS has merely buffered must not be reported as
persisted. This module is the one place those rules live:

- every JSON file is written to a sibling ``*.tmp``, fsynced, then
  atomically replaced into place;
- every payload carries ``"v"`` (a schema version) so a future format
  change can detect and migrate old files instead of guessing;
- append-only JSONL records are flushed and fsynced per line, so the last
  durable state of an operation is never a buffer the kernel still holds.

Nothing here is Telegram-specific; the helpers stay small and boring on
purpose -- the same properties a real database gives you, for files that
hold a handful of records.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

# Schema version stamped into every state file. Bump when a payload's shape
# changes in a way older readers could misinterpret.
STATE_VERSION = 1


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write ``payload`` (+ schema version) to ``path`` atomically.

    The write goes to a sibling temp file, is fsynced, and then replaces the
    target, so a crash leaves either the old file or the new one -- never a
    half-written record. Raises ``OSError`` on failure; callers decide
    whether an unpersisted state is fatal (floodgate) or logged (events).
    """
    data = {"v": STATE_VERSION, **payload}
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        fh.write(json.dumps(data, ensure_ascii=False))
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    """Append one JSON line to ``path`` and fsync it durable.

    Raises ``OSError`` on failure so the caller can classify the operation
    outcome honestly instead of believing a record exists that doesn't.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
