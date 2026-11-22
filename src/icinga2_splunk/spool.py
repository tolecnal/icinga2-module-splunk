"""Disk-backed spool for events that could not be delivered to Splunk.

Events are stored as newline-delimited, already serialized HEC envelopes in
segment files. New events are appended to the *current* segment; once it
reaches ``segment_max_bytes`` it is closed and a new one is started. Segments
are replayed oldest first and deleted once delivered, giving at-least-once
delivery (a crash during replay can cause a segment to be sent again).

The spool is bounded by total size and by age; when either limit is exceeded
the oldest segments are deleted and the lost events are logged.

This class is not thread-safe: it is owned by the HEC client's worker thread.
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import IO

logger = logging.getLogger(__name__)

SEGMENT_SUFFIX = ".jsonl"
DEFAULT_SEGMENT_MAX_BYTES = 4 * 1024 * 1024


class DiskSpool:
    """Bounded, append-only on-disk queue of serialized events.

    Args:
        directory: Spool directory; created with mode 0700 if missing.
        max_bytes: Maximum total size of all segments.
        max_age: Maximum age in seconds of a closed segment (by mtime).
        segment_max_bytes: Size at which the current segment is rolled.
        clock: Wall-clock source (injectable for tests).
    """

    def __init__(
        self,
        directory: str | os.PathLike[str],
        *,
        max_bytes: int,
        max_age: float,
        segment_max_bytes: int = DEFAULT_SEGMENT_MAX_BYTES,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """Open (or create) the spool and index any segments left from a previous run."""
        self.directory = Path(directory)
        self.max_bytes = max_bytes
        self.max_age = max_age
        # Keep several segments within the size budget so trimming is gradual.
        self.segment_max_bytes = max(1, min(segment_max_bytes, max_bytes // 4))
        self._clock = clock

        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        if not os.access(self.directory, os.W_OK | os.X_OK):
            raise PermissionError(f"Spool directory {self.directory} is not writable")

        # Closed segments, oldest first, with their sizes.
        self._segments: dict[Path, int] = {}
        for path in sorted(self.directory.glob(f"*{SEGMENT_SUFFIX}")):
            size = path.stat().st_size
            if size:
                self._segments[path] = size
            else:
                path.unlink(missing_ok=True)

        self._current: IO[bytes] | None = None
        self._current_path: Path | None = None
        self._current_bytes = 0
        self._seq = 0
        self.dropped_count = 0

        if self._segments:
            logger.info(
                "Found %d spooled segment(s) (%.1f MB) from a previous run",
                len(self._segments),
                self.size_bytes / 1_048_576,
            )

    # ---------------------------------------------------------------- status

    @property
    def size_bytes(self) -> int:
        """Total bytes on disk, including the current segment."""
        return sum(self._segments.values()) + self._current_bytes

    def __bool__(self) -> bool:
        """Whether any events are spooled."""
        return bool(self._segments) or self._current_bytes > 0

    # --------------------------------------------------------------- writing

    def append(self, lines: Iterable[str]) -> None:
        """Append serialized events to the spool, enforcing the size limit."""
        data = b"".join(line.encode("utf-8") + b"\n" for line in lines)
        if not data:
            return
        if self._current is None or self._current_bytes >= self.segment_max_bytes:
            self._roll()
            self._open_segment()
        assert self._current is not None  # noqa: S101 - for type narrowing
        self._current.write(data)
        self._current.flush()
        self._current_bytes += len(data)
        self._enforce_size()

    def _open_segment(self) -> None:
        self._seq += 1
        name = f"{time.time_ns():020d}-{os.getpid()}-{self._seq:06d}{SEGMENT_SUFFIX}"
        path = self.directory / name
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_APPEND, 0o600)
        self._current = os.fdopen(fd, "ab")
        self._current_path = path
        self._current_bytes = 0

    def _roll(self) -> None:
        """Close the current segment and make it available for replay."""
        if self._current is None or self._current_path is None:
            return
        self._current.flush()
        os.fsync(self._current.fileno())
        self._current.close()
        if self._current_bytes:
            self._segments[self._current_path] = self._current_bytes
        else:
            self._current_path.unlink(missing_ok=True)
        self._current = None
        self._current_path = None
        self._current_bytes = 0

    def close(self) -> None:
        """Flush and close the current segment."""
        self._roll()

    # --------------------------------------------------------------- reading

    def oldest(self) -> tuple[Path, list[str]] | None:
        """Return the oldest segment and its valid lines, or None if empty.

        The segment stays on disk until :meth:`remove` is called.
        """
        if not self._segments:
            self._roll()
        while self._segments:
            path = next(iter(self._segments))
            try:
                raw = path.read_bytes()
            except FileNotFoundError:
                self._segments.pop(path, None)
                continue
            return path, _valid_lines(raw, path)
        return None

    def remove(self, path: Path) -> None:
        """Delete a segment after it has been delivered."""
        self._segments.pop(path, None)
        path.unlink(missing_ok=True)

    # ---------------------------------------------------------------- limits

    def enforce_limits(self) -> None:
        """Delete segments that exceed the age or size limit."""
        cutoff = self._clock() - self.max_age
        if self._current_path is not None and self._current_bytes:
            # Make an idle current segment subject to the age limit.
            try:
                if self._current_path.stat().st_mtime < cutoff:
                    self._roll()
            except FileNotFoundError:
                pass
        for path in list(self._segments):
            try:
                mtime = path.stat().st_mtime
            except FileNotFoundError:
                self._segments.pop(path, None)
                continue
            if mtime >= cutoff:
                break
            self._drop(path, "older than the spool age limit")
        self._enforce_size()

    def _enforce_size(self) -> None:
        while self.size_bytes > self.max_bytes and self._segments:
            self._drop(next(iter(self._segments)), "spool size limit reached")
        if self.size_bytes > self.max_bytes and self._current_bytes:
            # Only the current segment is left and it is over the limit.
            self._roll()
            self._enforce_size()

    def _drop(self, path: Path, reason: str) -> None:
        try:
            count = path.read_bytes().count(b"\n")
        except FileNotFoundError:
            count = 0
        self.remove(path)
        self.dropped_count += count
        logger.error("Discarded %d spooled events (%s): %s", count, reason, path.name)


def _valid_lines(raw: bytes, path: Path) -> list[str]:
    """Split a segment into lines, skipping any that are not valid JSON.

    A torn final line can occur if the process crashed mid-write.
    """
    lines: list[str] = []
    bad = 0
    for chunk in raw.split(b"\n"):
        if not chunk.strip():
            continue
        try:
            line = chunk.decode("utf-8")
            json.loads(line)
        except ValueError:
            bad += 1
            continue
        lines.append(line)
    if bad:
        logger.warning("Skipped %d corrupt line(s) in spool segment %s", bad, path.name)
    return lines
